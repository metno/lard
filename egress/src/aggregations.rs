use crate::Error;
use crate::patchwork::{PatchworkTimeseriesTable, get_applicable_timeseries};
use chrono::{DateTime, Duration, TimeDelta, Utc};
use pg_interval::Interval;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tracing::warn;
use util::{PatchworkLabel, PooledPgConn, TsId};

#[derive(Debug, Deserialize, Clone, Copy)]
pub enum AggregationType {
    Max,
    Min,
    Avg,
    Sum,
    // TODO: deal with over_time (but these will be exceptions since they are coded values)
    // in addition need to support: integral_of_excess, integral_of_deficit?
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationPeriod {
    Hourly,
    TwiceDaily,
    Daily,
    //Monthly,
    //Yearly,
}

// need to have time resolution to be able to check the number of expected data points
// create a sort of map of aggregation periods to minimums for certain timeresolutions?
pub type MinCount = Arc<HashMap<AggregationPeriod, Vec<(Interval, i64)>>>;
pub fn minimum_count_timeresolution() -> MinCount {
    let min_count = HashMap::from([
        (
            AggregationPeriod::Hourly,
            vec![
                (Interval::from_duration(Duration::minutes(10)).unwrap(), 6),
                (Interval::from_duration(Duration::minutes(60)).unwrap(), 1),
            ],
        ),
        (
            AggregationPeriod::TwiceDaily,
            vec![
                (Interval::from_duration(Duration::minutes(10)).unwrap(), 72),
                (Interval::from_duration(Duration::minutes(60)).unwrap(), 12),
                (Interval::from_duration(Duration::hours(12)).unwrap(), 1),
            ],
        ),
        (
            AggregationPeriod::Daily,
            vec![
                (Interval::from_duration(Duration::minutes(10)).unwrap(), 144),
                (Interval::from_duration(Duration::minutes(60)).unwrap(), 24),
                (Interval::from_duration(Duration::days(1)).unwrap(), 1),
            ],
        ),
        // TODO: figure out what the minimum counts should be for monthly and yearly aggregations
        // they maybe need to be calculated from underlying aggregations (as in daily...?) - check kdvh triggers
    ]);
    Arc::new(min_count)
}

impl AggregationPeriod {
    pub fn as_str(&self) -> Option<String> {
        match self {
            AggregationPeriod::Hourly => Some("PT1H".to_string()),
            AggregationPeriod::TwiceDaily => Some("PT12H".to_string()),
            AggregationPeriod::Daily => Some("P1D".to_string()),
            // AggregationPeriod::Monthly => Some("P1M".to_string()),
            // AggregationPeriod::Yearly => Some("P1Y".to_string()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {
    pub value: Option<f64>,
    pub bin_end: DateTime<Utc>,
}

#[allow(clippy::too_many_arguments)]
pub async fn get_aggregation(
    conn: &PooledPgConn<'_>,
    period: AggregationPeriod,
    agg_func: &str,
    offset_hours: Option<i64>,
    accepted_qc: Vec<i32>,
    count_cutoff: bool,
    check_regular: bool,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    patchwork_label: PatchworkLabel,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
    roles_permit: &[i32],
    roles_station: &[i32],
) -> Result<Vec<Aggregation>, Error> {
    // get the applicable timeseries from patchwork
    // TODO: is this correct to use for aggregations, or do we want to go to timeseries directly?
    // if patchwork has timeresolution then maybe its more likely to work?
    let mut applicable_ts = get_applicable_timeseries(
        from,
        to.unwrap_or_else(Utc::now),
        patchwork_label,
        roles_permit,
        roles_station,
        patchwork_table,
    )?;

    // Post process the patch from/to dates to align with the aggregation period / offset
    // so that we don't get weird holes that we can avoid...
    #[inline]
    fn adjust_patch_time(t: DateTime<Utc>, offset: i64) -> DateTime<Utc> {
        t.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc() + TimeDelta::hours(offset)
    }
    // do not apply the offset for hourly aggregations since then the offset does not make sense, but for daily and twice daily it does
    // there is also no point in modifying the times if there is only one applicable timeseries
    if let Some(o) = offset_hours
        && (period == AggregationPeriod::Daily || period == AggregationPeriod::TwiceDaily)
        && applicable_ts.len() > 1
    {
        // NOTE: move the time forward since assume more likely to have continuation of older timeseries
        let (first, rest) = applicable_ts.split_at_mut(1);
        let (middle, last) = rest.split_at_mut(rest.len() - 1);

        // modify the to for the first
        first[0].to = adjust_patch_time(first[0].to, o);
        // modify the from for the last
        last[0].from = adjust_patch_time(last[0].from, o);

        for patch in middle {
            // modify both from and to in the middle
            patch.from = adjust_patch_time(patch.from, o);
            patch.to = adjust_patch_time(patch.to, o);
        }
    }
    let mut aggregations = vec![];

    // loop over all the tsid and get the aggregation for each, then combine into a single response
    for ts in applicable_ts {
        // cut down the time to ensure it overlaps with the from/to of the applicable ts
        let from = if ts.from > from { ts.from } else { from };
        let to_param = to.unwrap_or_else(Utc::now);
        let to = if ts.to < to_param { ts.to } else { to_param };
        if to <= from {
            // twisted since there was no overlap...
            continue;
        }
        // NOTE: an alternate way of dealing with the potential holes due to where patchwork series abut
        // is to allow the from / to time here to be outside what is allowed by the patchwork.
        // But then we would get double aggregations if there is data for both timeseries and would have
        // to deal with the fallout of that...
        let mut agg = get_aggregation_data(
            ts.tsid,
            from,
            to,
            agg_func,
            period,
            offset_hours,
            accepted_qc.clone(),
            count_cutoff,
            check_regular,
            conn,
        )
        .await?;
        if !agg.is_empty() {
            aggregations.append(&mut agg);
        }
    }
    // this will be empty if nothing could be calculated (404 happens at the handler level)
    Ok(aggregations)
}

#[allow(clippy::too_many_arguments)]
async fn get_aggregation_data(
    tsid: TsId,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    agg_func: &str,
    period: AggregationPeriod,
    offset_hours: Option<i64>,
    accepted_qc: Vec<i32>,
    count_cutoff: bool,
    check_regular: bool,
    conn: &tokio_postgres::Client,
) -> Result<Vec<Aggregation>, Error> {
    // offset...
    // create one for adding and one for subtracting
    let (plus_offset, minus_offset) = match offset_hours {
        Some(offset) => (
            format!("+ INTERVAL '{} hours'", offset),
            format!("- INTERVAL '{} hours'", offset),
        ),
        None => ("".to_string(), "".to_string()), // no offset
    };

    // create the time bin, and use the offset if it exists (it can just be an empty string if it doesn't)
    let (time_binning, hours_agg) = match period {
        AggregationPeriod::Hourly => (
            format!(
                "date_trunc('hour', obstime {} ) {}",
                minus_offset, plus_offset
            ),
            1,
        ),
        AggregationPeriod::TwiceDaily => (
            format!(
                "date_bin('12 hours', obstime {}, TIMESTAMP '2000-01-01 00:00:00') {}",
                minus_offset, plus_offset
            ),
            12,
        ),
        AggregationPeriod::Daily => (
            format!(
                "date_trunc('day', obstime {} ) {}",
                minus_offset, plus_offset
            ),
            24,
        ),
        // Monthly and Yearly should be based on daily, so should not be done in the SQL?
    };
    // Figure out how to not do the caculation if missing too much data (highly dependent on timeresolution)
    // See Ketil's comment about how it was done in kdvh triggers: https://codeberg.org/metno/lard/pulls/83
    // Have a map of aggregation periods to minimums for certain timeresolutions,
    // get the timeresolution for the timeseries, so we can check if we have enough data points for the aggregation
    let timeresolution = conn
        .query_one(
            "SELECT timeresolution FROM public.timeseries WHERE id = $1",
            &[&tsid],
        )
        .await?
        .get::<_, Option<Interval>>("timeresolution");

    // construct the query string
    let query_string = match timeresolution {
        Some(timeresolution) => {
            if check_regular {
                construct_sql_regular(agg_func, period, time_binning, timeresolution, count_cutoff)
                    .await
            } else {
                construct_sql_irregular(agg_func, time_binning).await
            }
        }
        None => construct_sql_irregular(agg_func, time_binning).await,
    };

    let agg_results = conn
        .query(
            query_string.as_str(),
            &[&tsid, &start_time, &end_time, &accepted_qc],
        )
        .await;

    match agg_results {
        Ok(rows) => {
            let data = if let Some(_timeresolution) = timeresolution {
                // TODO: do we need timresolution here, or is it more that it necessary to know it exists...?
                // fell back on hours_agg (which comes from it)

                // calculate how many aggregations we should be able to create and preallocate the array
                let period = Duration::hours(hours_agg);
                let num_aggregations = (end_time - start_time).num_seconds() / (hours_agg * 3600);

                // We populate with Nones since we cannot be sure that when we get the data we will have an aggregation
                // for each time step, so this way we return a regularlized timeseries with empty values where we are missing
                // enough data for an aggregation
                let mut data: Vec<Aggregation> = (0..num_aggregations)
                    .map(|i| {
                        Aggregation {
                            value: None,
                            // Bin end is used in the output since that is the way it has been done in the climate division
                            // that a time interval is represented by the end of the interval. The +1 is needed to start the count
                            // at 1 instead of 0 (as is done in the code).
                            bin_end: start_time + period * (i as i32 + 1),
                        }
                    })
                    .collect();

                for row in rows {
                    let value = row.get("agg_value");
                    let bin_start: DateTime<Utc> = row.get("time_bin");

                    let index =
                        ((bin_start - start_time).num_seconds() / period.num_seconds()) as usize;

                    data[index].value = value;
                }
                data
            } else {
                let mut data: Vec<Aggregation> = Vec::with_capacity(rows.len());
                for row in rows {
                    let value = row.get("agg_value");
                    let bin_start: DateTime<Utc> = row.get("time_bin");
                    data.push(Aggregation {
                        value,
                        bin_end: bin_start + Duration::hours(hours_agg),
                    });
                }
                data
            };
            Ok(data)
        }
        Err(e) => {
            warn!("Error executing aggregation query: {:?}", e);
            Err(Error::Database(e))
        }
    }
}

async fn construct_sql_regular(
    agg_func: &str,
    period: AggregationPeriod,
    time_binning: String,
    timeresolution: Interval,
    count_cutoff: bool,
) -> String {
    // have time resolution so can try to assess minimum counts
    let min_counts_for_aggregation_period = minimum_count_timeresolution().get(&period).cloned();
    let min_count = min_counts_for_aggregation_period
        .as_ref()
        .and_then(|counts| counts.iter().find(|(res, _)| *res == timeresolution))
        .map(|(_, count)| *count);

    if let Some(min_count) = min_count
        && count_cutoff
    {
        format!(
            r#"
                SELECT
                    {}(COALESCE(data.corrected, data.original)) AS agg_value,
                    {} AS time_bin
                FROM legacy.data AS data
                WHERE
                    data.timeseries = $1
                    AND data.obstime >= $2::timestamptz
                    AND data.obstime < $3::timestamptz
                    AND COALESCE(data.quality_code, -1) = ANY($4::int[])
                    AND MOD(
                        EXTRACT(EPOCH FROM (data.obstime - $2::timestamptz))::numeric,
                        EXTRACT(EPOCH FROM '{}'::interval)::numeric
                    ) = 0
                GROUP BY time_bin
                HAVING COUNT(*) >= {};
            "#,
            agg_func,
            time_binning,
            timeresolution.to_iso_8601(),
            min_count,
        )
    } else {
        format!(
            r#"
                SELECT
                    {}(COALESCE(data.corrected, data.original)) AS agg_value,
                    {} AS time_bin
                FROM legacy.data AS data
                WHERE
                    data.timeseries = $1
                    AND data.obstime >= $2::timestamptz
                    AND data.obstime < $3::timestamptz
                    AND COALESCE(data.quality_code, -1) = ANY($4::int[])
                    AND MOD(
                        EXTRACT(EPOCH FROM (data.obstime - $2::timestamptz))::numeric,
                        EXTRACT(EPOCH FROM '{}'::interval)::numeric
                    ) = 0
                GROUP BY time_bin;
            "#,
            agg_func,
            time_binning,
            timeresolution.to_iso_8601()
        )
    }
}

async fn construct_sql_irregular(agg_func: &str, time_binning: String) -> String {
    let sql_query = format!(
        r#"
            SELECT
                {}(COALESCE(data.corrected, data.original)) AS agg_value,
                {} AS time_bin,
                COUNT(*) AS agg_count
            FROM legacy.data
            WHERE
                timeseries = $1
                AND obstime >= $2::timestamptz
                AND obstime < $3::timestamptz
                AND COALESCE(quality_code, -1) = ANY($4::int[])
            GROUP BY time_bin;
        "#,
        agg_func, time_binning
    );
    sql_query
}
