use crate::Error;
use crate::patchwork::PatchworkTimeseriesTable;
use crate::patchwork::get_applicable_timeseries;
use crate::util::default_level_from_api_param;
use chrono::Timelike;
use chrono::{DateTime, Duration, TimeDelta, Utc};
use chronoutil::RelativeDuration;
use pg_interval::Interval;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tracing::warn;
use util::{PooledPgConn, TsId, deserialize::optional_comma_separated, stinfofacade::level};

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
pub type MinCount = Arc<RwLock<HashMap<AggregationPeriod, Vec<(Interval, i64)>>>>;
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

    Arc::new(RwLock::new(min_count))
}

impl From<AggregationPeriod> for RelativeDuration {
    fn from(val: AggregationPeriod) -> RelativeDuration {
        match val {
            AggregationPeriod::Hourly => RelativeDuration::hours(1),
            AggregationPeriod::TwiceDaily => RelativeDuration::hours(12),
            AggregationPeriod::Daily => RelativeDuration::days(1),
            //AggregationPeriod::Monthly => RelativeDuration::months(1),
            //AggregationPeriod::Yearly => RelativeDuration::years(1),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct AggregationParams {
    agg_type: AggregationType,
    period: AggregationPeriod,
    // Need a param that adds an offset so that people can create aggregations from 23-23 for example
    offset_hours: Option<i64>,
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>, // default to now if not provided
    #[serde(default, deserialize_with = "optional_comma_separated")]
    accepted_qc: Option<Vec<i32>>,
    count_cutoff: Option<bool>, // make it so can turn off filtering for minimum counts (default to true)
    check_regular: Option<bool>, // choose if should check the data is regular
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {
    pub value: Option<f64>,
    pub bin_start: DateTime<Utc>,
    pub bin_end: DateTime<Utc>,
    pub count: Option<i64>,
}

pub async fn get_aggregation(
    conn: &PooledPgConn<'_>,
    query: (i32, i32, AggregationParams),
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
    level_table: level::LevelTable,
    roles_permit: &[i32],
    roles_station: &[i32],
) -> Result<Vec<Aggregation>, Error> {
    let station_id = query.0;
    let param_id = query.1;
    let level = default_level_from_api_param(level_table, query.2.level, param_id)?;
    let label = util::PatchworkLabel {
        station_id,
        param_id,
        level,
        sensor: query.2.sensor.or(Some(0)), // default to sensor 0 if not provided
    };
    // get the applicable timeseries from patchwork
    // TODO: is this correct to use for aggregations, or do we want to go to timeseries directly?
    // if patchwork has timeresolution then maybe its more likely to work?
    let mut applicable_ts = get_applicable_timeseries(
        query.2.from,
        query.2.to.unwrap_or_else(Utc::now),
        label,
        roles_permit,
        roles_station,
        patchwork_table,
    )?;

    let agg_func = match query.2.agg_type {
        AggregationType::Max => "max",
        AggregationType::Min => "min",
        AggregationType::Avg => "avg",
        AggregationType::Sum => "sum",
    };

    let mut aggregations: Vec<Aggregation> = Vec::new();

    // filter the quality code of the underlying data (like in calculations)
    // TODO: should this be the default list?
    let accepted_qc = query
        .2
        .accepted_qc
        .unwrap_or_else(|| vec![-1, 0, 1, 2, 3, 4, 5, 6, 7]);

    // should filter for minimum counts?
    let count_cutoff = query.2.count_cutoff.unwrap_or(true);
    let check_regular = query.2.check_regular.unwrap_or(true);

    #[inline]
    fn adjust_patch_time(t: &mut DateTime<Utc>, offset: Option<i64>, period: AggregationPeriod) {
        // first 0 out the hour minutes and seconds, then apply the offset if it exists
        // TODO: we could remove this if we are certain these are truncated now in stinfosys
        *t = t
            .with_hour(0)
            .unwrap()
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap();
        // do not apply the offset for hourly aggregations since then the offset does not make sense, but for daily and twice daily it does
        if let Some(o) = offset
            && (period == AggregationPeriod::Daily || period == AggregationPeriod::TwiceDaily)
        {
            // NOTE: move forward since assume more likely to have continuation of older timeseries
            *t += TimeDelta::hours(o);
        }
    }

    // Post process the patch from/to dates to align with the aggregation period / offset
    // so that we don't get weird holes that we can avoid...
    let len = applicable_ts.len();
    // no need to adjust if only one timeseries, since it will be filtered by the from/to of the request
    if len > 1 {
        for (i, patch) in applicable_ts.iter_mut().enumerate() {
            if i == 0 {
                adjust_patch_time(&mut patch.to, query.2.offset_hours, query.2.period);
            } else if i == len - 1 {
                adjust_patch_time(&mut patch.from, query.2.offset_hours, query.2.period);
            } else {
                adjust_patch_time(&mut patch.to, query.2.offset_hours, query.2.period);
                adjust_patch_time(&mut patch.from, query.2.offset_hours, query.2.period);
            }
        }
    }

    // loop over all the tsid and get the aggregation for each, then combine into a single response
    for ts in applicable_ts {
        // cut down the time to ensure it overlaps with the from/to of the applicable ts
        let from = if ts.from > query.2.from {
            ts.from
        } else {
            query.2.from
        };
        let to_param = query.2.to.unwrap_or_else(Utc::now);
        let to = if ts.to < to_param { ts.to } else { to_param };
        // NOTE: an alternate way of dealing with the potential holes due to where patchwork series abut
        // is to allow the from / to time here to be outside what is allowed by the patchwork.
        // But then we would get double aggregations if there is data for both timeseries and would have
        // to deal with the fallout of that...
        let mut agg = get_aggregation_data(
            ts.tsid,
            from,
            to,
            agg_func,
            query.2.period,
            query.2.offset_hours,
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
    // TODO: figure out how to not do the caculation if missing too much data (highly dependent on timeresolution)
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

    let min_counts = minimum_count_timeresolution();
    let min_counts_for_aggregation_period = min_counts.read().unwrap().get(&period).cloned();

    let min_count = match timeresolution {
        Some(resolution) => min_counts_for_aggregation_period
            .as_ref()
            .and_then(|counts| counts.iter().find(|(res, _)| *res == resolution))
            .map(|(_, count)| *count),
        None => None,
    };

    let interval = match timeresolution {
        Some(val) if val == Interval::from_iso("PT1M")? => Some("1 minute"),
        Some(val) if val == Interval::from_iso("PT1H")? => Some("1 hour"),
        Some(val) if val == Interval::from_iso("P1D")? => Some("1 day"),
        Some(_) => None, // can we support all timeresolutions?
        None => None,    // if unknown timeresolution we cannot assess regularity
    };

    // TODO: the timestamp used in the mod clause must have any offset that is inherent to the
    // data itself added (for example if any hourly data always comes 10 min past the hour)
    let query_string = if let Some(timeres_interval) = interval
        && check_regular
    {
        format!(
            r#"
                SELECT
                    value AS agg_value,
                    time_bin,
                    agg_count
                FROM (
                    SELECT
                        {}(COALESCE(data.corrected, data.original)) AS value,
                        {} AS time_bin,
                        COUNT(*) AS agg_count
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
                ) AS aggregated;
            "#,
            agg_func, time_binning, timeres_interval
        )
    } else {
        format!(
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
        )
    };

    // modify the start and end time if have an offset
    // (for the mod, the binnings take care of the other part)
    let (offset_start_time, offset_end_time) = match offset_hours {
        Some(hours) => (
            start_time
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                + Duration::hours(hours),
            end_time
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                + Duration::hours(hours),
        ),
        None => (start_time, end_time),
    };

    // add the min_count filter if count_cutoff is true and min_count is Some
    let agg_results = if count_cutoff && let Some(min_count) = min_count {
        let query_with_cutoff = format!(
            "SELECT * FROM ({}) AS filtered WHERE agg_count >= $5",
            query_string.trim().trim_end_matches(';')
        );
        conn.query(
            query_with_cutoff.as_str(),
            &[
                &tsid,
                &offset_start_time,
                &offset_end_time,
                &accepted_qc,
                &min_count,
            ],
        )
        .await
    } else {
        conn.query(
            query_string.as_str(),
            &[&tsid, &offset_start_time, &offset_end_time, &accepted_qc],
        )
        .await
    };

    match agg_results {
        Ok(rows) => {
            let data = if let Some(_timeresolution) = timeresolution {
                // TODO: do we need timresolution here, or is it more that it necessary to know it exists...?
                // seems in the end I fell back on hours_agg (which comes from it)

                // calculate how many aggregations we should be able to create and preallocate the array
                let agg_duration = Duration::hours(hours_agg);
                let num_aggregations: usize = ((offset_end_time - offset_start_time)
                    .num_microseconds()
                    .unwrap()
                    / agg_duration.num_microseconds().unwrap())
                .try_into()
                .unwrap_or(0);

                let mut data_nulls: Vec<Aggregation> = (0..num_aggregations)
                    .map(|i| {
                        let bin_start = offset_start_time + agg_duration * (i as i32);
                        let bin_end = bin_start + agg_duration;
                        Aggregation {
                            bin_start,
                            bin_end,
                            value: None,
                            count: None,
                        }
                    })
                    .collect();

                for row in rows {
                    let row_count: i64 = row.get("agg_count");
                    let value = row.get("agg_value");
                    let bin_start: DateTime<Utc> = row.get("time_bin");
                    // insert the aggregation at the right spot in the null array
                    let offset_microseconds =
                        (bin_start - offset_start_time).num_microseconds().unwrap();
                    let step_microseconds = Duration::hours(hours_agg).num_microseconds().unwrap();
                    let index = (offset_microseconds / step_microseconds) as usize;

                    if let Some(aggregation) = data_nulls.get_mut(index) {
                        // Fill in aggregation.
                        aggregation.value = value;
                        aggregation.count = Some(row_count);
                    }
                }
                data_nulls
            } else {
                let mut data: Vec<Aggregation> = Vec::with_capacity(rows.len());
                for row in rows {
                    let row_count: i64 = row.get("agg_count");
                    let value = row.get("agg_value");
                    let bin_start: DateTime<Utc> = row.get("time_bin");
                    data.push(Aggregation {
                        value,
                        bin_start,
                        bin_end: bin_start + Duration::hours(hours_agg),
                        count: Some(row_count),
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
