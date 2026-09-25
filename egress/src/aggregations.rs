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

const PERCENT_DEVIATION_THRESHOLD: f64 = 0.1; // 10% deviation threshold for sub-daily aggregations

// need to have time resolution to be able to check the number of expected data points
// create a sort of map of aggregation periods to minimums for certain timeresolutions?
pub type MinCount = Arc<RwLock<HashMap<AggregationPeriod, Vec<(Interval, i64)>>>>;
pub fn minimum_count_timresolution() -> MinCount {
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregationDatum {
    pub value: Option<f64>,
    pub time_bin: DateTime<Utc>,
    pub count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {
    pub data: Vec<AggregationDatum>,
    start_time: DateTime<Utc>,
}

#[allow(clippy::too_many_arguments)]
pub async fn get_aggregation(
    conn: &PooledPgConn<'_>,
    station_id: i32,
    param_id: i32,
    params: AggregationParams,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
    level_table: level::LevelTable,
    roles_permit: &[i32],
    roles_station: &[i32],
) -> Result<Vec<Aggregation>, Error> {
    let level = default_level_from_api_param(level_table, params.level, param_id)?;
    let label = util::PatchworkLabel {
        station_id,
        param_id,
        level,
        sensor: params.sensor.or(Some(0)), // default to sensor 0 if not provided
    };
    // get the applicable timeseries from patchwork
    // TODO: is this correct to use for aggregations, or do we want to go to timeseries directly?
    // if patchwork has timeresolution then maybe its more likely to work?
    let mut applicable_ts = get_applicable_timeseries(
        params.from,
        params.to.unwrap_or_else(Utc::now),
        label,
        roles_permit,
        roles_station,
        patchwork_table,
    )?;

    let agg_func = match params.agg_type {
        AggregationType::Max => "max",
        AggregationType::Min => "min",
        AggregationType::Avg => "avg",
        AggregationType::Sum => "sum",
    };

    // offset...
    // create one for adding and one for subtracting
    let (plus_offset, minus_offset) = match params.offset_hours {
        Some(offset) => (
            format!("+ INTERVAL '{} hours'", offset),
            format!("- INTERVAL '{} hours'", offset),
        ),
        None => ("".to_string(), "".to_string()), // no offset
    };

    // create the time bins, and use the offset if it exists (it can just be an empty string if it doesn't)
    let time_binning = match params.period {
        AggregationPeriod::Hourly => format!(
            "date_trunc('hour', obstime {} ) {}",
            plus_offset, minus_offset
        ),
        AggregationPeriod::TwiceDaily => format!(
            "date_bin('12 hours', obstime {}, TIMESTAMP '2000-01-01 00:00:00') {}",
            plus_offset, minus_offset
        ),
        AggregationPeriod::Daily => format!(
            "date_trunc('day', obstime {} ) {}",
            plus_offset, minus_offset
        ),
        // Monthly and Yearly should be based on daily, so should not be done in the SQL?
    };

    let mut aggregations: Vec<Aggregation> = Vec::new();
    let min_counts = minimum_count_timresolution();
    let min_counts_for_aggregation_period = min_counts.read().unwrap().get(&params.period).cloned();

    // filter the quality code of the underlying data (like in calculations)
    // TODO: should this be the default list?
    let accepted_qc = params
        .accepted_qc
        .unwrap_or_else(|| vec![-1, 0, 1, 2, 3, 4, 5, 6, 7]);

    // should filter for minimum counts?
    let count_cutoff = params.count_cutoff.unwrap_or(true);

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
                adjust_patch_time(&mut patch.to, params.offset_hours, params.period);
            } else if i == len - 1 {
                adjust_patch_time(&mut patch.from, params.offset_hours, params.period);
            } else {
                adjust_patch_time(&mut patch.to, params.offset_hours, params.period);
                adjust_patch_time(&mut patch.from, params.offset_hours, params.period);
            }
        }
    }

    // loop over all the tsid and get the aggregation for each, then combine into a single response
    for ts in applicable_ts {
        // cut down the time to ensure it overlaps with the from/to of the applicable ts
        let from = if ts.from > params.from {
            ts.from
        } else {
            params.from
        };
        let to_param = params.to.unwrap_or_else(Utc::now);
        let to = if ts.to < to_param { ts.to } else { to_param };
        // NOTE: an alternate way of dealing with the potential holes due to where patchwork series abut
        // is to allow the from / to time here to be outside what is allowed by the patchwork.
        // But then we would get double aggregations if there is data for both timeseries and would have
        // to deal with the fallout of that...
        let agg = get_aggregation_data(
            agg_func,
            &time_binning,
            accepted_qc.clone(),
            count_cutoff,
            ts.tsid,
            from,
            to,
            conn,
            min_counts_for_aggregation_period.clone(),
        )
        .await?;
        if !agg.data.is_empty() {
            aggregations.push(agg);
        }
    }
    // this will be empty if nothing could be calculated (404 happens at the handler level)
    Ok(aggregations)
}

#[allow(clippy::too_many_arguments)]
async fn get_aggregation_data(
    agg_func: &str,
    time_binning: &str,
    accepted_qc: Vec<i32>,
    count_cutoff: bool,
    tsid: TsId,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    conn: &tokio_postgres::Client,
    min_counts_for_aggregation_period: Option<Vec<(Interval, i64)>>,
) -> Result<Aggregation, tokio_postgres::Error> {
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

    let min_count = match timeresolution {
        Some(resolution) => min_counts_for_aggregation_period
            .as_ref()
            .and_then(|counts| counts.iter().find(|(res, _)| *res == resolution))
            .map(|(_, count)| *count),
        None => None,
    };

    // find the number of seconds (if sub daily resolution)
    let max_deviation_expression = match timeresolution {
        // NOTE: this should check that its sub daily, but could add extra check?
        Some(resolution) if resolution.days == 0 && resolution.months == 0 => {
            let seconds_resolution = resolution.microseconds / 1_000_000;
            format!(
                r#"
                MAX(
                    ABS(
                        EXTRACT(EPOCH FROM (
                            obstime -
                            (
                                time_bin +
                                ROUND(
                                    EXTRACT(EPOCH FROM (obstime - time_bin))
                                    / {}
                                ) * {} * INTERVAL '1 second'
                            )
                        ))
                    )
                )
                "#,
                seconds_resolution, seconds_resolution
            )
        }
        _ => "NULL::double precision".to_string(),
    };

    let binned_query = format!(
        r#"
        SELECT
            original,
            corrected,
            obstime,
            {} AS time_bin
        FROM legacy.data
        WHERE
            timeseries = $1 AND
            obstime BETWEEN $2 AND $3
            AND COALESCE(quality_code, -1) = ANY($4::int[])
        "#,
        time_binning
    );

    let query_string = format!(
        r#"
        SELECT
            (agg_value).f1 AS agg_original,
            (agg_value).f2 AS agg_corrected,
            time_bin,
            agg_count,
            max_deviation
        FROM (
            SELECT
                ({}(original), {}(corrected)) AS agg_value,
                time_bin,
                COUNT(*) AS agg_count,
                ({} )::double precision AS max_deviation
            FROM (
                {}
            ) binned
            GROUP BY time_bin
        ) aggregated
        "#,
        agg_func, agg_func, max_deviation_expression, binned_query
    );

    // add the min_count filter if count_cutoff is true and min_count is Some
    let agg_results = if count_cutoff && let Some(min_count) = min_count {
        let query_with_cutoff = format!("{}\nWHERE agg_count >= $5", query_string);
        conn.query(
            query_with_cutoff.as_str(),
            &[&tsid, &start_time, &end_time, &accepted_qc, &min_count],
        )
        .await
    } else {
        conn.query(
            query_string.as_str(),
            &[&tsid, &start_time, &end_time, &accepted_qc],
        )
        .await
    };

    match agg_results {
        Ok(rows) => {
            let agg = {
                let mut data = Vec::with_capacity(rows.len());
                // TODO: handle gaps in the series
                for row in rows {
                    let row_count: i64 = row.get("agg_count");
                    // prioritize corrected values for aggregations, fallback to original if corrected is absent
                    let value = row
                        .get::<usize, Option<f64>>(1)
                        .or(row.get::<usize, Option<f64>>(0));
                    let max_deviation: Option<f64> = row.get("max_deviation");

                    // check if the max deviation exceeds the threshold for sub-daily aggregations
                    if let Some(max_dev) = max_deviation
                        && let Some(resolution) = timeresolution
                        && resolution.days == 0
                        && resolution.months == 0
                    {
                        let seconds_resolution = resolution.microseconds / 1_000_000;
                        if max_dev > (PERCENT_DEVIATION_THRESHOLD * seconds_resolution as f64) {
                            warn!(
                                "Max deviation {} exceeds threshold {} for time_bin {:?}, tsid: {}",
                                max_dev,
                                PERCENT_DEVIATION_THRESHOLD * seconds_resolution as f64,
                                row.get::<usize, DateTime<Utc>>(2),
                                tsid
                            );
                        } else {
                            data.push(AggregationDatum {
                                value,
                                time_bin: row.get(2),
                                count: row_count,
                            });
                        }
                    } else {
                        data.push(AggregationDatum {
                            value,
                            time_bin: row.get(2),
                            count: row_count,
                        });
                    }
                }
                Aggregation { data, start_time }
            };
            Ok(agg)
        }
        Err(e) => {
            warn!("Error executing aggregation query: {:?}", e);
            Err(e)
        }
    }
}
