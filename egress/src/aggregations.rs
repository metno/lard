use crate::Error;
use crate::patchwork::PatchworkTimeseriesTable;
use crate::patchwork::get_applicable_timeseries;
use chrono::{DateTime, Duration, Utc};
use chronoutil::RelativeDuration;
use http::StatusCode;
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
    Monthly,
    Yearly,
}

// need to have time resolution to be able to check the number of expected data points
// create a sort of map of aggregation periods to minimums for certain timeresolutions?
pub type MinCount = Arc<RwLock<HashMap<AggregationPeriod, Vec<(Interval, i64)>>>>;
pub fn minimum_count_timresolution() -> MinCount {
    let min_count = HashMap::from([
        (
            AggregationPeriod::Hourly,
            vec![(Interval::from_duration(Duration::minutes(10)).unwrap(), 6)],
        ),
        (
            AggregationPeriod::TwiceDaily,
            vec![
                (Interval::from_duration(Duration::minutes(10)).unwrap(), 72),
                (Interval::from_duration(Duration::minutes(60)).unwrap(), 12),
            ],
        ),
        (
            AggregationPeriod::Daily,
            vec![
                (Interval::from_duration(Duration::minutes(10)).unwrap(), 144),
                (Interval::from_duration(Duration::minutes(60)).unwrap(), 24),
            ],
        ),
        (AggregationPeriod::Monthly, vec![]),
        (AggregationPeriod::Yearly, vec![]),
    ]);

    Arc::new(RwLock::new(min_count))
}

impl From<AggregationPeriod> for RelativeDuration {
    fn from(val: AggregationPeriod) -> RelativeDuration {
        match val {
            AggregationPeriod::Hourly => RelativeDuration::hours(1),
            AggregationPeriod::TwiceDaily => RelativeDuration::hours(12),
            AggregationPeriod::Daily => RelativeDuration::days(1),
            AggregationPeriod::Monthly => RelativeDuration::months(1),
            AggregationPeriod::Yearly => RelativeDuration::years(1),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct AggregationParams {
    // TODO: add a param to filter the quality code of the underlying data (like in calculations)?
    // TODO: potentially add a param to specify if we want to be strict on how many underlying data points we have? default=true
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {
    pub data: Vec<(Option<f64>, DateTime<Utc>)>,
    start_time: DateTime<Utc>,
}

// TODO: move to util or check if this is reproduced somewhere else and can be reused
// function to find the default level for a param (used if not provided in the request)
pub fn get_default_level_for_param(param_id: i32, level_table: level::LevelTable) -> Option<i32> {
    let t = level_table.read().ok()?;
    let level = t.get(&param_id)?.default_hlevel;
    let level_type = t.get(&param_id)?.unit;
    let direction = t.get(&param_id)?.direction;
    match (level, level_type, direction) {
        (l, level::Unit::Cm, level::Direction::Up) => Some(l),
        (l, level::Unit::M, level::Direction::Up) => Some(l * 100),
        (l, level::Unit::Cm, level::Direction::Down) => Some(-l),
        (l, level::Unit::M, level::Direction::Down) => Some(-l * 100),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn get_aggregation(
    conn: &PooledPgConn<'_>,
    station_id: i32,
    param_id: i32,
    params: AggregationParams,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
    level_table: level::LevelTable,
    permit_roles: &[i32],
    station_roles: &[i32],
) -> Result<Vec<Aggregation>, Error> {
    let label = util::PatchworkLabel {
        station_id,
        param_id,
        level: params
            .level
            .or_else(|| get_default_level_for_param(param_id, level_table.clone())),
        sensor: params.sensor.or(Some(0)), // default to sensor 0 if not provided
    };
    // get the applicable timeseries from patchwork
    // TODO: is this correct to use for aggregations, or do we want to go to timeseries directly?
    // if patchwork has timeresolution then maybe its more likely to work?
    let applicable_ts = get_applicable_timeseries(
        params.from,
        params.to.unwrap_or_else(Utc::now),
        label,
        permit_roles,
        station_roles,
        patchwork_table,
    )?;
    //println!("Applicable timeseries: {:?}", applicable_ts);

    // check if we have any applicable timeseries, if not return 404
    if applicable_ts.is_empty() {
        return Err(Error::HttpStatus(StatusCode::NOT_FOUND));
    }

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
        AggregationPeriod::Monthly => format!(
            "date_trunc('month', obstime {} ) {}",
            plus_offset, minus_offset
        ),
        AggregationPeriod::Yearly => format!(
            "date_trunc('year', obstime {} ) {}",
            plus_offset, minus_offset
        ),
    };

    let mut aggregations: Vec<Aggregation> = Vec::new();
    let min_counts = minimum_count_timresolution();
    let min_counts_for_aggregation_period = min_counts.read().unwrap().get(&params.period).cloned();

    // filter the quality code of the underlying data (like in calculations)
    // TODO: should this be the default list?
    let accepted_qc = params
        .accepted_qc
        .unwrap_or_else(|| vec![-1, 0, 1, 2, 3, 4, 5, 6, 7]);

    // loop over all the tsid and get the aggregation for each, then combine into a single response
    for ts in applicable_ts {
        // TODO: cut down the time to ensure it overlaps with the from/o of the applicable ts
        let agg = get_aggregation_data(
            agg_func,
            &time_binning,
            accepted_qc.clone(),
            ts.tsid,
            params.from,
            params.to.unwrap_or_else(Utc::now),
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
    tsid: TsId,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    conn: &tokio_postgres::Client,
    min_counts_for_aggregation_period: Option<Vec<(Interval, i64)>>,
) -> Result<Aggregation, tokio_postgres::Error> {
    // TODO: figure out how to not do the caculation if missing too much data (highly dependent on timeresolution)
    // See Ketil's comment about how it was done in kdvh triggers: https://codeberg.org/metno/lard/pulls/83
    // Have a map of aggregation periods to minimums for certain timeresolutions,
    // but need to be able to ajust/turn off the filtering?

    let query_string = format!(
        r#"
        SELECT
            (agg_value).f1 AS agg_original,
            (agg_value).f2 AS agg_corrected,
            time_bin,
            agg_count
        FROM (
            SELECT
                ({}(original), {}(corrected)) AS agg_value,
                {} as time_bin,
                COUNT(*) AS agg_count
            FROM legacy.data
            WHERE
                timeseries = $1 AND
                obstime BETWEEN $2 AND $3
                AND COALESCE(quality_code, -1) = ANY($4::int[])
            GROUP BY time_bin
        ) aggregated
        "#,
        agg_func, agg_func, time_binning
    );
    //println!("Executing aggregation query: {}", query_string);

    let agg_results = conn
        .query(
            query_string.as_str(),
            &[&tsid, &start_time, &end_time, &accepted_qc],
        )
        .await;

    match agg_results {
        Ok(rows) => {
            let agg = {
                let mut data = Vec::with_capacity(rows.len());

                // get the timeresolution for the timeseries, so we can check if we have enough data points for the aggregation
                let timeresolution = conn
                    .query_one(
                        "SELECT timeresolution FROM public.timeseries WHERE id = $1",
                        &[&tsid],
                    )
                    .await
                    .unwrap()
                    .get::<_, Option<Interval>>("timeresolution");

                // TODO: handle gaps in the series
                for row in rows {
                    // check the time resolution, or default to hourly if not found
                    let resolution = timeresolution
                        .unwrap_or_else(|| Interval::from_duration(Duration::hours(1)).unwrap());
                    let min_count = min_counts_for_aggregation_period
                        .as_ref()
                        .and_then(|v| v.iter().find(|(res, _)| *res == resolution))
                        .map(|(_, count)| *count);
                    if let Some(min_count) = min_count {
                        let count: i64 = row.get(3);
                        if min_count > count {
                            /*
                                println!(
                                    "Skipping aggregation for time bin {:?} due to insufficient data points ({} < {})",
                                    row.get::<_, DateTime<Utc>>(1),
                                    count,
                                    min_count
                                );
                            */
                            continue;
                        }
                        // prioritize corrected values for aggregations, fallback to original if corrected is absent
                        let value = row
                            .get::<usize, Option<f64>>(1)
                            .or(row.get::<usize, Option<f64>>(0));
                        data.push((value, row.get(2)));
                    } else {
                        // If did not find a min count for the timeresolution,
                        // NO FILTERING APPLIED!
                        let value = row
                            .get::<usize, Option<f64>>(1)
                            .or(row.get::<usize, Option<f64>>(0));
                        data.push((value, row.get(2)));
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
