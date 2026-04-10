use crate::Error;
use crate::patchwork::PatchworkTimeseriesTable;
use crate::patchwork::get_applicable_timeseries;
use chrono::{DateTime, Utc};
use chronoutil::RelativeDuration;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use util::{PooledPgConn, TsId, stinfofacade::level};

#[derive(Debug, Deserialize, Clone, Copy)]
pub enum AggregationType {
    Max,
    Min,
    Avg,
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub enum AggregationPeriod {
    Hourly,
    Diurnal,
    Daily,
    Monthly,
    Yearly,
}

impl From<AggregationPeriod> for RelativeDuration {
    fn from(val: AggregationPeriod) -> RelativeDuration {
        match val {
            AggregationPeriod::Hourly => RelativeDuration::hours(1),
            AggregationPeriod::Diurnal => RelativeDuration::hours(12),
            AggregationPeriod::Daily => RelativeDuration::days(1),
            AggregationPeriod::Monthly => RelativeDuration::months(1),
            AggregationPeriod::Yearly => RelativeDuration::years(1),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct AggregationParams {
    agg_type: AggregationType,
    period: AggregationPeriod,
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>, // default to now if not provided
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregationResp {
    // TODO: Does this need to be a vec? It doesn't have to be if we assume that we only return one timeseries
    // which would be the case if using patchwork... but not if directly finding timeseries?
    pub aggregations: Vec<Aggregation>,
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
) -> Result<Vec<AggregationResp>, Error> {
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
    println!("Applicable timeseries: {:?}", applicable_ts);

    // check if we have any applicable timeseries, if not return 404
    if applicable_ts.is_empty() {
        return Err(Error::HttpStatus(StatusCode::NOT_FOUND));
    }

    let agg_func = match params.agg_type {
        AggregationType::Max => "max",
        AggregationType::Min => "min",
        AggregationType::Avg => "avg",
    };

    let time_binning = match params.period {
        AggregationPeriod::Hourly => "date_trunc('hour', obstime)",
        AggregationPeriod::Diurnal => {
            "date_bin('6 hours', obstime, TIMESTAMP '2000-01-01 00:00:00')"
        }
        AggregationPeriod::Daily => "date_trunc('day', obstime)",
        AggregationPeriod::Monthly => "date_trunc('month', obstime)",
        AggregationPeriod::Yearly => "date_trunc('year', obstime)",
    };

    let mut aggregations = Vec::with_capacity(applicable_ts.len());

    // loop over all the tsid and get the aggregation for each, then combine into a single response
    for ts in applicable_ts {
        // TODO: cut down the time to ensure it overlaps with the from/o of the applicable ts
        let agg = get_aggregation_data(
            agg_func,
            time_binning,
            ts.tsid,
            params.from,
            params.to.unwrap_or_else(Utc::now),
            conn,
        )
        .await?;
        aggregations.push(agg);
    }
    Ok(vec![AggregationResp { aggregations }])
}

async fn get_aggregation_data(
    agg_func: &str,
    time_binning: &str,
    tsid: TsId,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    conn: &tokio_postgres::Client,
) -> Result<Aggregation, tokio_postgres::Error> {
    // TODO: figure out how to not do the caculation if missing too much data (highly dependent on timeresolution)
    // See Ketil's comment about how it was done in kdvh triggers:https://codeberg.org/metno/lard/pulls/83
    // TODO: should this use corrected or original??? (or some sort of fallback logic if corrected is missing?)
    // should probably implement quality code filtering like in calculations
    let query_string = format!(
        r#"
        SELECT
            {}(original),
            {} as time_bin
        FROM legacy.data
        WHERE
            timeseries = $1 AND
            obstime BETWEEN $2 AND $3
        GROUP BY time_bin
        "#,
        agg_func, time_binning
    );

    let agg_results = conn
        .query(query_string.as_str(), &[&tsid, &start_time, &end_time])
        .await;

    match agg_results {
        Ok(rows) => {
            let agg = {
                let mut data = Vec::with_capacity(rows.len());

                // TODO: handle gaps in the series
                for row in rows {
                    data.push((row.get(0), row.get(1)));
                }
                Aggregation { data, start_time }
            };
            Ok(agg)
        }
        Err(e) => {
            println!("Error executing aggregation query: {:?}", e);
            Err(e)
        }
    }
}
