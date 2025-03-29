use crate::{
    internal_error,
    timeseries::{get_timeseries_info, TimeseriesInfo},
    PgConnectionPool,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use chronoutil::RelativeDuration;
use serde::{Deserialize, Serialize};
use util::{type_id_to_time_resolution, PooledPgConn};

#[derive(Debug, Deserialize)]
pub enum AggregationType {
    Max,
    Min,
    Avg,
}

// TODO: Just use RelativeDuration?
#[derive(Debug, Deserialize, Clone, Copy)]
pub enum AggregationPeriod {
    Hourly,
    Diurnal,
    Daily,
    Monthly,
    Yearly,
}

impl Into<RelativeDuration> for AggregationPeriod {
    fn into(self) -> RelativeDuration {
        match self {
            Self::Hourly => RelativeDuration::hours(1),
            Self::Diurnal => RelativeDuration::hours(12),
            Self::Daily => RelativeDuration::days(1),
            Self::Monthly => RelativeDuration::months(1),
            Self::Yearly => RelativeDuration::years(1),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AggregationParams {
    agg_type: AggregationType,
    period: AggregationPeriod,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregationResp {
    // TODO: Does this need to be a vec?
    pub aggregations: Vec<Aggregation>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {
    pub data: Vec<Option<f64>>,
    // pub header: TimeseriesInfo,
    start_time: DateTime<Utc>,
    // time_resolution: String,
}

pub async fn aggregation_handler(
    State(pool): State<PgConnectionPool>,
    // TODO: Should param here be something other than param_id? Perhaps some kind of abstract
    // param that represents several specific ones, like temperature instead of TA, TAX, TAM, TAN, etc.
    Path((station_id, param_id)): Path<(i32, i32)>,
    Query(query_params): Query<AggregationParams>,
) -> Result<Json<AggregationResp>, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let target_timeres: RelativeDuration = query_params.period.into();

    let headers = get_timeseries_info(&conn, station_id, param_id)
        .await
        .map_err(internal_error)?;

    let (_source_timeseres, source_header): (RelativeDuration, TimeseriesInfo) = headers
        .into_iter()
        // Keep only timeseries with type_ids that map to resolutions smaller than the target
        .filter_map(|header| {
            type_id_to_time_resolution(header.type_id)
                .filter(|time_res| *time_res < target_timeres)
                .map(|time_resolution| (time_resolution, header))
        })
        // Take the one left with the largest resolution
        // TODO: Is this right? In the case of a station-aggregated ts it probably is, but if it's
        // instantaneous measurements, we probably want the smallest?
        .max_by_key(|header| header.0)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "No matching timeseries found".to_string(),
            )
        })?;

    // TODO: Rounding of start and end time?
    let start_time = query_params.start_time.unwrap_or(source_header.fromtime);
    let end_time = query_params.end_time.unwrap_or(source_header.totime);

    let agg = get_aggregation(
        &conn,
        source_header,
        start_time,
        end_time,
        query_params.agg_type,
        query_params.period,
    )
    .await
    .map_err(internal_error)?;

    Ok(Json(AggregationResp {
        aggregations: vec![agg],
    }))
}

async fn get_aggregation(
    conn: &PooledPgConn<'_>,
    header: TimeseriesInfo,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    // time_resolution: String,
    agg_type: AggregationType,
    target_timeres: AggregationPeriod,
) -> Result<Aggregation, tokio_postgres::Error> {
    let agg_func = match agg_type {
        AggregationType::Max => "max",
        AggregationType::Min => "min",
        AggregationType::Avg => "avg",
    };
    // TODO: Should we be doing timezone correction here?
    let time_binning = match target_timeres {
        AggregationPeriod::Hourly => "date_trunc('hour', obstime)",
        AggregationPeriod::Diurnal => {
            "date_bin('6 hours', obstime, TIMESTAMP '2000-01-01 00:00:00')"
        }
        AggregationPeriod::Daily => "date_trunc('day', obstime)",
        AggregationPeriod::Monthly => "date_trunc('month', obstime)",
        AggregationPeriod::Yearly => "date_trunc('year', obstime)",
    };
    let query_string = format!(
        r#"
        SELECT
            {}(obsvalue),
            {} as time_bin
        FROM legacy.data
        WHERE
            timeseries = $1 AND
            obstime BETWEEN $2 AND $3",
        GROUP BY time_bin
        "#,
        agg_func, time_binning
    );

    let agg_results = conn
        .query(
            query_string.as_str(),
            &[&header.ts_id, &start_time, &end_time],
        )
        .await?;

    let agg = {
        let mut data = Vec::with_capacity(agg_results.len());

        // TODO: handle gaps in the series
        for row in agg_results {
            data.push(row.get(0));
        }

        Aggregation {
            // header,
            data,
            start_time,
            // time_resolution,
        }
    };

    Ok(agg)
}
