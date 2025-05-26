use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use util::PooledPgConn;

use crate::{error, PgConnectionPool};

use super::idf_station::IdfUnit;

const MAX_ALLOWED_DURATION: u32 = 10000;

/// Durations for which the maximum precipitation intensity sum is computed if no duration
/// value is provided in the query parameters
const DEFAULT_DURATIONS: &[u32] = &[
    1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 360, 720, 1440,
];

/// Query parameters struct for the idf/event/{station_id} endpoint
#[derive(Serialize, Deserialize)]
pub struct IdfEventParams {
    #[serde(default)]
    unit: IdfUnit,
    durations: Option<Vec<u32>>,
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
}

/// Response struct returned by the idf/event/{station_id} endpoint
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfEventResp {
    station_id: i32,
    unit: IdfUnit,
    values: Vec<IdfEvent>,
}

// IDF event is defined as the maximum sum of precipitation intensities
// over windows of a given duration
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdfEvent {
    /// Sum of rainfall intensities over a given duration window
    intensity: f64,
    /// The maximum allowed time delta between first and last observations in a window
    duration: u32,
    /// Timestamp of the first observation considered in the sum
    fromtime: DateTime<Utc>,
    /// Timestamp of the last observation considered in the sum
    totime: DateTime<Utc>,
}

// Struct used to deserialize rows fetched from LARD
struct RainfallDatum {
    obstime: DateTime<Utc>,
    obsvalue: f64,
}

/// Fetches all PT1M rainfall observations (param_id = 105) for the given station ID and time range
// TODO: Switch to hourly automatic data, instead of minute data, because old manual data was way
// too approximate. Use typeid 501 to select only hourly?
async fn fetch_rain_data(
    station_id: i32,
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
    conn: &PooledPgConn<'_>,
) -> Result<Vec<RainfallDatum>, tokio_postgres::Error> {
    // TODO: make sure default level is correct
    // TODO: in ODA this uses the met.no/filter label, so we probably need to implement that one
    // instead?
    // TODO: should obstime include totime timestamp?
    let query = "SELECT obstime, corrected FROM legacy.data \
                 JOIN labels.met ON (timeseries) \
                 WHERE station_id = $1 \
                   AND param_id = 105 \
                   AND sensor = 0 \
                   AND level = 200 \
                   AND corrected IS NOT NULL \
                   AND quality_code != 7
                   AND obstime >= $2 AND obstime < $3 \
                 ORDER BY obstime";

    let rows = conn
        .query(query, &[&station_id, &fromtime, &totime])
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| RainfallDatum {
            obstime: row.get(0),
            obsvalue: row.get(1),
        })
        .collect())
}

// Computes the IDF event for the input `duration` using the
// precipitation `data` fetched from LARD.
fn calculate_idf_event(duration: u32, data: &[RainfallDatum]) -> IdfEvent {
    let mut maximum = IdfEvent {
        duration,
        fromtime: DateTime::default(),
        totime: DateTime::default(),
        intensity: f64::NEG_INFINITY,
    };

    // NOTE: unfortunately we can't use a window iterator because the data is not regular
    for (i, val) in data.iter().enumerate() {
        let start_time = val.obstime;
        let cutoff_time = start_time + TimeDelta::minutes(duration as i64);

        // Manually compute the sum of intensities making sure all considered observations fall
        // inside the given cutoff_time
        let (window_sum, end_time) = data[i..]
            .iter()
            .take_while(|v| v.obstime < cutoff_time)
            .fold((0.0, val.obstime), |acc, v| (acc.0 + v.obsvalue, v.obstime));

        if window_sum > maximum.intensity {
            maximum.intensity = window_sum;
            maximum.fromtime = start_time;
            maximum.totime = end_time;
        }
    }

    maximum
}

// TODO: should spawn in separate thread and use par_iter instead of into_iter?
fn collect_idf_events(durations: &[u32], data: Vec<RainfallDatum>) -> Vec<IdfEvent> {
    durations
        .iter()
        .map(|&duration| calculate_idf_event(duration, &data))
        .collect()
}

pub async fn idf_event_handler(
    Path(station_id): Path<i32>,
    State(pool): State<PgConnectionPool>,
    Query(params): Query<IdfEventParams>,
) -> Result<Json<IdfEventResp>, (StatusCode, String)> {
    // We allow any provided duration that is less than `MAX_ALLOWED_DURATION`
    let durations: Option<Vec<u32>> = params.durations.map(|durations| {
        durations
            .into_iter()
            .filter(|d| *d <= MAX_ALLOWED_DURATION)
            .collect()
    });

    // TODO: is there a way to combine these `durations` preprocessing?
    let durations = match durations {
        Some(ref d) => d,
        None => DEFAULT_DURATIONS,
    };

    let conn = pool.get().await.map_err(error::internal_error)?;
    let data = fetch_rain_data(station_id, params.fromtime, params.totime, &conn)
        .await
        .map_err(error::internal_error)?;

    if data.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("No precipitation data found for station {station_id}"),
        ));
    }

    let values = collect_idf_events(durations, data);

    Ok(Json(IdfEventResp {
        station_id,
        unit: params.unit,
        values,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    impl RainfallDatum {
        fn new(year: i32, month: u32, day: u32, hour: u32, min: u32, obsvalue: f64) -> Self {
            Self {
                obstime: Utc
                    .with_ymd_and_hms(year, month, day, hour, min, 0)
                    .unwrap(),
                obsvalue,
            }
        }
    }

    #[test]
    fn test_oda_output_match() {
        let durations = [1, 2, 5, 7, 10, 15];

        let data = vec![
            RainfallDatum::new(2000, 1, 1, 0, 0, 1.),
            RainfallDatum::new(2000, 1, 1, 0, 10, 2.),
            RainfallDatum::new(2000, 1, 1, 0, 11, 3.),
            RainfallDatum::new(2000, 1, 1, 0, 12, 4.),
            RainfallDatum::new(2000, 1, 1, 0, 13, 5.),
            RainfallDatum::new(2000, 1, 1, 0, 14, 6.),
            RainfallDatum::new(2000, 1, 1, 0, 15, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 16, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 17, 14.),
            RainfallDatum::new(2000, 1, 1, 0, 18, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 19, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 20, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 21, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 22, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 23, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 24, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 25, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 30, 8.),
            RainfallDatum::new(2000, 1, 1, 0, 40, 9.),
            RainfallDatum::new(2000, 1, 1, 0, 50, 8.),
            RainfallDatum::new(2000, 1, 1, 0, 51, 7.),
            RainfallDatum::new(2000, 1, 1, 0, 52, 6.),
            RainfallDatum::new(2000, 1, 1, 0, 53, 5.),
            RainfallDatum::new(2000, 1, 1, 0, 54, 4.),
            RainfallDatum::new(2000, 1, 1, 0, 55, 3.),
            RainfallDatum::new(2000, 1, 1, 1, 5, 3.),
            RainfallDatum::new(2000, 1, 1, 1, 15, 23.),
            RainfallDatum::new(2000, 1, 2, 0, 5, 14.),
        ];

        let expected = vec![
            IdfEvent {
                intensity: 23.0,
                duration: 1,
                fromtime: Utc.with_ymd_and_hms(2000, 1, 1, 1, 15, 0).unwrap(),
                totime: Utc.with_ymd_and_hms(2000, 1, 1, 1, 15, 0).unwrap(),
            },
            IdfEvent {
                intensity: 23.0,
                duration: 2,
                fromtime: Utc.with_ymd_and_hms(2000, 1, 1, 1, 15, 0).unwrap(),
                totime: Utc.with_ymd_and_hms(2000, 1, 1, 1, 15, 0).unwrap(),
            },
            IdfEvent {
                intensity: 42.0,
                duration: 5,
                fromtime: Utc.with_ymd_and_hms(2000, 1, 1, 0, 15, 0).unwrap(),
                totime: Utc.with_ymd_and_hms(2000, 1, 1, 0, 19, 0).unwrap(),
            },
            IdfEvent {
                intensity: 56.0,
                duration: 7,
                fromtime: Utc.with_ymd_and_hms(2000, 1, 1, 0, 15, 0).unwrap(),
                totime: Utc.with_ymd_and_hms(2000, 1, 1, 0, 21, 0).unwrap(),
            },
            IdfEvent {
                intensity: 77.0,
                duration: 10,
                fromtime: Utc.with_ymd_and_hms(2000, 1, 1, 0, 15, 0).unwrap(),
                totime: Utc.with_ymd_and_hms(2000, 1, 1, 0, 24, 0).unwrap(),
            },
            IdfEvent {
                intensity: 102.0,
                duration: 15,
                fromtime: Utc.with_ymd_and_hms(2000, 1, 1, 0, 11, 0).unwrap(),
                totime: Utc.with_ymd_and_hms(2000, 1, 1, 0, 25, 0).unwrap(),
            },
        ];

        let values: Vec<_> = collect_idf_events(&durations, data);

        assert_eq!(values.len(), expected.len());

        for (val, exp) in values.into_iter().zip(expected.into_iter()) {
            assert_eq!(val, exp);
        }
    }
}
