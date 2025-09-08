use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, TimeDelta, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use serde::{Deserialize, Serialize};
use util::{DbPools, PooledPgConn};

use crate::{
    error::{self, internal_error, not_found_error},
    patchwork::{get_applicable_timeseries, PatchworkLabel, PatchworkTimeseriesTables},
};

use super::idf_station::IdfUnit;

const PRECIPITATION_PARAM_ID: i32 = 105;
// TODO: make sure default level is correct
const DEFAULT_LEVEL: Option<i32> = Some(200);
const DEFAULT_SENSOR: Option<i32> = Some(0);
const MAX_ALLOWED_DURATION: u32 = 10000;

/// Durations (in minutes) for which the maximum precipitation intensity sum is computed if no duration
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

/// An IDF event is defined as the maximum sum of precipitation intensities
/// over windows of a given duration
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
    timestamp: DateTime<Utc>,
    value: f64,
}

/// Fetches all rainfall observations given the vector of timeseries patches
// TODO: this is an adapted version of get_patchwork with two more WHERE conditions.
// Are we fine having separate "patchwork" queries for specific tasks?
async fn fetch_rain_data(
    timeseries: Vec<(i64, DateTime<Utc>, DateTime<Utc>)>,
    conn: &PooledPgConn<'_>,
) -> Result<Vec<RainfallDatum>, tokio_postgres::Error> {
    // TODO: are these timeseries ordered by fromtime?
    let mut futures = timeseries
        .iter()
        .map(|(tsid, from, to)| async move {
            conn.query(
                "SELECT obstime, corrected, \
                 FROM legacy.data \
                 WHERE timeseries = $1 \
                   AND corrected IS NOT NULL \
                   AND quality_code != 7 \
                   AND obstime BETWEEN $2 AND $3",
                &[&tsid, &from, &to],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();

    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            data.push(RainfallDatum {
                timestamp: row.get(0),
                value: row.get(1),
            });
        }
    }

    Ok(data)
}

/// Computes the IDF event for the input `duration` using the precipitation `data` fetched from LARD.
fn calculate_idf_event(duration: u32, data: &[RainfallDatum]) -> IdfEvent {
    let mut maximum = IdfEvent {
        duration,
        fromtime: DateTime::default(),
        totime: DateTime::default(),
        intensity: f64::NEG_INFINITY,
    };

    // NOTE: unfortunately we can't use a window iterator because the data is not regular
    for (i, val) in data.iter().enumerate() {
        let start_time = val.timestamp;
        let cutoff_time = start_time + TimeDelta::minutes(duration as i64);

        // Manually compute the sum of intensities using only observations that fall
        // before the given cutoff time
        let (window_intensity, end_time) = data[i..]
            .iter()
            .take_while(|obs| obs.timestamp < cutoff_time)
            .fold((0.0, val.timestamp), |acc, obs| {
                (acc.0 + obs.value, obs.timestamp)
            });

        if window_intensity > maximum.intensity {
            maximum.intensity = window_intensity;
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
    State(pools): State<DbPools>,
    State(tables): State<PatchworkTimeseriesTables>,
    Query(params): Query<IdfEventParams>,
) -> Result<Json<IdfEventResp>, (StatusCode, String)> {
    // We allow any provided duration that is less or equal to `MAX_ALLOWED_DURATION`
    let durations: Option<Vec<u32>> = params.durations.map(|durations| {
        durations
            .into_iter()
            .filter(|d| *d <= MAX_ALLOWED_DURATION)
            .collect()
    });

    let durations = durations.as_deref().unwrap_or(DEFAULT_DURATIONS);

    let label = PatchworkLabel::new(
        station_id,
        PRECIPITATION_PARAM_ID,
        DEFAULT_LEVEL,
        DEFAULT_SENSOR,
    );

    let timeseries = get_applicable_timeseries(
        params.fromtime,
        params.totime,
        label,
        tables.open,
        Some(tables.restricted),
    )
    .map_err(not_found_error)?
    // TODO: this should not return an option?
    .unwrap();

    // TODO: this should be handled by auth
    let conn = pools.open.get().await.map_err(error::internal_error)?;

    let data = fetch_rain_data(timeseries, &conn)
        .await
        .map_err(internal_error)?;

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
        fn new(year: i32, month: u32, day: u32, hour: u32, min: u32, value: f64) -> Self {
            Self {
                timestamp: Utc
                    .with_ymd_and_hms(year, month, day, hour, min, 0)
                    .unwrap(),
                value,
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
