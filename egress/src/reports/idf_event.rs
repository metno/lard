use std::sync::{Arc, RwLock};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Duration, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    error::{internal_error, Error},
    patchwork::{self, PatchworkTables, PatchworkTimeseriesTable},
    reports::idf_station::mm_to_lsha,
};
use util::{deserialize::optional_comma_separated, DbPools, PatchworkLabel, PgPool};

use super::idf_station::IdfUnit;

// Params for the IDF event label, the element name is `sum(precipitation_amount PT1M)`
const PRECIPITATION_PARAM_ID: i32 = 105;
const DEFAULT_LEVEL: Option<i32> = Some(200);
const DEFAULT_SENSOR: Option<i32> = Some(0);

/// Maximum duration value (in minutes) we can process
const MAX_ALLOWED_DURATION: u32 = 10000;

/// Durations (in minutes) for which the maximum precipitation intensity sum is computed if no duration
/// value is provided in the query parameters
pub const DEFAULT_DURATIONS: &[u32] = &[
    1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 360, 720, 1440,
];

/// Query parameters struct for the idf/event/{station_id} endpoint
#[derive(Serialize, Deserialize)]
pub struct IdfEventParams {
    #[serde(default)]
    unit: IdfUnit,
    #[serde(default, deserialize_with = "optional_comma_separated")]
    durations: Option<Vec<u32>>,
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
}

/// Response struct returned by the idf/event/{station_id} endpoint
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfEventResp {
    pub station_id: i32,
    pub unit: IdfUnit,
    pub values: Vec<IdfEvent>,
}

/// An IDF event is defined as the maximum sum of precipitation intensities
/// over windows of a given duration
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfEvent {
    /// Sum of rainfall intensities over a given duration window
    intensity: f64,
    /// The maximum allowed time delta between first and last observations in a window
    duration: u32,
    /// Timestamp of the first observation considered in the sum
    fromtime: DateTime<Utc>,
    /// Timestamp of the last observation considered in the sum
    totime: DateTime<Utc>,
}

impl IdfEvent {
    pub fn new(
        intensity: f64,
        duration: u32,
        fromtime: DateTime<Utc>,
        totime: DateTime<Utc>,
    ) -> Self {
        Self {
            intensity,
            duration,
            fromtime,
            totime,
        }
    }
}

// Struct used to deserialize rows fetched from LARD
#[derive(Debug)]
struct RainfallDatum {
    timestamp: DateTime<Utc>,
    value: f64,
}

/// Fetches rainfall observations given the vector of timeseries patches
async fn fetch_rain_data(
    label: PatchworkLabel,
    params: &IdfEventParams,
    roles_permit: &[i32],
    roles_station: &[i32],
    pool: PgPool,
    table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<RainfallDatum>, Error> {
    let patches = patchwork::get_applicable_timeseries(
        params.fromtime,
        params.totime,
        label,
        roles_permit,
        roles_station,
        table,
    )?;

    if patches.is_empty() {
        return Ok(vec![]);
    }

    let conn = pool.get().await?;

    // The IDF event calculation requires
    // - data that has been QCed (lines with `corrected`)
    // - non erroneous data (quality_code != 7)
    let query = conn
        .prepare(
            "SELECT obstime, corrected \
                FROM legacy.data \
                WHERE timeseries = $1 \
                    AND corrected IS NOT NULL \
                    AND corrected > -30000.0 \
                    AND quality_code != 7 \
                    AND obstime >= $2 \
                    AND obstime < $3 \
                ORDER BY obstime",
        )
        .await?;

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(&query, &[&patch.tsid, &patch.from, &patch.to])
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

/// Computes the IDF event (maximum sum of precipitation intensities over windows of
/// a given duration) for the input `duration` using the precipitation `data` fetched from LARD.
// TODO: there's a linear implementation that we could use in case this is too slow
fn calculate_idf_event(duration: u32, data: &[RainfallDatum], unit: IdfUnit) -> IdfEvent {
    let mut maximum = IdfEvent {
        duration,
        fromtime: DateTime::default(),
        totime: DateTime::default(),
        intensity: f64::NEG_INFINITY,
    };

    // NOTE: unfortunately we can't use a window iterator because the data is not regular
    for (i, val) in data.iter().enumerate() {
        let start_time = val.timestamp;
        let cutoff_time = start_time + Duration::minutes(duration as i64);

        // Manually compute the sum of intensities using only observations that fall
        // before the given cutoff time
        let (window_intensity, end_time) = data[i..]
            .iter()
            .take_while(|obs| obs.timestamp < cutoff_time)
            .fold((0.0, start_time), |acc, obs| {
                (acc.0 + obs.value, obs.timestamp)
            });

        if window_intensity > maximum.intensity {
            maximum.intensity = window_intensity;
            maximum.fromtime = start_time;
            maximum.totime = end_time;
        }
    }

    if unit == IdfUnit::Lsha {
        maximum.intensity = mm_to_lsha(maximum.intensity, duration)
    }

    maximum
}

// TODO: should spawn in separate thread and use par_iter instead of into_iter?
fn collect_idf_events(durations: &[u32], data: Vec<RainfallDatum>, unit: IdfUnit) -> Vec<IdfEvent> {
    durations
        .iter()
        .map(|&duration| calculate_idf_event(duration, &data, unit))
        .collect()
}

pub async fn idf_event_handler(
    Path(station_id): Path<i32>,
    State(pools): State<DbPools>,
    State(tables): State<PatchworkTables>,
    Query(params): Query<IdfEventParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<IdfEventResp>, (StatusCode, String)> {
    let idf_label = PatchworkLabel::new(
        station_id,
        PRECIPITATION_PARAM_ID,
        DEFAULT_LEVEL,
        DEFAULT_SENSOR,
    );

    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let (open_data, restricted_data) = tokio::try_join!(
        fetch_rain_data(
            idf_label,
            &params,
            &roles_permit,
            &roles_station,
            pools.open,
            tables.open
        ),
        fetch_rain_data(
            idf_label,
            &params,
            &roles_permit,
            &roles_station,
            pools.restricted,
            tables.restricted
        ),
    )
    .map_err(internal_error)?;

    // NOTE: given how permits work at the moment, these are mutually exclusive
    let data = match (open_data.is_empty(), restricted_data.is_empty()) {
        (false, _) => open_data,
        (_, false) => restricted_data,
        (true, true) => {
            return Err((
                StatusCode::NOT_FOUND,
                "no data found for this station".to_string(),
            ))
        }
    };

    // We allow any provided duration that is less or equal to `MAX_ALLOWED_DURATION`
    let inputs: Option<Vec<u32>> = params.durations.map(|values| {
        values
            .into_iter()
            .filter(|val| *val <= MAX_ALLOWED_DURATION)
            .collect()
    });

    let durations = inputs.as_deref().unwrap_or(DEFAULT_DURATIONS);
    let values = collect_idf_events(durations, data, params.unit);

    Ok(Json(IdfEventResp {
        station_id,
        unit: params.unit,
        values,
    }))
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct IdfEventAvailable {
    pub station_id: i32,
    permit: i32,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

impl IdfEventAvailable {
    pub fn new(
        station_id: i32,
        permit: i32,
        from: DateTime<Utc>,
        to: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            station_id,
            permit,
            from,
            to,
        }
    }
}

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct IdfEventAvailabilityResp {
    /// List of stations for which IDF calculation is possible
    pub stations: Vec<IdfEventAvailable>,
}

fn is_idf_event_timeseries(label: &PatchworkLabel) -> bool {
    label.param_id == PRECIPITATION_PARAM_ID
        && label.level == DEFAULT_LEVEL
        && label.sensor == DEFAULT_SENSOR
}

pub async fn idf_event_availability_handler(
    State(tables): State<PatchworkTables>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<IdfEventAvailabilityResp>, (StatusCode, String)> {
    let ot = tables.open.read().map_err(internal_error)?;

    let mut stations: Vec<_> = ot
        .iter()
        .filter(|(label, _)| is_idf_event_timeseries(label))
        .map(|(label, fills)| IdfEventAvailable {
            station_id: label.station_id,
            permit: fills[0].permit,
            from: fills[0].from,
            to: fills.iter().last().unwrap().to,
        })
        .collect();

    if let Some((roles_permit, roles_station)) = roles {
        let rt = tables.restricted.read().map_err(internal_error)?;

        stations.extend(
            rt.iter()
                .filter(|(label, _)| is_idf_event_timeseries(label))
                // NOTE: All fills should have the same permit id since restrictions are applied to whole
                // stations or single params
                .filter(|(label, fills)| {
                    roles_permit.contains(&fills[0].permit)
                        || roles_station.contains(&label.station_id)
                })
                .map(|(label, fills)| IdfEventAvailable {
                    station_id: label.station_id,
                    permit: fills[0].permit,
                    from: fills[0].from,
                    to: fills.iter().last().unwrap().to,
                }),
        );
    }

    // TODO: should this be sorted by station_id?
    Ok(Json(IdfEventAvailabilityResp { stations }))
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

        let values: Vec<_> = collect_idf_events(&durations, data, IdfUnit::Mm);

        assert_eq!(values.len(), expected.len());

        for (val, exp) in values.into_iter().zip(expected.into_iter()) {
            assert_eq!(val, exp);
        }
    }
}
