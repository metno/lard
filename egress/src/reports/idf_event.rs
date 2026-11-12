use std::sync::{Arc, RwLock};

use chrono::{DateTime, Duration, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use serde::{Deserialize, Serialize};

use crate::{
    Error, patchwork,
    reports::idf_station::{Unit, mm_to_lsha},
};
use util::{PgPool, timerange::ClosedTimerange};

// Params for the IDF event label, the element name is `sum(precipitation_amount PT1M)`
pub const PRECIPITATION_PARAM_ID: i32 = 105;
pub const DEFAULT_LEVEL: Option<i32> = Some(200);
pub const DEFAULT_SENSOR: Option<i32> = Some(0);

/// Maximum duration value (in minutes) we can process
pub const MAX_ALLOWED_DURATION: u32 = 10000;

/// Durations (in minutes) for which the maximum precipitation intensity sum is computed if no duration
/// value is provided in the query parameters
pub const DEFAULT_DURATIONS: &[u32] = &[
    1, 2, 3, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 360, 720, 1440,
];

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
pub struct RainfallDatum {
    timestamp: DateTime<Utc>,
    value: f64,
}

/// Fetches rainfall observations given the vector of timeseries patches
pub async fn fetch_rain_data(
    label: patchwork::Label,
    timerange: ClosedTimerange,
    roles_permit: &[i32],
    roles_station: &[i32],
    pool: PgPool,
    table: Arc<RwLock<patchwork::Table>>,
) -> Result<Vec<RainfallDatum>, Error> {
    let patches =
        patchwork::get_applicable_timeseries(timerange, label, roles_permit, roles_station, table)?;

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
fn calculate_idf_event(duration: u32, data: &[RainfallDatum], unit: Unit) -> IdfEvent {
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

    if unit == Unit::Lsha {
        maximum.intensity = mm_to_lsha(maximum.intensity, duration)
    }

    maximum
}

// TODO: should spawn in separate thread and use par_iter instead of into_iter?
pub fn collect_idf_events(
    durations: &[u32],
    data: Vec<RainfallDatum>,
    unit: Unit,
) -> Vec<IdfEvent> {
    durations
        .iter()
        .map(|&duration| calculate_idf_event(duration, &data, unit))
        .collect()
}

pub fn is_idf_event_timeseries(label: &patchwork::Label) -> bool {
    label.param_id == PRECIPITATION_PARAM_ID
        && label.level == DEFAULT_LEVEL
        && label.sensor == DEFAULT_SENSOR
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

        let values: Vec<_> = collect_idf_events(&durations, data, Unit::Mm);

        assert_eq!(values.len(), expected.len());

        for (val, exp) in values.into_iter().zip(expected) {
            assert_eq!(val, exp);
        }
    }
}
