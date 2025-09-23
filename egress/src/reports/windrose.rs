use std::sync::{Arc, RwLock};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use postgres_types::FromSql;
use serde::{Deserialize, Serialize};
use util::{deserialize::optional_comma_separated, DbPools, PgPool, PooledPgConn};

use crate::{
    error::{internal_error, Error},
    patchwork::{self, Patch, PatchworkLabel, PatchworkTables, PatchworkTimeseriesTable},
};

const WIND_SPEED_PARAM_ID: i32 = 81;
const WIND_DIRECTION_PARAM_ID: i32 = 61;
const DEFAULT_LEVEL: Option<i32> = Some(1000);
const DEFAULT_SENSOR: Option<i32> = Some(0);

const WIND_SPEED_LABELS: &[&str] = &[
    "0.3-1.5",
    "1.6-3.3",
    "3.4-5.4",
    "5.5-7.9",
    "8.0-10.7",
    "10.8-13.8",
    "13.9-17.1",
    "17.2-20.7",
    "20.8-24.4",
    "24.5-28.4",
    "28.5-32.6",
    ">32.6",
];

const WIND_DIRECTION_LABELS: &[&str] = &[
    "348.75-11.25",
    "11.25-33.75",
    "33.75-56.25",
    "56.25-78.75",
    "78.75-101.25",
    "101.25-123.75",
    "123.75-146.25",
    "146.25-168.75",
    "168.75-191.25",
    "191.25-213.75",
    "213.75-236.25",
    "236.25-258.75",
    "258.75-281.25",
    "281.25-303.75",
    "303.75-326.25",
    "326.25-348.75",
];

// Round float to 2 decimal digits
fn round(value: f64) -> f64 {
    (value * 100.0).round() * 1e-2
}

fn compute_normalized_hists(
    hist: Vec<Vec<f64>>,
    inv_norm_factor: f64,
) -> (Vec<Vec<f64>>, Vec<f64>, Vec<f64>) {
    let x_size = hist.len();
    let y_size = hist[0].len();

    let mut normalized_hist = vec![vec![0.0; y_size]; x_size];
    let mut x_hist = vec![0.0; x_size];
    let mut y_hist = vec![0.0; y_size];

    for (i, x) in hist.into_iter().enumerate() {
        for (j, val) in x.into_iter().enumerate() {
            let norm = val * inv_norm_factor;

            // Sum over rows
            x_hist[i] += norm;

            // Sum over columns
            y_hist[j] += norm;

            // Round the normalized value to two decimal places
            // Needs to be done last to preserve precision in the sums
            normalized_hist[i][j] = round(norm)
        }

        // Round the row sum
        x_hist[i] = round(x_hist[i]);
    }

    // Round the column sums
    for sum in y_hist.iter_mut() {
        *sum = round(*sum)
    }

    (normalized_hist, x_hist, y_hist)
}

// Variable bin size axis with overflow bin
#[derive(Debug)]
struct VariableAxis {
    edges: Vec<f64>,
}

impl VariableAxis {
    fn new(edges: Vec<f64>) -> Self {
        Self { edges }
    }

    /// Return the index of the bin where the input value falls into
    // TODO: could do binary search but probably not a huge deal with < 20 items
    fn index(&self, coordinate: f64) -> usize {
        // We skip the first edge since that's the threshold for silent wind
        self.edges[1..]
            .iter()
            .position(|x| coordinate < *x)
            .unwrap_or(self.nbins() - 1)
    }

    fn nbins(&self) -> usize {
        self.edges.len()
    }

    fn first(&self) -> f64 {
        self.edges[0]
    }
}

// Axis with uniform cyclic bins.
// The last bin wraps around to the first one.
// Inserted values are assumed to be in range, no explicit re-centering is performed.
#[derive(Debug)]
struct CyclicAxis {
    nbins: usize,
    low: f64,
    high: f64,
    step: f64,
}

impl CyclicAxis {
    fn new(nbins: usize, low: f64, step: f64) -> Self {
        let high = (nbins - 1) as f64 * step + low;

        Self {
            nbins,
            low,
            high,
            step,
        }
    }

    fn nbins(&self) -> usize {
        self.nbins
    }

    /// Return the index of the bin where the input value falls into
    fn index(&self, value: f64) -> usize {
        if value < self.low || value >= self.high {
            return 0;
        }

        let steps = (value - self.low) / self.step;

        steps as usize + 1
    }
}

/// Special wind categories that are calculted together with the windrose histogram
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WindCategories {
    /// Percentage of observations that are below a certain threshold of wind speed
    silent_wind: f64,
    /// Percentage of observation where the wind direction could not be estimated (it has a
    /// negative value)
    variable_wind: f64,
}

impl WindCategories {
    pub fn new(silent_wind: f64, variable_wind: f64) -> Self {
        Self {
            silent_wind,
            variable_wind,
        }
    }
}

/// A 2D histogram of wind speed vs wind direction.
/// The X-axis (wind speed) has variable sized bins, while the Y-axis (wind direction) has uniform
/// cyclic bins.
struct Windrose {
    /// Values of the 2D histogram
    hist: Vec<Vec<f64>>,
    /// Histogram of the wind speeds
    speed_hist: Vec<f64>,
    /// Histogram of the wind directions
    direction_hist: Vec<f64>,
    /// Categories for non standard observation values that need to be accounted for separately
    wind_categories: WindCategories,
    /// Total number of observations used to create the histograms
    total_obs: usize,
}

impl Windrose {
    /// Axes used at MET Norway
    fn default_axes() -> (VariableAxis, CyclicAxis) {
        let x_axis = VariableAxis::new(vec![
            0.3, 1.6, 3.4, 5.5, 8.0, 10.8, 13.9, 17.2, 20.8, 24.5, 28.5, 32.6,
        ]);

        let y_axis = CyclicAxis::new(16, 11.25, 22.5);
        (x_axis, y_axis)
    }

    /// Compute the windrose histogram using the given axes and the daily aggregated wind data from LARD
    fn new_from_days((x_axis, y_axis): (VariableAxis, CyclicAxis), days: Vec<WindDay>) -> Self {
        let mut hist = vec![vec![0.0; y_axis.nbins()]; x_axis.nbins()];
        let mut total_obs = 0;
        let mut silent_wind = 0.0;
        let mut variable_wind = 0.0;

        let n_days = days.len();

        // We multiply by 100.0 to convert to percentage
        let inv_norm_factor = 100.0 / n_days as f64;

        // Calculate 2D histogram
        for day in days {
            let n_obs = day.observations.len();

            // Observations in each day sum up to 1.0 (each day weighs the same)
            let weight = 1.0 / n_obs as f64;

            total_obs += n_obs;

            for obs in day.observations {
                // Check if we are below the silent wind threshold
                if obs.speed < x_axis.first() {
                    silent_wind += weight;
                    continue;
                }

                // Negative wind direction means that the observation
                // could not be generated/does not make sense
                if obs.direction < 0.0 {
                    variable_wind += weight;
                    continue;
                }

                let i = x_axis.index(obs.speed);
                let j = y_axis.index(obs.direction);

                hist[i][j] += weight;
            }
        }

        // Normalize values by number of days and compute 1D histograms
        // of wind speed and wind direction
        let (hist, speed_hist, direction_hist) = compute_normalized_hists(hist, inv_norm_factor);

        let wind_categories = WindCategories {
            silent_wind: round(silent_wind * inv_norm_factor),
            variable_wind: round(variable_wind * inv_norm_factor),
        };

        Self {
            hist,
            speed_hist,
            direction_hist,
            total_obs,
            wind_categories,
        }
    }
}

#[derive(Debug, Clone, FromSql)]
#[postgres(name = "windobs")]
struct WindObs {
    speed: f64,
    direction: f64,
}

#[cfg(test)]
impl WindObs {
    fn new(speed: f64, direction: f64) -> Self {
        Self { speed, direction }
    }
}

// A day of wind observations aggregated from LARD
#[derive(Debug, Clone)]
struct WindDay {
    observations: Vec<WindObs>,
}

/// Query parameter for reports/windrose/{station_id} endpoint
#[derive(Debug, Serialize, Deserialize)]
pub struct WindroseParams {
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
    #[serde(default, deserialize_with = "optional_comma_separated")]
    months: Option<Vec<i32>>,
}

/// Metadata returned with the response
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
    station_id: i32,
    number_of_values: usize,
    // TOOD: not sure this is what we want?
    #[serde(skip_serializing_if = "Option::is_none")]
    months: Option<Vec<i32>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Axis {
    /// Axis labels
    #[serde(skip_deserializing)]
    labels: &'static [&'static str],
    /// 1D histogram values
    pub sums: Vec<f64>,
}

/// Response from reports/windrose/{station_id} endpoint
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindroseResp {
    pub wind_speed: Axis,
    pub wind_direction: Axis,
    pub extras: WindCategories,
    pub table: Vec<Vec<f64>>,
    pub metadata: Metadata,
}

/// Aggregate hourly wind speed and wind direction observations by day
/// NOTE: edge cases
/// 1. When wind speed is 0, wind direction is also 0
/// 2. Wind direction can be negative. These are special values to indicate that either the
///    measurement could not be taken out or the result is non-sense, so they are not actually observations.
///    In these cases the data points fall into the 'variable wind' category.
// TODO: normal windroses are calculated from hourly observations, but for some stations, SVV for
// example, we don't have hourly observations. Verify that this query works for those cases or need
// to implement separate algorithm
// NOTE: this query only works if the timeseries are both open or both restricted,
// if they are somehow mixed we need to implement it manually
async fn get_wind_days(
    patches: Vec<WindPatch>,
    months: &Option<Vec<i32>>,
    conn: &PooledPgConn<'_>,
) -> Result<Vec<WindDay>, Error> {
    // The windrose calculation requires
    // - data that has been QCed (lines with `corrected`)
    // - non erroneous data (lines with `quality_code`)
    // - hourly data (we extract only data where the minute field is 0,
    //   in case the timeseries has higher resolution)
    // - we only keep observations that have matching obstime for wind speed and wind direction
    // - optionally we select only the requested months
    // - finally we group by day, so we can easily calculate each observation's weight,
    //   since they are weighted by day
    // TODO: there's probably a better way to do this query?
    let query = conn
        .prepare(
            "SELECT \
                DATE_TRUNC('day', obstime) AS day, \
                ARRAY_AGG((speed.obs, direction.obs)::windobs) \
            FROM ( \
                SELECT obstime, corrected AS obs FROM legacy.data \
                WHERE timeseries = $1 \
                AND corrected IS NOT NULL \
                AND corrected > -30000.0 \
                AND quality_code IS NOT NULL \
                AND quality_code != 7 \
                AND EXTRACT(minute FROM obstime)::int = 0 \
                AND obstime >= $4 AND obstime < $5 \
                AND ($3::int[] IS NULL OR EXTRACT(month FROM obstime)::int = ANY($3)) \
            ) speed \
            INNER JOIN ( \
                SELECT obstime, corrected AS obs FROM legacy.data \
                WHERE timeseries = $2 \
                AND corrected IS NOT NULL \
                AND corrected > -30000.0 \
                AND quality_code IS NOT NULL \
                AND quality_code != 7 \
                AND EXTRACT(minute FROM obstime)::int = 0 \
                AND obstime >= $4 AND obstime < $5 \
                AND ($3::int[] IS NULL OR EXTRACT(month FROM obstime)::int = ANY($3)) \
            ) direction \
            USING (obstime) \
            GROUP BY day",
        )
        .await?;

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(
                &query,
                &[
                    &patch.speed_tsid,
                    &patch.direction_tsid,
                    &months,
                    &patch.from,
                    &patch.to,
                ],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut days = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            days.push(WindDay {
                observations: row.get(1),
            });
        }
    }

    Ok(days)
}

#[derive(Clone, Default, PartialEq, Debug)]
struct WindPatch {
    speed_tsid: i64,
    direction_tsid: i64,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

// Merge two patchwork timeseries
// TODO: is there a better algorithm? Both vectors should be quite small, so probably this is good
// enough
fn merge_patches(left_patches: Vec<Patch>, right_patches: Vec<Patch>) -> Option<Vec<WindPatch>> {
    if left_patches.is_empty() || right_patches.is_empty() {
        return None;
    }

    let mut patches = vec![];

    for left in left_patches {
        for right in &right_patches {
            // Skip if patches don't overlap
            if left.from >= right.to || left.to <= right.from {
                continue;
            }

            let start = left.from.max(right.from);
            let end = left.to.min(right.to);

            patches.push(WindPatch {
                speed_tsid: left.tsid,
                direction_tsid: right.tsid,
                from: start,
                to: end,
            });
        }
    }

    Some(patches)
}

fn create_default_label(station_id: i32, param_id: i32) -> PatchworkLabel {
    PatchworkLabel::new(station_id, param_id, DEFAULT_LEVEL, DEFAULT_SENSOR)
}

// Helper that finds required patches for wind speed and wind direction timeseries,
// returning corresponding data fetched from LARD
async fn fetch_wind_data(
    station_id: i32,
    params: &WindroseParams,
    roles: &[i32],
    pool: PgPool,
    table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<WindDay>, Error> {
    let speed_label = create_default_label(station_id, WIND_SPEED_PARAM_ID);
    let direction_label = create_default_label(station_id, WIND_DIRECTION_PARAM_ID);

    let speed_patches = patchwork::get_applicable_timeseries(
        params.fromtime,
        params.totime,
        speed_label,
        roles,
        table.clone(),
    )?;

    let direction_patches = patchwork::get_applicable_timeseries(
        params.fromtime,
        params.totime,
        direction_label,
        roles,
        table,
    )?;

    let Some(patches) = merge_patches(speed_patches, direction_patches) else {
        // Cannot query necessary data if there are no timeseries for either wind speed
        // or wind direction
        return Ok(vec![]);
    };

    let conn = pool.get().await?;
    let days = get_wind_days(patches, &params.months, &conn).await?;

    Ok(days)
}

pub async fn windrose_handler(
    Path(station_id): Path<i32>,
    Query(params): Query<WindroseParams>,
    State(pools): State<DbPools>,
    State(tables): State<PatchworkTables>,
    Extension(roles): Extension<Option<Vec<i32>>>,
) -> Result<Json<WindroseResp>, (StatusCode, String)> {
    let r = roles.unwrap_or_default();

    // NOTE: given how permits work at the moment, open and restricted are mutually exclusive
    let (open_data, restricted_data) = tokio::try_join!(
        fetch_wind_data(station_id, &params, &r, pools.open, tables.open),
        fetch_wind_data(station_id, &params, &r, pools.restricted, tables.restricted),
    )
    .map_err(internal_error)?;

    let days = match (open_data.is_empty(), restricted_data.is_empty()) {
        (false, _) => open_data,
        (_, false) => restricted_data,
        (true, true) => {
            return Err((
                StatusCode::NOT_FOUND,
                "no data found for this station".to_string(),
            ))
        }
    };

    // TODO: spawn sync thread here?
    let windrose = Windrose::new_from_days(Windrose::default_axes(), days);

    let metadata = Metadata {
        station_id,
        fromtime: params.fromtime,
        totime: params.totime,
        number_of_values: windrose.total_obs,
        months: params.months,
    };

    Ok(Json(WindroseResp {
        wind_direction: Axis {
            labels: WIND_DIRECTION_LABELS,
            sums: windrose.direction_hist,
        },
        wind_speed: Axis {
            labels: WIND_SPEED_LABELS,
            sums: windrose.speed_hist,
        },
        metadata,
        extras: windrose.wind_categories,
        table: windrose.hist,
    }))
}

#[derive(Debug, Serialize)]
pub struct WindroseAvailable {
    pub station_id: i32,
    permit: i32,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct WindroseAvailabilityResp {
    stations: Vec<WindroseAvailable>,
}

fn is_wind_speed_timeseries(label: &PatchworkLabel) -> bool {
    label.param_id == WIND_SPEED_PARAM_ID
        && label.level == DEFAULT_LEVEL
        && label.sensor == DEFAULT_SENSOR
}

pub async fn windrose_availability_handler(
    State(tables): State<PatchworkTables>,
    Extension(roles): Extension<Option<Vec<i32>>>,
) -> Result<Json<WindroseAvailabilityResp>, (StatusCode, String)> {
    // TODO: not sure how performant this is, maybe we need a different data structure?
    let mut stations: Vec<_> = {
        let ot = tables.open.read().map_err(internal_error)?;

        ot.iter()
            .filter(|(label, _)| is_wind_speed_timeseries(label))
            // Check that the filtered stations also have a corresponding wind direction timeseries
            // NOTE: this only works if the timeseries are both open or both restricted,
            // if they are somehow mixed we need to implement something different
            .filter_map(|(label, speed)| {
                let direction = ot.get(&create_default_label(
                    label.station_id,
                    WIND_DIRECTION_PARAM_ID,
                ))?;

                Some((label, speed, direction))
            })
            .map(|(label, speed, direction)| {
                let speed_to = speed.iter().last().unwrap().to;
                let direction_to = direction.iter().last().unwrap().to;

                let to = speed_to.min(direction_to);
                let from = speed[0].from.max(direction[0].from);

                WindroseAvailable {
                    station_id: label.station_id,
                    permit: speed[0].permit,
                    from,
                    to,
                }
            })
            .collect()
    };

    if let Some(roles) = roles {
        let rt = tables.restricted.read().map_err(internal_error)?;

        stations.extend(
            rt.iter()
                .filter(|(label, _)| is_wind_speed_timeseries(label))
                // NOTE: All fills should have the same permit id since restrictions are applied
                // to whole stations or single params
                .filter(|(_, fills)| roles.contains(&fills[0].permit))
                // Check that the filtered stations also have a corresponding wind direction timeseries
                .filter_map(|(label, speed)| {
                    let direction = rt.get(&create_default_label(
                        label.station_id,
                        WIND_DIRECTION_PARAM_ID,
                    ))?;

                    Some((label, speed, direction))
                })
                // Check that both timeseries have the same permit
                .filter(|(_, speed, direction)| speed[0].permit == direction[0].permit)
                .map(|(label, speed, direction)| {
                    let speed_to = speed.iter().last().unwrap().to;
                    let direction_to = direction.iter().last().unwrap().to;

                    let to = speed_to.min(direction_to);
                    let from = speed[0].from.max(direction[0].from);

                    WindroseAvailable {
                        station_id: label.station_id,
                        permit: speed[0].permit,
                        from,
                        to,
                    }
                }),
        );
    }

    Ok(Json(WindroseAvailabilityResp { stations }))
}

#[cfg(test)]
mod test {
    use chrono::{Duration, TimeZone};

    use super::*;

    struct ExpectedWindrose {
        x_sum: Vec<f64>,
        y_sum: Vec<f64>,
        hist: Vec<Vec<f64>>,
        category: WindCategories,
    }

    fn test_values_and_sums(
        days: Vec<WindDay>,
        axes: (VariableAxis, CyclicAxis),
        expected: ExpectedWindrose,
    ) {
        let windrose = Windrose::new_from_days(axes, days);

        assert_eq!(windrose.hist, expected.hist);
        assert_eq!(windrose.wind_categories, expected.category);
        assert_eq!(windrose.speed_hist, expected.x_sum);
        assert_eq!(windrose.direction_hist, expected.y_sum);
    }

    #[test]
    fn test_single_day() {
        let days = vec![WindDay {
            observations: vec![
                WindObs::new(0.1, 220.0),
                WindObs::new(0.4, 30.0),
                WindObs::new(21.0, 330.0),
                WindObs::new(37.0, 15.0),
            ],
        }];

        let axes = (
            VariableAxis::new(vec![0.3, 1.0, 30.]), // [0.3, 1.0) [1.0, 30.0) [30.0, +inf)
            CyclicAxis::new(3, 0.0, 120.0),         // [240.0, 0.0) [0.0, 120.0) [120.0, 240.0)
        );

        let expected = ExpectedWindrose {
            x_sum: vec![25.0; 3],
            y_sum: vec![25.0, 50.0, 0.0],
            hist: vec![
                vec![0.0, 25.0, 0.0],
                vec![25.0, 0.0, 0.0],
                vec![0.0, 25.0, 0.0],
            ],
            category: WindCategories {
                silent_wind: 25.0,
                variable_wind: 0.0,
            },
        };

        test_values_and_sums(days, axes, expected);
    }

    #[test]
    fn test_multiple_days() {
        let days = vec![
            WindDay {
                // this weighs 1
                observations: vec![WindObs::new(0.5, 220.0)],
            },
            WindDay {
                // these weigh 0.33 each
                observations: vec![
                    WindObs::new(10.0, 30.0),
                    WindObs::new(10.0, 330.0),
                    WindObs::new(10.0, 15.0),
                ],
            },
        ];

        let axes = (
            VariableAxis::new(vec![0.3, 1.0]), // [0.3, 1.0) [1.0, +inf)
            CyclicAxis::new(2, 20.0, 320.0),   // [340.0, 20.0) [20.0, 340.0)
        );

        let expected = ExpectedWindrose {
            x_sum: vec![50., 50.],
            y_sum: vec![16.67, 83.33],
            hist: vec![vec![0.0, 50.0], vec![16.67, 33.33]],
            category: WindCategories {
                silent_wind: 0.0,
                variable_wind: 0.0,
            },
        };

        test_values_and_sums(days, axes, expected);
    }

    #[test]
    fn test_oda_four_values_one_silent() {
        let days = vec![WindDay {
            observations: vec![
                WindObs::new(0.1, 220.0),
                WindObs::new(0.4, 30.0),
                WindObs::new(21.0, 330.0),
                WindObs::new(37.0, 15.0),
            ],
        }];

        let axes = Windrose::default_axes();
        let y_bins = axes.1.nbins();

        let expected = ExpectedWindrose {
            x_sum: vec![25., 0., 0., 0., 0., 0., 0., 0., 25., 0., 0., 25.],

            y_sum: vec![
                0., 50., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 25.,
            ],
            hist: vec![
                vec![
                    0., 25., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                ],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![
                    0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 25.,
                ],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![
                    0., 25., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                ],
            ],
            category: WindCategories {
                silent_wind: 25.0,
                variable_wind: 0.0,
            },
        };

        test_values_and_sums(days, axes, expected);
    }

    #[test]
    fn test_oda_skip_month() {
        let days = vec![
            WindDay {
                observations: vec![WindObs::new(0.1, 220.0)],
            },
            WindDay {
                observations: vec![WindObs::new(0.4, 30.0)],
            },
            WindDay {
                observations: vec![WindObs::new(37.0, 15.0)],
            },
        ];

        let axes = Windrose::default_axes();
        let y_bins = axes.1.nbins();

        let expected = ExpectedWindrose {
            x_sum: vec![33.33, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 33.33],
            y_sum: vec![
                0., 66.67, 0., 0., 0., 0., 0.0, 0., 0., 0., 0., 0., 0., 0., 0., 0.,
            ],
            hist: vec![
                vec![
                    0., 33.33, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                ],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![
                    0., 33.33, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                ],
            ],
            category: WindCategories {
                silent_wind: 33.33,
                variable_wind: 0.0,
            },
        };

        test_values_and_sums(days, axes, expected);
    }

    #[test]
    fn test_merge() {
        struct Case<'a> {
            title: &'a str,
            left: Vec<Patch>,
            right: Vec<Patch>,
            expected: Option<Vec<WindPatch>>,
        }

        let from = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let first = from + Duration::days(10);
        let second = from + Duration::days(15);
        let third = from + Duration::days(20);
        let to = from + Duration::days(30);

        let cases = [
            Case {
                title: "Matching fromto",
                left: vec![
                    Patch {
                        tsid: 1,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 2,
                        from: first,
                        to,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 4,
                        from: first,
                        to,
                    },
                ],
                expected: Some(vec![
                    WindPatch {
                        speed_tsid: 1,
                        direction_tsid: 3,
                        from,
                        to: first,
                    },
                    WindPatch {
                        speed_tsid: 2,
                        direction_tsid: 4,
                        from: first,
                        to,
                    },
                ]),
            },
            Case {
                title: "single left",
                left: vec![Patch { tsid: 1, from, to }],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 4,
                        from: first,
                        to,
                    },
                ],
                expected: Some(vec![
                    WindPatch {
                        speed_tsid: 1,
                        direction_tsid: 3,
                        from,
                        to: first,
                    },
                    WindPatch {
                        speed_tsid: 1,
                        direction_tsid: 4,
                        from: first,
                        to,
                    },
                ]),
            },
            Case {
                title: "single right",
                right: vec![Patch { tsid: 1, from, to }],
                left: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 4,
                        from: first,
                        to,
                    },
                ],
                expected: Some(vec![
                    WindPatch {
                        speed_tsid: 3,
                        direction_tsid: 1,
                        from,
                        to: first,
                    },
                    WindPatch {
                        speed_tsid: 4,
                        direction_tsid: 1,
                        from: first,
                        to,
                    },
                ]),
            },
            Case {
                title: "staggered middle point",
                left: vec![
                    Patch {
                        tsid: 1,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 2,
                        from: first,
                        to,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: third,
                    },
                    Patch {
                        tsid: 4,
                        from: third,
                        to,
                    },
                ],
                expected: Some(vec![
                    WindPatch {
                        speed_tsid: 1,
                        direction_tsid: 3,
                        from,
                        to: first,
                    },
                    WindPatch {
                        speed_tsid: 2,
                        direction_tsid: 3,
                        from: first,
                        to: third,
                    },
                    WindPatch {
                        speed_tsid: 2,
                        direction_tsid: 4,
                        from: third,
                        to,
                    },
                ]),
            },
            Case {
                title: "staggered start",
                left: vec![
                    Patch {
                        tsid: 1,
                        from: first,
                        to: third,
                    },
                    Patch {
                        tsid: 2,
                        from: third,
                        to,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: second,
                    },
                    Patch {
                        tsid: 4,
                        from: second,
                        to,
                    },
                ],
                expected: Some(vec![
                    WindPatch {
                        speed_tsid: 1,
                        direction_tsid: 3,
                        from: first,
                        to: second,
                    },
                    WindPatch {
                        speed_tsid: 1,
                        direction_tsid: 4,
                        from: second,
                        to: third,
                    },
                    WindPatch {
                        speed_tsid: 2,
                        direction_tsid: 4,
                        from: third,
                        to,
                    },
                ]),
            },
            Case {
                title: "staggered end",
                left: vec![
                    Patch {
                        tsid: 1,
                        from: first,
                        to: third,
                    },
                    Patch {
                        tsid: 2,
                        from: third,
                        to,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 4,
                        from: first,
                        to: second,
                    },
                ],
                expected: Some(vec![WindPatch {
                    speed_tsid: 1,
                    direction_tsid: 4,
                    from: first,
                    to: second,
                }]),
            },
        ];

        for case in cases {
            let merged = merge_patches(case.left, case.right);
            assert_eq!(merged, case.expected, "{}", case.title);
        }
    }
}
