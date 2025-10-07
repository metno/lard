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

// Paramters for timeseries labels
const WIND_SPEED_PARAM_ID: i32 = 81;
const WIND_DIRECTION_PARAM_ID: i32 = 61;
const DEFAULT_LEVEL: Option<i32> = Some(1000);
const DEFAULT_SENSOR: Option<i32> = Some(0);

/// Default wind speed axis used at MET
/// Wind speed is measured in [meters per second]
const SPEED_AXIS: VariableAxis = VariableAxis::new(
    &[
        0.3, 1.6, 3.4, 5.5, 8.0, 10.8, 13.9, 17.2, 20.8, 24.5, 28.5, 32.6,
    ],
    Some("Wind speed"),
    Some(&[
        "0.3-1.5",
        "1.5-3.3",
        "3.3-5.4",
        "5.4-7.9",
        "7.9-10.7",
        "10.7-13.8",
        "13.8-17.1",
        "17.1-20.7",
        "20.7-24.4",
        "24.4-28.4",
        "28.4-32.6",
        ">=32.6",
    ]),
);

/// Default wind direction axis used at MET
/// Wind direction is measured in [degrees]
const DIRECTION_AXIS: CyclicAxis = CyclicAxis::new(
    11.25,
    22.5,
    16,
    Some("Wind direction"),
    Some(&[
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
    ]),
);

/// Variable bin size axis with overflow bin (ie the last bin is not closed).
/// This is used to calculate wind speed statistics.
/// Input edges are assumed to be sorted in ascending order.
#[derive(Debug, Serialize, Deserialize)]
struct VariableAxis<'a> {
    /// Axis title
    #[serde(skip_deserializing)]
    title: Option<&'a str>,

    /// Axis labels
    #[serde(skip_deserializing)]
    labels: Option<&'a [&'a str]>,

    /// Vector of bins left edges
    #[serde(skip)]
    edges: &'a [f64],

    #[serde(skip)]
    nbins: usize,
}

impl<'a> VariableAxis<'a> {
    const fn new(edges: &'a [f64], title: Option<&'a str>, labels: Option<&'a [&'a str]>) -> Self {
        let nbins = edges.len();

        Self {
            title,
            labels,
            nbins,
            edges,
        }
    }

    // Return the index of the bin the input value is in
    // TODO: could do binary search but probably not a huge deal with < 20 items
    fn assign_bin(&self, value: f64) -> usize {
        // Skip the first edge since that's the threshold for silent wind
        self.edges[1..]
            .iter()
            .position(|x| value < *x)
            .unwrap_or(self.nbins - 1)
    }

    fn first(&self) -> f64 {
        self.edges[0]
    }
}

/// Axis with uniform cyclic bins, used to calculate wind direction statistics (direction observations are angles).
/// Cyclic means that the last bin wraps around.
/// Inserted values are assumed to be in range, no explicit "re-centering" is performed.
#[derive(Debug, Serialize, Deserialize)]
struct CyclicAxis<'a> {
    /// Axis title
    #[serde(skip_deserializing)]
    title: Option<&'a str>,

    /// Axis labels
    #[serde(skip_deserializing)]
    labels: Option<&'a [&'a str]>,

    /// Number of bins
    #[serde(skip)]
    nbins: usize,

    /// Right side of the first bin
    #[serde(skip)]
    low: f64,

    /// Bin size
    #[serde(skip)]
    step: f64,

    /// Left side of the first bin
    /// This is calculated from the other three fields during initialization
    #[serde(skip)]
    high: f64,
}

impl<'a> CyclicAxis<'a> {
    const fn new(
        low: f64,
        step: f64,
        nbins: usize,
        title: Option<&'a str>,
        labels: Option<&'a [&'a str]>,
    ) -> Self {
        let high = (nbins - 1) as f64 * step + low;

        Self {
            title,
            labels,
            nbins,
            low,
            high,
            step,
        }
    }

    // Return the index of the bin the input value is in
    fn assign_bin(&self, value: f64) -> usize {
        if value < self.low || value >= self.high {
            return 0;
        }

        // Here value is always going to be between
        // self.low and self.high
        let steps = (value - self.low) / self.step;

        (steps as usize + 1) % self.nbins
    }
}

/// Special wind categories that are calculted together with the windrose histogram
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WindCategories {
    /// Percentage of observations that are below a certain threshold of wind speed
    pub silent_wind: f64,
    /// Percentage of observation where the wind direction could not be estimated
    /// (ie, it has a negative value)
    pub variable_wind: f64,
}

impl WindCategories {
    pub fn new(silent_wind: f64, variable_wind: f64) -> Self {
        Self {
            silent_wind,
            variable_wind,
        }
    }
}

/// Type grouping the 1D histogram of wind speed and wind direction, and the combined 2D histogram.
/// The X-axis (wind speed) has variable sized bins, while the Y-axis (wind direction) has uniform
/// cyclic bins.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Windrose<'a> {
    /// Wind speed is the first axis of the 2D histogram
    x_axis: VariableAxis<'a>,

    /// Wind direction is the second axis of the 2D histogram
    y_axis: CyclicAxis<'a>,

    /// Values of the 2D histogram
    #[serde(rename = "table")]
    pub hist: Vec<Vec<f64>>,

    /// Values of the wind speed histogram
    #[serde(rename = "x_sums")]
    pub speed_hist: Vec<f64>,

    /// Values of the wind direction histogram
    #[serde(rename = "y_sums")]
    pub direction_hist: Vec<f64>,

    /// Categories for non standard observation values that need to be accounted for separately
    #[serde(rename = "extra")]
    pub wind_categories: WindCategories,

    /// Total number of observations used to create the histograms
    pub total_obs: usize,
}

impl<'a> Windrose<'a> {
    /// Compute the windrose histogram using the given axes and the daily aggregated wind data from LARD
    fn new_from_days(x_axis: VariableAxis<'a>, y_axis: CyclicAxis<'a>, days: Vec<WindDay>) -> Self {
        let mut windrose = Windrose {
            hist: vec![vec![0.0; y_axis.nbins]; x_axis.nbins],
            speed_hist: vec![0.0; x_axis.nbins],
            direction_hist: vec![0.0; y_axis.nbins],
            total_obs: 0,
            wind_categories: WindCategories {
                silent_wind: 0.0,
                variable_wind: 0.0,
            },
            x_axis,
            y_axis,
        };

        // We multiply by 100.0 to convert to percentage
        let inv_norm_factor = 100.0 / days.len() as f64;

        // Calculate histograms
        for day in days {
            let n_obs = day.observations.len();

            // Observations in each day sum up to 1.0 (each day weighs the same)
            let weight = 1.0 / n_obs as f64;
            let weight = weight * inv_norm_factor;

            windrose.total_obs += n_obs;

            for obs in day.observations {
                // Check if we are below the silent wind threshold
                if obs.speed < windrose.x_axis.first() {
                    windrose.wind_categories.silent_wind += weight;
                    continue;
                }

                // Negative wind direction means that the observation
                // could not be generated/does not make sense
                if obs.direction < 0.0 {
                    windrose.wind_categories.variable_wind += weight;
                    continue;
                }

                let i = windrose.x_axis.assign_bin(obs.speed);
                let j = windrose.y_axis.assign_bin(obs.direction);

                windrose.hist[i][j] += weight;
                windrose.speed_hist[i] += weight;
                windrose.direction_hist[j] += weight;
            }
        }

        windrose
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

/// Aggregate hourly wind speed and wind direction observations by day
/// NOTE: edge cases
/// 1. When wind speed is 0, wind direction is also 0
/// 2. Wind direction can be negative. These are special values to indicate that either the
///    measurement could not be taken out or the result is non-sense, so they are not actually observations.
///    In these cases the data points fall into the 'variable wind' category.
// TODO: normal windroses are calculated from hourly observations, but for some stations, SVV for
// example, we don't have hourly observations. Verify that this query works for those cases or we need
// to implement something different
// NOTE: this query only works if the timeseries are both open or both restricted,
// if they are somehow mixed we need to implement it manually
async fn get_wind_days(
    patches: &[WindPatch],
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

/// Merge the speed and direction timeseries patches
fn merge_patches(speeds: Vec<Patch>, directions: Vec<Patch>) -> Vec<WindPatch> {
    if speeds.is_empty() || directions.is_empty() {
        return vec![];
    }

    let patches = speeds
        .iter()
        .flat_map(|speed| {
            directions.iter().filter_map(|direction| {
                let overlap = direction.overlap(speed)?;

                Some(WindPatch {
                    speed_tsid: speed.tsid,
                    direction_tsid: direction.tsid,
                    from: overlap.from,
                    to: overlap.to,
                })
            })
        })
        .collect();

    patches
}

fn create_default_label(station_id: i32, param_id: i32) -> PatchworkLabel {
    PatchworkLabel::new(station_id, param_id, DEFAULT_LEVEL, DEFAULT_SENSOR)
}

#[derive(Debug, Default)]
struct WindData {
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
    days: Vec<WindDay>,
}

impl WindData {
    fn is_empty(&self) -> bool {
        self.days.is_empty()
    }
}

/// Helper function that finds required patches for wind speed and wind direction timeseries,
/// returning corresponding data fetched from LARD
async fn fetch_wind_data(
    station_id: i32,
    params: &WindroseParams,
    roles: &[i32],
    pool: PgPool,
    table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<WindData, Error> {
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

    let patches = merge_patches(speed_patches, direction_patches);
    if patches.is_empty() {
        // Cannot query necessary data if either
        // - there are no timeseries for wind speed or wind direction
        // - no speed and direction patchwork timeseries overlap
        return Ok(WindData::default());
    };

    let conn = pool.get().await?;
    let days = get_wind_days(&patches, &params.months, &conn).await?;

    Ok(WindData {
        days,
        fromtime: patches.first().unwrap().from,
        totime: patches.last().unwrap().to,
    })
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
    // TOOD: not sure this is what we want?
    #[serde(skip_serializing_if = "Option::is_none")]
    months: Option<Vec<i32>>,
}

/// Response from reports/windrose/{station_id} endpoint
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindroseResp<'a> {
    pub metadata: Metadata,
    pub windrose: Windrose<'a>,
}

pub async fn windrose_handler<'a>(
    Path(station_id): Path<i32>,
    Query(params): Query<WindroseParams>,
    State(pools): State<DbPools>,
    State(tables): State<PatchworkTables>,
    Extension(roles): Extension<Option<Vec<i32>>>,
) -> Result<Json<WindroseResp<'a>>, (StatusCode, String)> {
    let roles = roles.unwrap_or_default();

    // NOTE: given how permits work at the moment, open and restricted are mutually exclusive
    let (open_data, restricted_data) = tokio::try_join!(
        fetch_wind_data(station_id, &params, &roles, pools.open, tables.open),
        fetch_wind_data(
            station_id,
            &params,
            &roles,
            pools.restricted,
            tables.restricted
        ),
    )
    .map_err(internal_error)?;

    let WindData {
        fromtime,
        totime,
        days,
    } = match (open_data.is_empty(), restricted_data.is_empty()) {
        (false, _) => open_data,
        (_, false) => restricted_data,
        (true, true) => {
            return Err((
                StatusCode::NOT_FOUND,
                "no data found for this station".to_string(),
            ))
        }
    };

    let windrose =
        tokio::task::spawn_blocking(|| Windrose::new_from_days(SPEED_AXIS, DIRECTION_AXIS, days))
            .await
            .map_err(internal_error)?;

    let metadata = Metadata {
        station_id,
        fromtime,
        totime,
        months: params.months,
    };

    Ok(Json(WindroseResp { metadata, windrose }))
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct WindroseAvailable {
    station_id: i32,
    permit: i32,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

impl WindroseAvailable {
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

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct WindroseAvailabilityResp {
    pub stations: Vec<WindroseAvailable>,
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

    // Some percentages we expect being returned from the tests
    const ONE_THIRD: f64 = 100.0 / 3.0;
    const TWO_THIRDS: f64 = 2.0 * ONE_THIRD;

    fn is_close(a: f64, b: f64) -> bool {
        const DELTA: f64 = 1e-6;
        (a - b).abs() < DELTA
    }

    fn test_values_and_sums(
        days: Vec<WindDay>,
        x: VariableAxis,
        y: CyclicAxis,
        expected: ExpectedWindrose,
    ) {
        let windrose = Windrose::new_from_days(x, y, days);

        windrose
            .speed_hist
            .into_iter()
            .zip(expected.x_sum)
            .for_each(|(val, exp)| assert!(is_close(val, exp), "{val} {exp}"));

        windrose
            .direction_hist
            .iter()
            .zip(expected.y_sum)
            .for_each(|(val, exp)| assert!(is_close(*val, exp), "{val} {exp}"));

        windrose
            .hist
            .iter()
            .zip(expected.hist)
            .for_each(|(x, x_exp)| {
                x.iter()
                    .zip(x_exp)
                    .for_each(|(val, exp)| assert!(is_close(*val, exp), "{val} {exp}"));
            });

        assert!(is_close(
            windrose.wind_categories.silent_wind,
            expected.category.silent_wind
        ));

        assert!(is_close(
            windrose.wind_categories.variable_wind,
            expected.category.variable_wind
        ));
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

        let x = VariableAxis::new(&[0.3, 1.0, 30.], None, None); // [0.3, 1.0) [1.0, 30.0) [30.0, +inf)
        let y = CyclicAxis::new(0.0, 120.0, 3, None, None); // [240.0, 0.0) [0.0, 120.0) [120.0, 240.0)

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

        test_values_and_sums(days, x, y, expected);
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

        let x = VariableAxis::new(&[0.3, 1.0], None, None); // [0.3, 1.0) [1.0, +inf)
        let y = CyclicAxis::new(20.0, 320.0, 2, None, None); // [340.0, 20.0) [20.0, 340.0)

        let expected = ExpectedWindrose {
            x_sum: vec![50., 50.],
            y_sum: vec![0.5 * ONE_THIRD, 50.0 + ONE_THIRD],
            hist: vec![vec![0.0, 50.0], vec![0.5 * ONE_THIRD, ONE_THIRD]],
            category: WindCategories {
                silent_wind: 0.0,
                variable_wind: 0.0,
            },
        };

        test_values_and_sums(days, x, y, expected);
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

        let y_bins = DIRECTION_AXIS.nbins;

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

        test_values_and_sums(days, SPEED_AXIS, DIRECTION_AXIS, expected);
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

        let y_bins = DIRECTION_AXIS.nbins;

        let third = 100.0 / 3.0;

        let expected = ExpectedWindrose {
            x_sum: vec![ONE_THIRD, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., ONE_THIRD],
            y_sum: vec![
                0., TWO_THIRDS, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
            ],
            hist: vec![
                vec![
                    0., ONE_THIRD, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
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
                    0., third, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                ],
            ],
            category: WindCategories {
                silent_wind: third,
                variable_wind: 0.0,
            },
        };

        test_values_and_sums(days, SPEED_AXIS, DIRECTION_AXIS, expected);
    }

    #[test]
    fn test_merge() {
        struct Case<'a> {
            title: &'a str,
            left: Vec<Patch>,
            right: Vec<Patch>,
            expected: Vec<WindPatch>,
        }

        let from = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let first = from + Duration::days(10);
        let second = from + Duration::days(15);
        let third = from + Duration::days(20);
        let to = from + Duration::days(30);

        let cases = [
            Case {
                title: "No overlap",
                left: vec![
                    Patch {
                        tsid: 1,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 2,
                        from: first,
                        to: second,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from: second,
                        to: third,
                    },
                    Patch {
                        tsid: 4,
                        from: third,
                        to,
                    },
                ],
                expected: vec![],
            },
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
                expected: vec![
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
                ],
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
                expected: vec![
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
                ],
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
                expected: vec![
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
                ],
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
                expected: vec![
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
                ],
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
                expected: vec![
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
                ],
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
                expected: vec![WindPatch {
                    speed_tsid: 1,
                    direction_tsid: 4,
                    from: first,
                    to: second,
                }],
            },
        ];

        for case in cases {
            let merged = merge_patches(case.left, case.right);
            assert_eq!(merged, case.expected, "{}", case.title);
        }
    }

    #[test]
    fn test_cyclic_axis() {
        let axis = CyclicAxis::new(-10.0, 2.0, 20, None, None);

        // Note: these return the same result with or without modulo
        let tests = [
            (-11.0, 0),
            (-10.0, 1),
            (-2.1, 4),
            (2.0, 7),
            (5.2, 8),
            (10.3, 11),
            (27.0, 19),
            (40.4, 0),
            (60.5, 0),
            (f64::INFINITY, 0),
            (f64::NEG_INFINITY, 0),
            (f64::NAN, 1), // returns 1 because (NaN as usize) == 0
        ];

        for (t, exp) in tests.into_iter() {
            let idx = axis.assign_bin(t);
            assert_eq!(idx, exp, "val = {t}");
        }
    }
}
