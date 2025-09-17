use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, NaiveDate, Utc};
use postgres_types::FromSql;
use serde::{Deserialize, Serialize};
use util::PooledPgConn;

use crate::{error, PgConnectionPool};

const WIND_SPEED_PARAMID: i32 = 81;
const WIND_DIRECTION_PARAMID: i32 = 61;

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
#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct WindCategories {
    /// Percentage of observations that are below a certain threshold of wind speed
    silent_wind: f64,
    /// Percentage of observation where the wind direction could not be estimated (it has a
    /// negative value)
    variable_wind: f64,
}

/// A 2D histogram of wind speed vs wind direction.
/// The X-axis has variable sized bins, while the Y-axis uniform cyclic bins
struct Windrose {
    /// The histogram values
    hist: Vec<Vec<f64>>,
    /// Categories for non standard observation values that need to be accounted for separately
    wind_categories: WindCategories,
    /// Total number of observations fetched from LARD to create the histogram
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

        let n_days = days.len() as f64;

        for day in days {
            let n_obs = day.observations.len();

            // Observations in each day sum up to 1.0 (each day weighs the same)
            let weight = 1.0 / n_obs as f64;

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

            total_obs += n_obs;
        }

        // Normalize by number of days
        hist.iter_mut().for_each(|x| {
            x.iter_mut().for_each(|val| {
                // TODO: make sure this is actually correct!
                // TODO: this could also be simply done at serialization time? Format with 2
                // significant digits?
                // NOTE: we multiply by 100.0 to convert to percentage
                *val = round(*val / n_days * 100.0);
            })
        });

        let wind_categories = WindCategories {
            silent_wind: round(silent_wind / n_days * 100.0),
            variable_wind: round(variable_wind / n_days * 100.0),
        };

        Self {
            hist,
            total_obs,
            wind_categories,
        }
    }

    /// Sum along the y axis
    // TODO: should round here too?
    fn wind_speed_hist(&self) -> Vec<f64> {
        self.hist.iter().map(|y| y.iter().sum()).collect()
    }

    /// Sum along the x axis
    // TODO: should round here too?
    fn wind_direction_hist(&self) -> Vec<f64> {
        let mut sum = vec![0.0; self.hist[0].len()];

        for x in &self.hist {
            for (i, val) in x.iter().enumerate() {
                sum[i] += val;
            }
        }

        sum
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
    _date: NaiveDate,
    observations: Vec<WindObs>,
}

/// Query parameter for reports/windrose/{station_id} endpoint
#[derive(Debug, Serialize, Deserialize)]
pub struct WindroseParams {
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
    months: Vec<i32>,
}

/// Metadata returned with the response
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
    station_id: i32,
    number_of_values: usize,
    months: Vec<i32>,
}

#[derive(Debug, Serialize)]
struct Axis {
    /// Axis labels
    labels: &'static [&'static str],
    /// 1D histogram values
    sums: Vec<f64>,
}

/// Response from reports/windrose/{station_id} endpoint
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindroseResp {
    wind_speed: Axis,
    wind_direction: Axis,
    extras: WindCategories,
    table: Vec<Vec<f64>>,
    metadata: Metadata,
}

/// Aggregate wind speed and wind direction observations by day in the given [fromtime, totime)
/// range
// NOTE: edge cases:
//  1. When wind speed is 0, so is wind direction
//  2. Wind direction can be negative. These are special values to indicate that either the
//     measurement could not be taken out or the result is non-sense, so they are not actually observations.
//     In these cases the data points fall into the 'variable wind' category.
async fn get_wind_days(
    wind_speed_ts: i64,
    wind_direction_ts: i64,
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
    months: &[i32],
    conn: &PooledPgConn<'_>,
) -> Result<Vec<WindDay>, (StatusCode, String)> {
    // TODO: there's probably a better way to do this query?
    // TODO: RIGTH JOIN? Do we want to keep wind_directions that are NULL?
    // TODO: only use hourly data?
    // TODO: normal windroses are calculated from hourly observations, but for some stations, SVV for
    // example, we don't have hourly observations so we might need separate algos depending on the station?
    let rows = conn
        .query(
            "SELECT \
                DATE_TRUNC('day', obstime), \
                ARRAY_AGG((speed.obs, direction.obs)::windobs) \
            FROM ( \
                SELECT obstime, corrected AS obs FROM legacy.data \
                WHERE timeseries = $1 \
                AND corrected IS NOT NULL \
                AND quality_code IS NOT NULL \
                AND quality_code != 7 \
            ) speed \
            INNER JOIN ( \
                SELECT obstime, corrected AS obs FROM legacy.data \
                WHERE timeseries = $2 \
                AND corrected IS NOT NULL \
                AND quality_code IS NOT NULL \
                AND quality_code != 7 \
            ) direction \
            USING (obstime) \
            AND ($5::int[] = '{}' OR EXTRACT(month FROM obstime)::int = ANY($5)) \
            WHERE obstime BETWEEN $6 AND $7 \
            GROUP BY day",
            &[
                &wind_speed_ts,
                &wind_direction_ts,
                &fromtime,
                &totime,
                &months,
            ],
        )
        .await
        .map_err(error::internal_error)?;

    let days = rows
        .iter()
        .map(|row| WindDay {
            _date: row.get(0),
            observations: row.get(1),
        })
        .collect();

    Ok(days)
}

/// Return the TSID for the given station and parameter
async fn get_tsid(
    station_id: i32,
    param_id: i32,
    conn: &PooledPgConn<'_>,
) -> Result<i64, (StatusCode, String)> {
    // FIXME: this can return many timeseries if the station has multiple sensors/levels
    // TODO: should we use default sensor/level values? Use filter timeseries?
    let row = conn
        .query_one(
            "SELECT timeseries FROM labels.met \
            WHERE station_id = $1 \
            AND param_id = $2",
            &[&station_id, &param_id],
        )
        .await
        .map_err(error::internal_error)?;

    Ok(row.get(0))
}

pub async fn windrose_handler(
    Path(station_id): Path<i32>,
    Query(params): Query<WindroseParams>,
    State(pool): State<PgConnectionPool>,
) -> Result<Json<WindroseResp>, (StatusCode, String)> {
    let conn = pool.get().await.map_err(error::internal_error)?;

    let wind_speed_ts = get_tsid(station_id, WIND_SPEED_PARAMID, &conn).await?;
    let wind_direction_ts = get_tsid(station_id, WIND_DIRECTION_PARAMID, &conn).await?;

    let days = get_wind_days(
        wind_speed_ts,
        wind_direction_ts,
        params.fromtime,
        params.totime,
        &params.months,
        &conn,
    )
    .await?;

    // TODO: spawn sync thread here?
    let windrose = Windrose::new_from_days(Windrose::default_axes(), days);

    let metadata = Metadata {
        fromtime: params.fromtime,
        totime: params.totime,
        station_id,
        number_of_values: windrose.total_obs,
        months: params.months,
    };

    Ok(Json(WindroseResp {
        wind_direction: Axis {
            labels: WIND_DIRECTION_LABELS,
            sums: windrose.wind_direction_hist(),
        },
        wind_speed: Axis {
            labels: WIND_SPEED_LABELS,
            sums: windrose.wind_speed_hist(),
        },
        metadata,
        extras: windrose.wind_categories,
        table: windrose.hist,
    }))
}

#[cfg(test)]
mod test {
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
        assert_eq!(windrose.wind_speed_hist(), expected.x_sum);
        assert_eq!(windrose.wind_direction_hist(), expected.y_sum);
    }

    #[test]
    fn test_single_day() {
        let days = vec![WindDay {
            _date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
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
                _date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
                observations: vec![WindObs::new(0.5, 220.0)],
            },
            WindDay {
                // these weigh 0.33 each
                _date: NaiveDate::from_ymd_opt(2000, 1, 2).unwrap(),
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
            _date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
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
                _date: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
                observations: vec![WindObs::new(0.1, 220.0)],
            },
            WindDay {
                _date: NaiveDate::from_ymd_opt(2000, 2, 1).unwrap(),
                observations: vec![WindObs::new(0.4, 30.0)],
            },
            WindDay {
                _date: NaiveDate::from_ymd_opt(2000, 4, 1).unwrap(),
                observations: vec![WindObs::new(37.0, 15.0)],
            },
        ];

        let axes = Windrose::default_axes();
        let y_bins = axes.1.nbins();

        // TODO: these probabilities do not actually sum up to 100
        let expected = ExpectedWindrose {
            x_sum: vec![33.33, 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 33.33],
            y_sum: vec![
                0., 66.66, 0., 0., 0., 0., 0.0, 0., 0., 0., 0., 0., 0., 0., 0., 0.,
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
}
