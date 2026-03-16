use axum::extract::{Path, Query, State};
use axum::{routing::get, Json, Router};
use chrono::{DateTime, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error;
use crate::error::Error;
use crate::patchwork::{Fill, PatchworkTimeseriesTable};
use crate::EgressState;
use crate::PatchworkTables;
use util::{DbPools, OpenTimerange, PooledPgConn};
mod humidity;
use crate::calculations::humidity::{
    dew_point_temperature, humidity_mixing_ratio, specific_humidity,
    water_vapor_partial_pressure_in_air,
};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct CalculationParams {
    stationid: i32,
    level: Option<i32>,
    sensor: i32,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalculationsAvailableResponse {
    param_id: i32,
    station_id: i32,
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataQCtuple {
    value: f64,
    quality_code: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalculationsResponse {
    param_id: i32,
    timestamp: DateTime<Utc>,
    value: f64,
    underlying_data: Option<HashMap<i32, DataQCtuple>>, // paramid -> (value, quality_code)
}

#[derive(Debug, Clone)]
pub struct CalculationsConstructor {
    label: PotentialCalculationsLabel,
    input_paramids: Vec<(i32, Vec<Fill>)>,
}

// label needed for sorting if the patchwork labels have all
// the input paramids for a calculation
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct PotentialCalculationsLabel {
    pub station_id: i32,
    pub level: Option<i32>,
    pub sensor: Option<i32>,
}

#[derive(Clone, Default, PartialEq, Debug)]
struct CalculationPatch {
    tsids: Vec<i64>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

pub fn available_calculations_for_param(
    param_id: i32,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<CalculationsConstructor>, Error> {
    match param_id {
        // "dew_point_temperature"
        217 => get_param_calculations(vec![211, 262], patchwork_table),
        // "specific_humidity"
        3123 => get_param_calculations(vec![211, 262, 173], patchwork_table),
        // "over_time(humidity_mixing_ratio P1D)"
        3197 => get_param_calculations(vec![211, 262, 173], patchwork_table),
        // "mean(water_vapor_partial_pressure_in_air P1D)"
        3136 => get_param_calculations(vec![211, 262], patchwork_table),
        _ => Err(Error::InvalidParam(param_id.to_string())),
    }
}

fn unwrap_original_corrected(
    original: Option<f64>,
    corrected: Option<f64>,
) -> Result<Option<f64>, Error> {
    // deal with unwrapping the options, choosing corrected if exists, or else original
    match (original, corrected) {
        (Some(_), Some(corrected)) => Ok(Some(corrected)),
        (None, Some(corrected)) => Ok(Some(corrected)),
        (Some(original), None) => Ok(Some(original)),
        _ => Ok(None),
    }
}

async fn get_calculation_data_pair(
    patches: &[CalculationPatch],
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, DataQCtuple, DataQCtuple)>, Error> {
    let query = conn
        .prepare(
            "SELECT \
                param1.obstime, param2.obstime, \
                param1.original, param2.original, \
                param1.corrected, param2.corrected, \
                param1.quality_code, param2.quality_code \
            FROM ( \
                SELECT obstime, original, corrected, quality_code FROM legacy.data \
                WHERE timeseries = $1 \
                AND obstime >= $3 AND obstime < $4 \
            ) param1 \
            INNER JOIN ( \
                SELECT obstime, original, corrected, quality_code FROM legacy.data \
                WHERE timeseries = $2 \
                AND obstime >= $3 AND obstime < $4 \
            ) param2 \
            USING (obstime)",
        )
        .await?;

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(
                &query,
                &[&patch.tsids[0], &patch.tsids[1], &patch.from, &patch.to],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            if let Some((val1, val2)) = row
                .get::<usize, Option<f64>>(2)
                .or(row.get(4))
                .zip(row.get::<usize, Option<f64>>(3).or(row.get(5)))
            {
                let d1 = DataQCtuple {
                    value: val1,
                    quality_code: row.get(6),
                };
                let d2 = DataQCtuple {
                    value: val2,
                    quality_code: row.get(7),
                };
                data.push((row.get(0), d1, d2));
            }
        }
    }

    Ok(data)
}

async fn get_calculation_data_triple(
    patches: &[CalculationPatch],
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, DataQCtuple, DataQCtuple, DataQCtuple)>, Error> {
    let query = conn
        .prepare(
            "SELECT \
                param1.obstime, param2.obstime, param3.obstime, \
                param1.original, param2.original, param3.original, \
                param1.corrected, param2.corrected, param3.corrected, \
                param1.quality_code, param2.quality_code, param3.quality_code \
            FROM ( \
                SELECT obstime, original, corrected, quality_code FROM legacy.data \
                WHERE timeseries = $1 \
                AND obstime >= $4 AND obstime < $5 \
            ) param1 \
            INNER JOIN ( \
                SELECT obstime, original, corrected, quality_code FROM legacy.data \
                WHERE timeseries = $2 \
                AND obstime >= $4 AND obstime < $5 \
            ) param2 \
            INNER JOIN ( \
                SELECT obstime, original, corrected, quality_code FROM legacy.data \
                WHERE timeseries = $3 \
                AND obstime >= $4 AND obstime < $5 \
            ) param3 \
            USING (obstime)",
        )
        .await?;

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(
                &query,
                &[
                    &patch.tsids[0],
                    &patch.tsids[1],
                    &patch.tsids[2],
                    &patch.from,
                    &patch.to,
                ],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            let value1 = unwrap_original_corrected(row.get(3), row.get(6))?;
            let value2 = unwrap_original_corrected(row.get(4), row.get(7))?;
            let value3 = unwrap_original_corrected(row.get(5), row.get(8))?;
            if value1.is_none() || value2.is_none() || value3.is_none() {
                continue; // if don't have a value for one of the params, skip this row
            }
            let d1 = DataQCtuple {
                value: value1.unwrap(),
                quality_code: row.get(9),
            };
            let d2 = DataQCtuple {
                value: value2.unwrap(),
                quality_code: row.get(10),
            };
            let d3 = DataQCtuple {
                value: value2.unwrap(),
                quality_code: row.get(11),
            };
            data.push((row.get(0), d1, d2, d3));
        }
    }

    Ok(data)
}

fn merge_fills_pair(param1: Vec<Fill>, param2: Vec<Fill>) -> Vec<CalculationPatch> {
    if param1.is_empty() || param2.is_empty() {
        return vec![];
    }

    let patches = param1
        .iter()
        .flat_map(|fill1| {
            param2.iter().filter_map(|fill2| {
                // construct the from/to times to use to look for the overlap
                let fill1_fromto = OpenTimerange {
                    from: Some(fill1.from),
                    to: fill1.to,
                };
                let fill2_fromto = OpenTimerange {
                    from: Some(fill2.from),
                    to: fill2.to,
                };

                let overlap = fill1_fromto.overlap(fill2_fromto)?;

                Some(CalculationPatch {
                    tsids: vec![fill1.tsid, fill2.tsid],
                    from: overlap.from.unwrap_or_default(), // should always have a from time, but just in case use default?
                    to: overlap.to,
                })
            })
        })
        .collect();
    patches
}

fn merge_fills_triple(
    param1: Vec<Fill>,
    param2: Vec<Fill>,
    param3: Vec<Fill>,
) -> Vec<CalculationPatch> {
    if param1.is_empty() || param2.is_empty() || param3.is_empty() {
        return vec![];
    }
    let intermediate_patches = merge_fills_pair(param1, param2);
    let patches = intermediate_patches
        .iter()
        .flat_map(|patch| {
            param3.iter().filter_map(|fill3| {
                let patch_fromto = OpenTimerange {
                    from: Some(patch.from),
                    to: patch.to,
                };
                let fill3_fromto = OpenTimerange {
                    from: Some(fill3.from),
                    to: fill3.to,
                };

                let overlap = patch_fromto.overlap(fill3_fromto)?;

                Some(CalculationPatch {
                    tsids: vec![patch.tsids[0], patch.tsids[1], fill3.tsid],
                    from: overlap.from.unwrap_or_default(), // should always have a from time, but just in case use default?
                    to: overlap.to,
                })
            })
        })
        .collect();

    patches
}

fn get_calculation_patch_for_label_pair(
    label: PotentialCalculationsLabel,
    potential_fills: Vec<CalculationsConstructor>,
) -> Result<Vec<CalculationPatch>, Error> {
    // filter down to correct label and merge the fills
    let patches = potential_fills
        .iter()
        .filter(|calc| {
            calc.label.station_id == label.station_id
                && calc.label.level == label.level
                && calc.label.sensor == label.sensor
        })
        .flat_map(|calculation| {
            merge_fills_pair(
                calculation.input_paramids[0].1.clone(),
                calculation.input_paramids[1].1.clone(),
            )
        })
        .collect::<Vec<CalculationPatch>>();
    Ok(patches)
}

fn get_calculation_patch_for_label_triple(
    label: PotentialCalculationsLabel,
    potential_fills: Vec<CalculationsConstructor>,
) -> Result<Vec<CalculationPatch>, Error> {
    // filter down to correct label and merge the fills
    let patches = potential_fills
        .iter()
        .filter(|calc| {
            calc.label.station_id == label.station_id
                && calc.label.level == label.level
                && calc.label.sensor == label.sensor
        })
        .flat_map(|calculation| {
            merge_fills_triple(
                calculation.input_paramids[0].1.clone(),
                calculation.input_paramids[1].1.clone(),
                calculation.input_paramids[2].1.clone(),
            )
        })
        .collect::<Vec<CalculationPatch>>();
    Ok(patches)
}

fn get_param_calculations(
    input_paramids: Vec<i32>,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<CalculationsConstructor>, Error> {
    let mut param_available: Vec<CalculationsConstructor> = Vec::new();

    // just do the open table for now
    let table_guard = patchwork_table.read()?;

    let mut found_params: HashMap<PotentialCalculationsLabel, Vec<(i32, Vec<Fill>)>> =
        HashMap::new();
    // iterate over all the labels in the patchwork table
    for (key, value) in table_guard.iter() {
        if key.station_id > 99999 {
            // skip data from outside Norway
            continue;
        }
        // for each calculation, keep anything that could be an input param
        if input_paramids[0..].contains(&key.param_id) {
            let label = PotentialCalculationsLabel {
                station_id: key.station_id,
                level: key.level,
                sensor: key.sensor,
            };
            found_params
                .entry(label)
                .or_default()
                .push((key.param_id, value.to_vec()));
        }
    }
    // if have all the input params for the calculation, then add to available calculations
    // TODO: check the time range... cut down to overlap!
    for (key, value) in found_params.iter() {
        // actually have all the input parameters?
        if value.len() == input_paramids.len() {
            // add to the calculation table
            param_available.push(CalculationsConstructor {
                label: key.clone(),
                input_paramids: value.clone(),
            });
        }
    }
    drop(table_guard); // release the read lock
    Ok(param_available)
}

pub async fn calculations_available_handler(
    Path(param_id): Path<i32>,
    State(patchwork_tables): State<PatchworkTables>,
) -> Result<Json<Vec<CalculationsAvailableResponse>>, (StatusCode, String)> {
    // TODO:
    // Make it work for more than the open timeseries
    let available: Vec<CalculationsConstructor> =
        available_calculations_for_param(param_id, patchwork_tables.open)
            .map_err(error::internal_error)?;
    let mut available_calculations: Vec<CalculationsAvailableResponse> = Vec::new();
    for calculation in available {
        // when do I have all the input params?
        let mut param_fromto: Vec<(i32, OpenTimerange)> = Vec::new();
        for (paramid, fill) in calculation.input_paramids.iter() {
            // for now find the earliest and latest (open) times?
            let first_time = fill.iter().map(|item| item.from).min().unwrap();
            let last_time = if fill.iter().any(|item| item.to.is_none()) {
                // if there is a None to time, that means the series is open ended,
                // which is the latest possible to time. but Option's Ord impl
                // counts None as less than Some. So we have this if check to
                // override that behaviour
                None
            } else {
                fill.iter().map(|item| item.to).max().unwrap()
            };
            param_fromto.push((
                *paramid,
                OpenTimerange {
                    from: Some(first_time),
                    to: last_time,
                },
            ));
        }
        // then find the overlap
        let mut timerange: Option<OpenTimerange> = None;
        for window in param_fromto.windows(2) {
            let prev_timerange = window[0].1;
            let curr_timerange = window[1].1;
            timerange = prev_timerange.overlap(curr_timerange);
        }
        // there is a range where they overlap
        if let Some(timerange) = timerange {
            if let Some(from) = timerange.from {
                available_calculations.push(CalculationsAvailableResponse {
                    param_id,
                    station_id: calculation.label.station_id,
                    level: calculation.label.level,
                    sensor: calculation.label.sensor,
                    from,
                    to: timerange.to,
                });
            }
        }
    }

    Ok(Json(available_calculations))
}

//#[axum::debug_handler(state = EgressState)]
pub async fn dew_point_temperature_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<CalculationParams>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<CalculationsResponse> = Vec::new();

    // labels to get data for: 211, 262
    let potential_fills = get_param_calculations(vec![211, 262], patchwork_tables.open.clone())
        .map_err(error::internal_error)?;

    let patches = get_calculation_patch_for_label_pair(
        PotentialCalculationsLabel {
            station_id: params.stationid,
            level: params.level,
            sensor: Some(params.sensor),
        },
        potential_fills,
    )
    .map_err(error::internal_error)?;

    let data = get_calculation_data_pair(&patches, &open_conn)
        .await
        .map_err(error::internal_error)?;
    for (obstime, air_temperature, relative_humidity) in data {
        let value = dew_point_temperature(air_temperature.value, relative_humidity.value).unwrap();
        response.push(CalculationsResponse {
            param_id: 217,
            timestamp: obstime,
            value,
            underlying_data: Some(
                vec![(211, air_temperature), (262, relative_humidity)]
                    .into_iter()
                    .collect(),
            ),
        });
    }

    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

pub async fn specific_humidity_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<CalculationParams>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<CalculationsResponse> = Vec::new();

    // labels to get data for: 211, 262, 173
    let potential_fills =
        get_param_calculations(vec![211, 262, 173], patchwork_tables.open.clone())
            .map_err(error::internal_error)?;

    let patches = get_calculation_patch_for_label_triple(
        PotentialCalculationsLabel {
            station_id: params.stationid,
            level: params.level,
            sensor: Some(params.sensor),
        },
        potential_fills,
    )
    .map_err(error::internal_error)?;

    let data = get_calculation_data_triple(&patches, &open_conn)
        .await
        .map_err(error::internal_error)?;
    for (obstime, air_temperature, relative_humidity, surface_air_pressure) in data {
        let value = specific_humidity(
            air_temperature.value,
            relative_humidity.value,
            surface_air_pressure.value,
        )
        .unwrap();
        response.push(CalculationsResponse {
            param_id: 3123,
            timestamp: obstime,
            value,
            underlying_data: Some(
                vec![
                    (211, air_temperature),
                    (262, relative_humidity),
                    (173, surface_air_pressure),
                ]
                .into_iter()
                .collect(),
            ),
        });
    }

    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

pub async fn humidity_mixing_ratio_router(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<CalculationParams>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<CalculationsResponse> = Vec::new();

    // labels to get data for: 211, 262, 173
    let potential_fills =
        get_param_calculations(vec![211, 262, 173], patchwork_tables.open.clone())
            .map_err(error::internal_error)?;

    let patches = get_calculation_patch_for_label_triple(
        PotentialCalculationsLabel {
            station_id: params.stationid,
            level: params.level,
            sensor: Some(params.sensor),
        },
        potential_fills,
    )
    .map_err(error::internal_error)?;

    let data = get_calculation_data_triple(&patches, &open_conn)
        .await
        .map_err(error::internal_error)?;
    for (obstime, air_temperature, relative_humidity, surface_air_pressure) in data {
        let value = humidity_mixing_ratio(
            air_temperature.value,
            relative_humidity.value,
            surface_air_pressure.value,
        )
        .unwrap();
        response.push(CalculationsResponse {
            param_id: 3197,
            timestamp: obstime,
            value,
            underlying_data: Some(
                vec![
                    (211, air_temperature),
                    (262, relative_humidity),
                    (173, surface_air_pressure),
                ]
                .into_iter()
                .collect(),
            ),
        });
    }
    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

pub async fn water_vapor_partial_pressure_in_air_router(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<CalculationParams>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<CalculationsResponse> = Vec::new();
    // labels to get data for: 211, 262
    let potential_fills = get_param_calculations(vec![211, 262], patchwork_tables.open.clone())
        .map_err(error::internal_error)?;

    let patches = get_calculation_patch_for_label_pair(
        PotentialCalculationsLabel {
            station_id: params.stationid,
            level: params.level,
            sensor: Some(params.sensor),
        },
        potential_fills,
    )
    .map_err(error::internal_error)?;

    let data = get_calculation_data_pair(&patches, &open_conn)
        .await
        .map_err(error::internal_error)?;
    for (obstime, air_temperature, relative_humidity) in data {
        let value =
            water_vapor_partial_pressure_in_air(air_temperature.value, relative_humidity.value)
                .unwrap();
        response.push(CalculationsResponse {
            param_id: 217,
            timestamp: obstime,
            value,
            underlying_data: Some(
                vec![(211, air_temperature), (262, relative_humidity)]
                    .into_iter()
                    .collect(),
            ),
        });
    }

    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

// TODO: can one have spaces in the path of the routes?
pub fn products_router() -> Router<EgressState> {
    Router::new()
        .route("/available/{param_id}", get(calculations_available_handler))
        .route("/217", get(dew_point_temperature_handler))
        .route("/3123", get(specific_humidity_handler))
        .route("/3197", get(humidity_mixing_ratio_router))
        .route("/3136", get(water_vapor_partial_pressure_in_air_router))
}
