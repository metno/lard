use std::collections::HashMap;

use crate::error;
use crate::error::Error;
use crate::products::ProductsConstructor;
use crate::EgressState;
use crate::ProductTables;
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::error;
use util::deserialize::comma_separated;
use util::{DbPools, PooledPgConn};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductParams {
    #[serde(deserialize_with = "comma_separated")]
    stationids: Vec<i32>,
    #[serde(deserialize_with = "comma_separated")]
    levels: Vec<i32>,
    #[serde(deserialize_with = "comma_separated")]
    sensors: Vec<i32>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductsAvailableResponse {
    element: String,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductsResponse {
    name: String,
    timestamp: DateTime<Utc>,
    value: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalculationsDatum {
    paramid: i32,
    original: Option<f64>,
    corrected: Option<f64>,
}

pub fn dew_point_temperature(temperature: f64, relative_humidity: f64) -> Result<f64, Error> {
    //'$3=$1-((100-$2)/5)'
    Ok(temperature - ((100.0 - relative_humidity) / 5.0))
}

pub fn get_available_products(
    product_tables: ProductTables,
    element: &String,
) -> Result<Vec<ProductsConstructor>, (StatusCode, String)> {
    let product_guard = product_tables.open.read().map_err(error::internal_error)?;

    let available = product_guard.get(element).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("No products found for element_id: {}", element),
        )
    })?;
    Ok(available.to_vec())
}

pub async fn get_calculation_data(
    conn: &PooledPgConn<'_>,
    tsids_paramids: Vec<(i64, i32)>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<HashMap<DateTime<Utc>, Vec<CalculationsDatum>>, Error> {
    // TODO: do something with quality code, so have the underlying ts quality for the product
    let query = conn
        .prepare(
            "SELECT timeseries, obstime, original, corrected, quality_code \
                FROM legacy.data \
                WHERE timeseries = $1 \
                    AND obstime >= $2 \
                    AND obstime < $3 \
                ORDER BY obstime",
        )
        .await?;

    let mut futures = tsids_paramids
        .iter()
        .map(async |id| conn.query(&query, &[&id.0, &from, &to]).await)
        .collect::<FuturesOrdered<_>>()
        .enumerate();

    let mut data: HashMap<DateTime<Utc>, Vec<CalculationsDatum>> = HashMap::new();

    while let Some((i, res)) = futures.next().await {
        let rows = match res {
            Ok(val) => val,
            Err(err) => {
                error!("getting last obstime failed: {}, {}", i, err);
                continue;
            }
        };
        for row in rows {
            let tsid: i64 = row.get(0);
            let time: DateTime<Utc> = row.get(1);
            let paramid = tsids_paramids.iter().find(|i| i.0 == tsid);
            if let Some(p) = paramid {
                // put the paramid back with the data for use later
                let datum = CalculationsDatum {
                    paramid: p.1,
                    original: row.get(2),
                    corrected: row.get(3),
                };
                data.entry(time).or_default().push(datum);
            }
        }
    }

    Ok(data)
}

pub async fn products_available_handler(
    Path(element_id): Path<String>,
    State(product_tables): State<ProductTables>,
) -> Result<Json<Vec<ProductsAvailableResponse>>, (StatusCode, String)> {
    // TODO:
    // load list of timeseries that have the inputs for each product, for products "available"
    let available: Vec<ProductsConstructor> = get_available_products(product_tables, &element_id)?;

    let mut available_products: Vec<ProductsAvailableResponse> = Vec::new();
    for product in available {
        available_products.push(ProductsAvailableResponse {
            element: element_id.clone(),
            from: product.from,
            to: product.to,
        });
    }

    Ok(Json(available_products))
}

#[axum::debug_handler(state = EgressState)]
pub async fn products_handler(
    State(pools): State<DbPools>,
    Path(element_id): Path<String>,
    State(product_tables): State<ProductTables>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let available: Vec<ProductsConstructor> = get_available_products(product_tables, &element_id)?;
    let tsid_paramid_list: Vec<(i64, i32)> =
        available.iter().map(|p| (p.tsid, p.paramid)).collect();

    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    // actually get the data
    let data = get_calculation_data(&open_conn, tsid_paramid_list, params.from, params.to)
        .await
        .map_err(error::internal_error);
    let mut response: Vec<ProductsResponse> = Vec::new();

    if let Ok(d) = data {
        // do the calculation
        for (time, vector) in d {
            // if have the same timestamp for the input paramids
            if element_id == "dew_point_temperature" {
                // dew point temperature
                // need temperature and relative humidity

                let find_air_temperature = vector.iter().find(|v| v.paramid == 211);
                let find_relative_humidity = vector.iter().find(|v| v.paramid == 262);
                match (find_air_temperature, find_relative_humidity) {
                    (Some(found_air_temperature), Some(found_relative_humidity)) => {
                        match (
                            found_air_temperature.corrected,
                            found_air_temperature.original,
                            found_relative_humidity.corrected,
                            found_relative_humidity.original,
                        ) {
                            (Some(air_temperature), Some(_), Some(relative_humidity), Some(_)) => {
                                let value =
                                    dew_point_temperature(air_temperature, relative_humidity)
                                        .unwrap();
                                response.push(ProductsResponse {
                                    name: element_id.clone(),
                                    timestamp: time,
                                    value,
                                });
                            }
                            (None, Some(air_temperature), None, Some(relative_humidity)) => {
                                let value =
                                    dew_point_temperature(air_temperature, relative_humidity)
                                        .unwrap();
                                response.push(ProductsResponse {
                                    name: element_id.clone(),
                                    timestamp: time,
                                    value,
                                });
                            }
                            _ => println!(
                                "Didn't have either both corrected values, or both original"
                            ),
                        };
                    }
                    _ => println!("Didn't find both paramids"),
                }
            }
        }
    }
    // return something for now...
    Ok(Json(response))
}
