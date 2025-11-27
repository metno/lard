use crate::error;
use crate::error::Error;
use crate::ProductTables;
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use util::DbPools;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductParams {
    stationid: i32,
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
    value: f64,
}

pub fn dew_point_temperature(temperature: f64, relative_humidity: f64) -> Result<f64, Error> {
    //'$3=$1-((100-$2)/5)'
    Ok(temperature - ((100.0 - relative_humidity) / 5.0))
}

pub async fn products_available_handler(
    Path(element_id): Path<String>,
    State(product_tables): State<ProductTables>,
) -> Result<Json<Vec<ProductsAvailableResponse>>, (StatusCode, String)> {
    println!("Getting available products for element_id: {}", element_id);

    // TODO:
    // load list of timeseries that have the inputs for each product, for products "available"
    let product_guard = product_tables.open.read().map_err(error::internal_error)?;

    let available = product_guard.get(&element_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("No products found for element_id: {}", element_id),
        )
    })?;
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

pub async fn products_handler(
    State(pools): State<DbPools>,
    Path(element_id): Path<String>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time

    // TODO: keep the structure from available products so can find the right timeseries ids
    // for the input params for the product

    let open_conn = pools.open.get().await.map_err(error::internal_error)?;

    let query = open_conn
        .query(
            "SELECT timeseries, obstime, original, corrected, quality_code \
                FROM legacy.data \
                WHERE timeseries = $1 \
                    AND obstime >= $2 \
                    AND obstime < $3 \
                ORDER BY obstime",
            &[&1, &params.from.timestamp(), &params.to.timestamp()],
        )
        .await;

    #[allow(clippy::type_complexity)]
    let mut data: Vec<(i64, DateTime<Utc>, Option<f64>, Option<f64>, Option<i16>)> = Vec::new();

    for row in query.unwrap() {
        let timeseries: i64 = row.get(0);
        let obstime: DateTime<Utc> = row.get(1);
        let original: Option<f64> = row.get(2);
        let corrected: Option<f64> = row.get(3);
        let quality_code: Option<i16> = row.get(4);
        data.push((timeseries, obstime, original, corrected, quality_code));
    }

    // do the calculation
    let mut value = 0.0;
    if element_id == "dew_point_temperature" {
        // dew point temperature
        // need temperature and relative humidity
        value = dew_point_temperature(0.0, 0.0).unwrap();
    }

    // return something for now...
    Ok(Json(vec![ProductsResponse {
        name: element_id.to_string(),
        value,
    }]))
}
