use axum::{routing::get, Router};
use chrono::{DateTime, Utc};
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, RwLock};

use crate::error::Error;
use crate::patchwork::{Fill, PatchworkTimeseriesTable};
use crate::EgressState;
use crate::PatchworkLabel;
use crate::PatchworkTables;

mod calculations;
use calculations::{products_available_handler, products_handler};

// define a struct for products
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ProductParse {
    pub input_paramids: String,
    pub output_paramid: i32,
    #[serde(rename = "element_id")]
    pub element: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Product {
    pub input_paramids: Vec<i32>,
    pub output_paramid: i32,
    pub element: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductsConstructor {
    paramid: i32,
    tsid: i64,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

pub type ProductsTimeseriesTable = HashMap<String, Vec<ProductsConstructor>>;

#[derive(Debug, Clone)]
pub struct ProductTables {
    pub open: Arc<RwLock<ProductsTimeseriesTable>>,
    pub restricted: Arc<RwLock<ProductsTimeseriesTable>>,
}

impl ProductTables {
    pub fn new(open: ProductsTimeseriesTable, restricted: ProductsTimeseriesTable) -> Self {
        Self {
            open: Arc::new(RwLock::new(open)),
            restricted: Arc::new(RwLock::new(restricted)),
        }
    }

    // Initialize product tables, requires the patchwork tables to find input timeseries
    pub async fn init(patchwork_tables: PatchworkTables) -> Result<Self, Error> {
        let patchwork_table_open =
            create_product_calculations_table(patchwork_tables.open.clone())?;

        let patchwork_table_restricted = ProductsTimeseriesTable::new();

        Ok(Self::new(patchwork_table_open, patchwork_table_restricted))
    }
}

pub fn load_product_list(filename: &str) -> Result<Vec<Product>, Error> {
    let mut list: Vec<Product> = Vec::new();

    // TODO: avoid the unwrap here???
    let file = File::open(filename).unwrap();
    let mut rdr = ReaderBuilder::new().delimiter(b';').from_reader(file);

    rdr.deserialize().for_each(|result| {
        let record: ProductParse = result.unwrap();

        let parsed_vector: Vec<i32> = record
            .input_paramids
            .trim_matches(|c| c == '[' || c == ']') // Remove brackets if present
            .split(',')
            .filter_map(|s| s.trim().parse().ok()) // Parse each element
            .collect();

        list.push(Product {
            input_paramids: parsed_vector,
            output_paramid: record.output_paramid,
            element: record.element,
        });
    });
    Ok(list)
}

pub fn create_product_calculations_table(
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<ProductsTimeseriesTable, Error> {
    let products_path = std::env::var("PRODUCTS_CSV").unwrap();
    let product_list = load_product_list(&products_path)?;

    let mut open_product_table: ProductsTimeseriesTable = HashMap::new();

    // just do the open table for now
    let table_guard = patchwork_table
        .read()
        .map_err(|e| Error::Lock(e.to_string()))?;

    for product in product_list {
        let mut found_params: HashMap<PatchworkLabel, Vec<Fill>> = HashMap::new();
        // iterate over all the labels in the patchwork table
        for (key, value) in table_guard.iter() {
            // for each product, keep anything that could be an input param
            if product.input_paramids[0..].contains(&key.param_id) {
                found_params.insert(*key, value.to_vec());
            }
        }
        // if have all the input params for the product, then add to available products
        // TODO: check the time range... cut down to overlapp!
        if found_params.len() == product.input_paramids.len() {
            // get the timeseries ids for the input params
            for (key, value) in found_params.iter() {
                for fill in value {
                    if product.input_paramids.contains(&key.param_id) {
                        // add to the product table
                        let entry = open_product_table
                            .entry(product.element.clone())
                            .or_default();
                        entry.push(ProductsConstructor {
                            paramid: key.param_id,
                            tsid: fill.tsid,
                            from: fill.from,
                            to: fill.to,
                        });
                    }
                }
            }
        }
    }
    drop(table_guard);

    Ok(open_product_table)
}

// TODO: figure out how to use the element id to dynamically get the name of the handler?
// or use the element id as a switch in the handler to determine how to calculate the product
pub fn products_router() -> Router<EgressState> {
    Router::new()
        .route("/available/{element_id}", get(products_available_handler))
        .route("/{element_id}", get(products_handler))
}
