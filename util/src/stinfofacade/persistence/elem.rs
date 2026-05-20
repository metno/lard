use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::stinfofacade::{
    elem::Tables,
    persistence::{Error, read_from_csv, write_to_csv},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Elem {
    pub param: i32,
    pub elem_code: Option<String>, // elem_code is being deprecated...
    pub elem_id: String,
}

const PATH: &str = "persistence/elem.csv";

pub async fn persist_to_path(records: Vec<Elem>, path: impl AsRef<Path>) -> Result<(), Error> {
    write_to_csv(records, path).await
}

pub async fn persist(records: Vec<Elem>) -> Result<(), Error> {
    persist_to_path(records, PATH).await
}

pub fn build_table(records: &[Elem]) -> Tables {
    let elem_to_param_table = records
        .iter()
        .map(|elem| (elem.elem_id.clone(), elem.param))
        .collect();

    let mut code_to_elem_table: HashMap<String, Vec<String>> = HashMap::new();
    for x in records.iter() {
        if let Some(code) = &x.elem_code {
            code_to_elem_table
                .entry(code.clone())
                .or_default()
                .push(x.elem_id.clone());
        }
    }

    Tables {
        elem_to_param_table,
        code_to_elem_table,
    }
}

async fn load_persisted_from_path(path: impl AsRef<Path>) -> Result<Tables, Error> {
    let records = read_from_csv(path).await?;

    Ok(build_table(&records))
}

pub async fn load_persisted() -> Result<Tables, Error> {
    warn!("failed to load elem tables from stinfosys, loading from persisted cache");

    load_persisted_from_path(PATH).await
}
