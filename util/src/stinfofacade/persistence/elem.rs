use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::stinfofacade::{
    elem::Tables,
    persistence::{read_from_csv, write_to_csv, Error},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Elem {
    pub param: i32,
    pub elem_code: Option<String>,
    pub elem_id: Option<String>,
}

const PATH: &str = "persistence/elem.csv";

pub async fn persist_to_path(records: Vec<Elem>, path: impl AsRef<Path>) -> Result<(), Error> {
    write_to_csv(records, path).await
}

pub async fn persist(records: Vec<Elem>) -> Result<(), Error> {
    persist_to_path(records, PATH).await
}

pub fn build_table(records: &[Elem]) -> Tables {
    let param_to_elem_table = records
        .iter()
        .map(|elem| (elem.param, (elem.elem_code.clone(), elem.elem_id.clone())))
        .collect();

    let code_to_param_table = records
        .iter()
        .filter(|elem| elem.elem_code.is_some())
        .map(|elem| (elem.elem_code.clone().unwrap(), elem.param))
        .collect();

    Tables {
        param_to_elem_table,
        code_to_param_table,
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
