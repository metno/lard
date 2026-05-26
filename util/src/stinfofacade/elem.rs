use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio_postgres::NoTls;
use tracing::{error, warn};

use crate::stinfofacade::Error;

// in order to go from code to param you must go through elem id (for normals that includes specific period / frequency)
#[derive(Clone, Debug, PartialEq)]
pub struct Tables {
    pub elem_to_param_table: HashMap<String, i32>,
    pub code_to_elem_table: HashMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Elem {
    pub param: i32,
    pub elem_code: Option<String>, // elem_code is being deprecated...
    pub elem_id: String,
}

pub type ElemTables = Arc<RwLock<Tables>>;

fn build_tables(records: &[Elem]) -> Tables {
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

/// Get a fresh cache of elem conversions from stinfosys
pub async fn fetch_elems(stinfo_conn_string: &Option<String>) -> Result<ElemTables, Error> {
    let stinfo_conn_string = match stinfo_conn_string {
        Some(s) => s,
        None => return Err(Error::NoConnString),
    };
    // get stinfo conn
    let (client, conn) = tokio_postgres::connect(stinfo_conn_string, NoTls).await?;

    // conn object independently performs communication with database, so needs it's own task.
    // it will return when the client is dropped
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {}", e);
        }
    });

    let rows = client
        .query(
            "SELECT p.paramid, k.elem_code, p.element_id  
FROM param AS p, kdvh_element AS k 
WHERE p.element_id=k.element_id",
            &[],
        )
        .await
        .inspect_err(|e| warn!("failed to query param and kdvh_element: {e}"))?;

    let elems: Vec<Elem> = rows
        .into_iter()
        .map(|row| Elem {
            param: row.get(0),
            elem_code: row.get(1),
            elem_id: row.get(2),
        })
        .collect();

    let tables = build_tables(&elems);

    Ok(Arc::new(RwLock::new(tables)))
}
