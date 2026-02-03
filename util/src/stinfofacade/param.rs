use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio_postgres::NoTls;
use tracing::{error, info};

use crate::stinfofacade::Error;

/// Type that maps a subset of columns from the Stinfosys 'param' table
#[derive(Clone, Debug)]
pub struct ReferenceParam {
    /// Numerical identifier of the parameter (e.g., 212)
    pub id: i32,
    /// Descriptive identifier of the paramater (e.g., 'air_temperature')
    pub _element_id: String,
    /// Whether the parameter is marked as scalar in Stinfosys
    pub is_scalar: bool,
}

#[derive(Clone, Debug)]
pub struct Tables {
    /// The string here is paramcode, an older text identifier than element_id,
    /// that's used in obsinn's message format
    pub code_table: HashMap<String, ReferenceParam>,
    pub scalar_paramids: Vec<i32>,
}

pub type ParamTables = Arc<RwLock<Tables>>;

/// Get a fresh cache of param conversions from stinfosys
async fn fetch_params(stinfo_conn_string: &str) -> Result<Tables, Error> {
    // get stinfo conn
    let (client, conn) = tokio_postgres::connect(stinfo_conn_string, NoTls).await?;

    // conn object independently performs communication with database, so needs it's own task.
    // it will return when the client is dropped
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {}", e);
        }
    });

    // TODO: check this query, particularly that name is indeed paramcode, and
    //     scalar is type bool
    // query param table
    let rows = client
        .query("SELECT paramid, name, element_id, scalar FROM param", &[])
        .await?;

    let code_table: HashMap<String, ReferenceParam> = rows
        .into_iter()
        .map(|row| {
            (
                row.get(1),
                ReferenceParam {
                    id: row.get(0),
                    _element_id: row.get(2),
                    is_scalar: row.get(3),
                },
            )
        })
        .collect();

    //let scalar_paramids: Vec<i32> = rows
    //.iter()
    //.filter_map(|row| row.is_scalar.then(|| row.get(0)))
    //.collect();
    let scalar_paramids: Vec<i32> = code_table
        .values()
        .filter(|param| param.is_scalar)
        .map(|param| param.id)
        .collect();

    Ok(Tables {
        code_table,
        scalar_paramids,
    })
}

pub async fn setup_params(
    stinfo_conn_string: Option<&'static str>,
    mut refresh_interval: tokio::time::Interval,
) -> Result<ParamTables, Error> {
    let stinfo_conn_string = stinfo_conn_string.unwrap();
    let param_tables = Arc::new(RwLock::new(fetch_params(stinfo_conn_string).await?));
    let loop_tables = param_tables.clone();

    tokio::task::spawn(async move {
        loop {
            refresh_interval.tick().await;

            info!("Refreshing level tables");
            // TODO: better error handling here? Nothing is listening to what
            // returns on this task but we could surface failures in metrics. Also
            // we maybe don't want to bork the task forever if these functions fail
            let new_param_tables = fetch_params(stinfo_conn_string).await.unwrap();
            let mut tables = loop_tables.write().unwrap();
            *tables = new_param_tables;
        }
    });

    Ok(param_tables)
}

/// used for easy construction in tests, not for production
pub fn from_codes(codes: &[&str]) -> ParamTables {
    let code_table: HashMap<String, ReferenceParam> = [
        (
            "TA".to_string(),
            ReferenceParam {
                id: 211,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "CI".to_string(),
            ReferenceParam {
                id: 4,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "IR".to_string(),
            ReferenceParam {
                id: 9,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "KLOBS".to_string(),
            ReferenceParam {
                id: 1022,
                _element_id: "".to_string(),
                is_scalar: false,
            },
        ),
        (
            "TJ".to_string(),
            ReferenceParam {
                id: 226,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "X1R".to_string(),
            ReferenceParam {
                id: 2740,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "X2R".to_string(),
            ReferenceParam {
                id: 2741,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "RR_1".to_string(),
            ReferenceParam {
                id: 106,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "RR_01".to_string(),
            ReferenceParam {
                id: 105,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "TGM".to_string(),
            ReferenceParam {
                id: 222,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "TGX".to_string(),
            ReferenceParam {
                id: 225,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "FF".to_string(),
            ReferenceParam {
                id: 81,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "DD".to_string(),
            ReferenceParam {
                id: 61,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "RI_01".to_string(),
            ReferenceParam {
                id: 10127,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
        (
            "FG_01".to_string(),
            ReferenceParam {
                id: 10083,
                _element_id: "".to_string(),
                is_scalar: true,
            },
        ),
    ]
    .into_iter()
    .filter(|param| codes.contains(&param.0.as_str()))
    .collect();

    let scalar_paramids: Vec<i32> = code_table
        .values()
        .filter(|ref_param| ref_param.is_scalar)
        .map(|ref_param| ref_param.id)
        .collect();

    Arc::new(RwLock::new(Tables {
        code_table,
        scalar_paramids,
    }))
}
