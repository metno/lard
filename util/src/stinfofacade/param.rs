use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tracing::{error, info, warn};

use crate::stinfofacade::{
    persistence::param::{build_table, load_persisted, persist, Record},
    Error,
};

/// Type that maps a subset of columns from the Stinfosys 'param' table
#[derive(Clone, Debug, PartialEq)]
pub struct ReferenceParam {
    /// Numerical identifier of the parameter (e.g., 212)
    pub id: i32,
    /// Whether the parameter is marked as scalar in Stinfosys
    pub is_scalar: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Tables {
    /// The string here is paramcode, an older text identifier than element_id,
    /// that's used in obsinn's message format
    pub code_table: HashMap<String, ReferenceParam>,
    pub scalar_paramids: Vec<i32>,
}

pub type ParamTables = Arc<RwLock<Tables>>;

pub fn extract_scalar_paramids(params: &[Record]) -> Vec<i32> {
    let mut scalar_paramids: Vec<i32> = params
        .iter()
        .filter(|record| record.is_scalar)
        .map(|record| record.id)
        .collect();
    scalar_paramids.sort();
    scalar_paramids
}

/// Get a fresh cache of param conversions from stinfosys
async fn fetch_params(stinfo_conn_string: Option<&str>) -> Result<Tables, Error> {
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

    // TODO: check this query, particularly that name is indeed paramcode, and
    //     scalar is type bool
    // query param table
    let rows = client
        .query("SELECT paramid, name, scalar FROM param", &[])
        .await
        .inspect_err(|e| warn!("failed to query params: {e}"))?;

    let params: Vec<Record> = rows
        .into_iter()
        .map(|row| Record {
            code: row.get(1),
            id: row.get(0),
            is_scalar: row.get(2),
        })
        .collect();

    let tables = build_table(&params);

    persist(params).await?;

    Ok(tables)
}

pub async fn setup_params(
    stinfo_conn_string: Option<&'static str>,
    mut refresh_interval: tokio::time::Interval,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<(ParamTables, JoinHandle<()>), Error> {
    let param_tables = Arc::new(RwLock::new(match fetch_params(stinfo_conn_string).await {
        Ok(tables) => tables,
        Err(_) => load_persisted().await?,
    }));
    let loop_tables = param_tables.clone();

    let handle = tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                _ = refresh_interval.tick() => {
                    info!("Refreshing level tables");

                    let _ = async {
                        // TODO: make a metric to track these failures?
                        let new_param_tables = fetch_params(stinfo_conn_string).await?;
                        let mut tables = loop_tables.write()?;
                        *tables = new_param_tables;

                        Ok::<(), Error>(())
                    }.await.inspect_err(|err| warn!("failed to refresh param from stinfosys: {err}"));
                }
            }
        }
    });

    Ok((param_tables, handle))
}

/// used for easy construction in tests, not for production
pub fn from_codes(codes: &[&str]) -> ParamTables {
    let code_table: HashMap<String, ReferenceParam> = [
        (
            "TA".to_string(),
            ReferenceParam {
                id: 211,
                is_scalar: true,
            },
        ),
        (
            "UU".to_string(),
            ReferenceParam {
                id: 262,
                is_scalar: true,
            },
        ),
        (
            "CI".to_string(),
            ReferenceParam {
                id: 4,
                is_scalar: true,
            },
        ),
        (
            "IR".to_string(),
            ReferenceParam {
                id: 9,
                is_scalar: true,
            },
        ),
        (
            "KLOBS".to_string(),
            ReferenceParam {
                id: 1022,
                is_scalar: false,
            },
        ),
        (
            "TJ".to_string(),
            ReferenceParam {
                id: 226,
                is_scalar: true,
            },
        ),
        (
            "X1R".to_string(),
            ReferenceParam {
                id: 2740,
                is_scalar: true,
            },
        ),
        (
            "X2R".to_string(),
            ReferenceParam {
                id: 2741,
                is_scalar: true,
            },
        ),
        (
            "RR_1".to_string(),
            ReferenceParam {
                id: 106,
                is_scalar: true,
            },
        ),
        (
            "RR_01".to_string(),
            ReferenceParam {
                id: 105,
                is_scalar: true,
            },
        ),
        (
            "TGM".to_string(),
            ReferenceParam {
                id: 222,
                is_scalar: true,
            },
        ),
        (
            "TGX".to_string(),
            ReferenceParam {
                id: 225,
                is_scalar: true,
            },
        ),
        (
            "FF".to_string(),
            ReferenceParam {
                id: 81,
                is_scalar: true,
            },
        ),
        (
            "DD".to_string(),
            ReferenceParam {
                id: 61,
                is_scalar: true,
            },
        ),
        (
            "RI_01".to_string(),
            ReferenceParam {
                id: 10127,
                is_scalar: true,
            },
        ),
        (
            "FG_01".to_string(),
            ReferenceParam {
                id: 10083,
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
