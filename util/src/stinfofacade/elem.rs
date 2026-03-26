use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tracing::{error, info, warn};

use crate::stinfofacade::{
    persistence::elem::{build_table, load_persisted, persist, Elem},
    Error,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Tables {
    pub param_to_elem_table: HashMap<i32, (Option<String>, Option<String>)>,
    pub code_to_param_table: HashMap<String, i32>,
}

pub type ElemTables = Arc<RwLock<Tables>>;

/// Get a fresh cache of elem conversions from stinfosys
async fn fetch_elems(stinfo_conn_string: Option<&str>) -> Result<Tables, Error> {
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

    let tables = build_table(&elems);

    persist(elems).await?;

    Ok(tables)
}

pub async fn setup_elems(
    stinfo_conn_string: Option<&'static str>,
    mut refresh_interval: tokio::time::Interval,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<(ElemTables, JoinHandle<()>), Error> {
    let elem_tables = Arc::new(RwLock::new(match fetch_elems(stinfo_conn_string).await {
        Ok(tables) => tables,
        Err(_) => load_persisted().await?,
    }));
    let loop_tables = elem_tables.clone();

    let handle = tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                _ = refresh_interval.tick() => {
                    info!("Refreshing elem tables");

                    let _ = async {
                        // TODO: make a metric to track these failures?
                        let new_elem_tables = fetch_elems(stinfo_conn_string).await?;
                        let mut tables = loop_tables.write()?;
                        *tables = new_elem_tables;

                        Ok::<(), Error>(())
                    }.await.inspect_err(|err| warn!("failed to refresh elem from stinfosys: {err}"));
                }
            }
        }
    });

    Ok((elem_tables, handle))
}
