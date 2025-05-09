use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use thiserror::Error;
use tokio_postgres::NoTls;
use tracing::error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("RwLock was poisoned: {0}")]
    Lock(String),
}

#[derive(Debug, Clone)]
pub struct Level {
    hlevel: i32,
    hlevel_scale: i32,
}

#[cfg(feature = "integration_tests")]
impl Level {
    pub fn new(hlevel: i32, hlevel_scale: i32) -> Level {
        Level {
            hlevel,
            hlevel_scale,
        }
    }
}

type ParamID = i32;

pub type ParamLevelTable = HashMap<ParamID, Level>;

/// Get a fresh cache of levels from stinfosys
pub async fn fetch_levels(stinfo_conn_string: &str) -> Result<ParamLevelTable, Error> {
    // get stinfo conn
    let (client, conn) = tokio_postgres::connect(stinfo_conn_string, NoTls).await?;

    // conn object independently performs communication with database, so needs it's own task.
    // it will return when the client is dropped
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {}", e);
        }
    });

    // query param table
    let rows = client
        .query(
            "SELECT standard_hlevel, hlevel_scale, paramid 
                FROM param WHERE standard_hlevel is not null",
            &[],
        )
        .await?;

    // build hashmap of param permits
    let mut param_level = HashMap::new();

    for row in rows {
        let hlevel_scale: i32 = row.get(1);
        if ![0, -2].contains(&hlevel_scale) {
            // currently only have 0 and -2, aka m and cm
            param_level.insert(
                row.get(3),
                Level {
                    hlevel: row.get(0),
                    hlevel_scale,
                },
            );
        }
    }
    Ok(param_level)
}

pub fn param_get_level(
    level_table: Arc<RwLock<ParamLevelTable>>,
    param_id: i32,
    level: i32,
) -> Result<Option<i32>, Error> {
    let level_table = level_table.read().map_err(|e| Error::Lock(e.to_string()))?;

    if let Some(param_level) = level_table.get(&param_id) {
        // if level is 0, replace with default
        let mut lvl = level;
        if level == 0 {
            lvl = param_level.hlevel; // this is the default
        }
        // Convert level to cm (use scale from stinfosys)
        // scale for things that are currently in m is 0, so need to shift
        let scale = param_level.hlevel_scale;
        return Ok(Some(match scale {
            0 => lvl * 100, // convert from meters
            -2 => lvl,      // already in cm
            // oh dear, this isn't meters or cm?
            _ => unreachable!(), // Shouldn't happen due to check when reading from stinfosys!
        }));
    }
    Ok(None)
}
