//! Level as used here refers to meteorological height above ground (or depth
//! if negative) of a sensor in centimetres.
//!
//! "Meteorological" in this case means that the height may not be the sensor's
//! exact physical height, but a standard height close-by which has been
//! approved for comparison to other stations. Also, for certain
//! parameters, this height is not taken relative to the ground (i.e for wind
//! it is taken relative to the top of any uneveness in the nearby landscape).
//! Notably meteorological height differs from but is easily confused with:
//! - elevation/height above sea level (hamsl) - typically refers to the vertical
//!   distance between mean mean sea level and the Earth surface,
//!   in other words the ground the station stands on.
//! - altitude - typically refers to the vertical distance between mean sea level
//!   and the sensor at any point in the atmosphere (ex.: flight, radiosonde,...).
//!  
//!
//! This differs from previous level semantics at Met (notably in ODA, kvalobs,
//! stinfosys), where level typically meant metres (though sometimes centimetres
//! depending on the parameter and as defined by the value hlevel_scale).
//! The level could not be 0 metre as 0 was used as a special value indicating that
//! the level is a default for the param (default value is defined in standard_hlevel
//! as found in stinfosys), and NULL is always equivalent to 0 (when the data is
//! meant to have a level, which not all data does).  In the previous semantics,
//! negative integers were not allowed in kvalobs. Positive integers were given
//! a direction from the stinfosys table sensorlevel defining:
//!   1) height_above_ground in metre, 
//!   2) depth_below_surface in centimetre,
//!   3) depth_below_sea_surface in metre.
//!
//! Justifications for the old semantics (incomplete and inferred, as the
//! people responsible are no longer with us):
//! - `0 = default` reduces configuration work
//! - `NULL = 0` means default levels can be omitted from messages reporting
//!   reporting data from stations, saving some bytes, which used to be quite
//!   expensive.
//!
//! Justifications for changing to the new semantics:
//! - `0 = default` encourages mistakes. A pattern observed at met is people
//!   setting level = 0 for a new sensor to get it working/reporting quickly,
//!   assuming someone will fix it later, which doesn't happen.
//! - `0 = default` makes it impossible to represent a level that is genuinely
//!   0, which does happen (i.e. surface parameters such as surface temperature)
//! - `0 = default` is not what most end users want (although they got used to it)
//!   meaning we either increase the burden on them, or convert to a different
//!   scheme at request time, which is redundant and confusing.
//! - `0 = default` is a friction point with our international collaborators
//!   who nowadays aim to publish physical height instead, only Norway
//!   and the UK Met. Office have not moved away from this practice.
//! - `NULL = 0` removes the ability to use NULL for cases where we don't know
//!   the level, or it isn't relevant.
//! - Inconsistent units and signed integers for level are likely a result of
//!   the system originally being designed for positive integer metres only,
//!   and then having to tack on sub-metre and negative levels.
//!   Might as well fix that while we're making changes.
//!
//! These semantics do not apply to levels in the `kvalobs` and `obsinn` labels
//! as those source-specific labels are meant to reflect how the data was
//! reported in to lard, which in those cases will keep the old semantics.
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use thiserror::Error;
use tokio_postgres::NoTls;
use tracing::{error, warn};

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("RwLock was poisoned: {0}")]
    Lock(String),
    #[error("issues with level conversion: {0}")]
    Level(String),
}

#[derive(Debug, Clone)]
pub struct Level {
    hlevel: i32,
    hlevel_scale: Option<i32>,
}

#[cfg(feature = "integration_tests")]
impl Level {
    pub fn new(hlevel: i32, hlevel_scale: i32) -> Level {
        Level {
            hlevel,
            hlevel_scale: Some(hlevel_scale),
        }
    }
}

type ParamID = i32;

/// this table is where to look for the default level and scale
/// for a given parameter
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
                row.get(2),
                Level {
                    hlevel: row.get(0),
                    hlevel_scale: Some(hlevel_scale),
                },
            );
        } else {
            error!("Invalid hlevel_scale found in stinfosys: {}", hlevel_scale);
        }
    }
    Ok(param_level)
}

pub fn param_get_level(
    level_table: Arc<RwLock<ParamLevelTable>>,
    param_id: i32,
    level: Option<i32>,
) -> Result<Option<i32>, Error> {
    let level_table = level_table.read().map_err(|e| Error::Lock(e.to_string()))?;

    if let Some(param_level) = level_table.get(&param_id) {
        // if level passed into this function is 0, replace with default from stinfosys
        let lvl = match level {
            Some(0) => param_level.hlevel, // this is the default
            Some(lvl) => lvl,              // keep the value
            None => return Ok(None),
        };
        // Convert level to cm (use scale from stinfosys)
        // scale for things that are currently in m is 0, so need to shift
        // could be that we do not have a scale, or encounter ones we have not currently accounted for
        let lvl = match param_level.hlevel_scale {
            Some(0) => lvl * 100, // convert from meters
            Some(-2) => lvl,      // already in cm
            // oh dear, this isn't meters or cm?
            _ => unreachable!(), // this shouldn't happen due to check at import!
        };
        return Ok(Some(lvl));
    }
    warn!("could not find a level for this param: {}", param_id);
    Ok(None)
}
