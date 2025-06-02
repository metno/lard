//! Level as used here refers to meteorological height above ground (or depth
//! if negative) of a sensor in centimetres.
//!
//! "Meteorological" in this case means that the height may not be the sensor's
//! exact physical height, but a standard height close-by which has been
//! approved for comparison to other stations. Also, for certain
//! parameters, this height is not taken relative to the ground (i.e for wind
//! it is taken relative to the top of any uneveness in the nearby landscape).
//! Notably meteorological height differs from but is easily confused with:
//! - elevation/height above mean sea level (hamsl) - typically refers to the vertical
//!   distance between mean sea level and the Earth surface,
//!   in other words the ground the station stands on.
//! - altitude - typically refers to the vertical distance between mean sea level
//!   and a sensor at any point in the atmosphere (ex.: flight, radiosonde, ...).
//!  
//!
//! This differs from previous level semantics at Met (notably in ODA, kvalobs,
//! stinfosys), where level typically meant metres (though sometimes centimetres
//! depending on the parameter and as defined by the value hlevel_scale).
//! The level could not be 0 metre since 0 was used as a special value indicating that
//! the level is a default for the param (default value is defined in standard_hlevel
//! as found in stinfosys), and NULL is always equivalent to 0 (when the data is
//! meant to have a level, which not all data does).  In the previous semantics,
//! negative integers were not allowed in kvalobs. Positive integers were given
//! a direction in the stinfosys table sensorlevel which defines:
//!   1) height_above_ground in metre,
//!   2) depth_below_surface in centimetre,
//!   3) depth_below_sea_surface in metre.
//!
//! Justifications for the old semantics (incomplete and mostly inferred):
//! - `0 = default` reduces configuration work
//! - `NULL = 0` means default levels can be omitted from messages
//!   reporting data from stations, saving some bytes, which used to be quite
//!   expensive.
//!
//! Justifications for changing to the new semantics:
//! - `0 = default` encourages mistakes. A pattern observed at met is people
//!   setting level = 0 for a new sensor to get it working/reporting quickly,
//!   assuming someone will fix it later, which doesn't happen.
//! - `0 = default` makes it impossible to represent a level different than the default one that is genuinely
//!   0, which does happen (i.e. surface parameters such as surface temperature)
//! - `0 = default` is not what most end users want (although they got used to it)
//!   meaning we either increase the burden on them, or convert to a different
//!   scheme at request time, which is redundant and confusing.
//! - `0 = default` is a friction point with our international collaborators
//!   who nowadays aim to publish physical height instead.
//!   Only Norway met and UK met have not moved away from this practice.
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Unit {
    M,
    Cm,
}

/// Currently stinfosys only allows three directions:
/// `height above ground`, `depth below surface` and `depth below sea surface`.
/// These are defined in the `sensorlevel` table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    /// `height above ground` in stinfosys
    Up,
    /// `depth below surface` or `depth below sea surface`
    Down,
    /// No direction specified in stinfosys
    Missing,
}

/// Level information derived from stinfosys relevant to a single parameter.
/// Useful to convert levels coming from kvalobs/obsinn into our own scheme.
#[derive(Debug, Clone)]
pub struct Level {
    /// The default to be substituted for levels specified as 0 for the given
    /// param
    ///
    /// NOTE: In stinfosys (and obsinn?) these are negative where `Direction`
    /// is `Down` (Similarly to our own scheme), But as kvalobs strips the
    /// signs on all its levels we choose to strip the sign from these too for
    /// simplicity, and rely entirely on `Direction` to determine the sign of
    /// our levels post-conversion.
    default_hlevel: i32,
    unit: Unit,
    direction: Direction,
}

#[cfg(feature = "integration_tests")]
impl Level {
    pub fn new(default_hlevel: i32, unit: Unit, direction: Direction) -> Level {
        Level {
            default_hlevel,
            unit,
            direction,
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
            "SELECT standard_hlevel, hlevel_scale, paramid, sensorlevel_id FROM param \
             JOIN element_info \
             ON param.element_id = element_info.element_id \
             WHERE hlevel_scale IS NOT NULL",
            &[],
        )
        .await?;

    // build hashmap of param permits
    let mut param_level = HashMap::new();

    for row in rows {
        let standard_hlevel: Option<i32> = row.get(0);
        let hlevel_scale = row.get(1);
        let paramid = row.get(2);
        let sensorlevel_id: Option<&str> = row.get(3);

        let unit = match hlevel_scale {
            0 => Unit::M,
            -2 => Unit::Cm,
            _ => {
                error!("Invalid hlevel_scale found in stinfosys: {hlevel_scale:?}");
                continue;
            }
        };

        // If `standard_hlevel` is NULL in stinfosys we set it to 0.
        // This makes sense because the sensors that usually have no default are measuring things
        // essentially at the 'surface' (such as snow temperature). Or they are parameters where
        // the level essentially doesn't matter (KLOBS which is a time).
        // We then take the absolute value since in stinfosys `standard_hlevel` can be negative.
        // We will give it the correct sign during ingestion by checking its direction.
        let default_hlevel = standard_hlevel.unwrap_or(0).abs();

        // convert the `sensorlevel_id` to a direction
        // Down if `sensorlevel_id` from stinfosys is depth below surface or depth below sea surface
        // Currently 3 values: "height_above_ground", "depth_below_surface", and "depth_below_sea_surface"
        // need to change code if more are added to stinfosys
        let direction = match sensorlevel_id {
            Some("height_above_ground") => Direction::Up,
            Some("depth_below_surface") => Direction::Down,
            Some("depth_below_sea_surface") => Direction::Down,
            Some(s) => {
                warn!("Invalid sensorlevel_id found in stinfosys: {s:?}");
                continue;
            }
            None => Direction::Missing,
        };

        param_level.insert(
            paramid,
            Level {
                default_hlevel,
                unit,
                direction,
            },
        );
    }

    Ok(param_level)
}

pub fn param_get_level(
    level_table: Arc<RwLock<ParamLevelTable>>,
    param_id: i32,
    level: Option<i32>,
) -> Result<Option<i32>, Error> {
    let level_table = level_table.read().map_err(|e| Error::Lock(e.to_string()))?;

    // Since we have filled in things from stinfosys as long as we found a scale
    // this means no scale existed for this param, and thus it cannot be used
    // since we cannot convert it, and be sure it has the right units.
    // Thus our only option (if we want to keep this data) is to insert it with
    // NULL, and eventually have it corrected by content managers.
    let Some(param_level) = level_table.get(&param_id) else {
        warn!("could not find a scale for this param: {param_id}");
        return Ok(None);
    };

    // If input level is already NULL, we simply insert NULL
    // however, this should never happen since kvalobs / obsinn have default 0
    let Some(mut lvl) = level else {
        return Ok(None);
    };

    // if level passed into this function is 0, replace with default from stinfosys
    // If there is no default in stinfosys, param_level.default_hlevel is imported as 0
    // in the previous function.
    if lvl == 0 {
        lvl = param_level.default_hlevel;
    }

    // convert level to cm if currently in meters
    if param_level.unit == Unit::M {
        lvl *= 100
    }

    // convert to negative if 'Down'
    if param_level.direction == Direction::Down {
        // NOTE: should abs be done outside of this if, earlier?
        lvl = lvl.abs(); // in case it was signed
        lvl *= -1;
    }

    Ok(Some(lvl))
}
