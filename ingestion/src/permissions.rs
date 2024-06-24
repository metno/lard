use crate::Error;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio_postgres::NoTls;

#[derive(Debug, Clone)]
pub struct ParamPermit {
    type_id: i32,
    param_id: i32,
    permit_id: i32,
}

#[cfg(feature = "integration_tests")]
impl ParamPermit {
    pub fn new(type_id: i32, param_id: i32, permit_id: i32) -> ParamPermit {
        ParamPermit {
            type_id,
            param_id,
            permit_id,
        }
    }
}

type StationId = i32;
/// This integer is used like an enum in stinfosys to define who data can be shared with. For
/// details on what each number means, refer to the `permit` table in stinfosys. Here we mostly
/// only care that 1 == open
type PermitId = i32;

/// This table is the first place to look for whether a timeseries is open, as it overrides the
/// defaults set in [`StationPermitTable`]. The type_id and param_id here can both be zeroed, which
/// means that entry applies to all type_ids or param_ids respectively. In practice this table is
/// very small, and in most cases we will be looking to [`StaionPermitTable`].
pub type ParamPermitTable = HashMap<StationId, Vec<ParamPermit>>;
/// Entries represent the default [`PermitId`] for all timeseries with the matching station_id.
/// [`ParamPermitTable`] can override this table, so it should be checked first.
pub type StationPermitTable = HashMap<StationId, PermitId>;

/// Get a fresh cache of permits from stinfosys
pub async fn fetch_permits() -> Result<(ParamPermitTable, StationPermitTable), Error> {
    // get stinfo conn
    let (client, conn) = tokio_postgres::connect(&std::env::var("STINFO_STRING")?, NoTls).await?;

    // conn object independently performs communication with database, so needs it's own task.
    // it will return when the client is dropped
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e); // TODO: trace this?
        }
    });

    // query param permit table
    let rows = client
        .query(
            "SELECT stationid, message_formatid, paramid, permitid \
                 FROM v_station_param_policy",
            &[],
        )
        .await?;

    // build hashmap of param permits
    let mut param_permits = HashMap::new();

    for row in rows {
        param_permits
            .entry(row.get(0))
            .or_insert_with(Vec::new)
            .push(ParamPermit {
                type_id: row.get(1),
                param_id: row.get(2),
                permit_id: row.get(3),
            });
    }

    // query station permit table
    let rows = client
        .query(
            "SELECT stationid, permitid \
                 FROM station_policy",
            &[],
        )
        .await?;

    // build hashmap of station permits
    let mut station_permits = HashMap::new();

    for row in rows {
        station_permits.insert(row.get(0), row.get(1));
    }

    Ok((param_permits, station_permits))
}

/// Using cached permits, check whether a given timeseries is open-access
pub fn timeseries_is_open(
    permit_tables: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
    station_id: i32,
    type_id: i32,
    param_id: i32,
) -> Result<bool, Error> {
    let permit_tables = permit_tables
        .read()
        .map_err(|e| Error::Lock(e.to_string()))?;

    if let Some(param_permit_list) = permit_tables.0.get(&station_id) {
        for permit in param_permit_list {
            if (permit.type_id == 0 || permit.type_id == type_id)
                && (permit.param_id == 0 || permit.param_id == param_id)
            {
                return Ok(permit.permit_id == 1);
            }
        }
    }

    if let Some(station_permit) = permit_tables.1.get(&station_id) {
        return Ok(*station_permit == 1);
    }

    Ok(false)
}
