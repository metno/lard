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
pub type PermitId = i32;

/// This table is the first place to look for whether a timeseries is open, as it overrides the
/// defaults set in [`StationPermitTable`]. The type_id and param_id here can both be zeroed, which
/// means that entry applies to all type_ids or param_ids respectively. In practice this table is
/// very small, and in most cases we will be looking to [`StaionPermitTable`].
pub type ParamPermitTable = HashMap<StationId, Vec<ParamPermit>>;
/// Entries represent the default [`PermitId`] for all timeseries with the matching station_id.
/// [`ParamPermitTable`] can override this table, so it should be checked first.
pub type StationPermitTable = HashMap<StationId, PermitId>;

pub type PermitTables = Arc<RwLock<(ParamPermitTable, StationPermitTable)>>;

/// Get a fresh cache of permits from stinfosys
pub async fn fetch_permits(
    stinfo_conn_string: &str,
) -> Result<(ParamPermitTable, StationPermitTable), Error> {
    // get stinfo conn
    let (client, conn) = tokio_postgres::connect(stinfo_conn_string, NoTls).await?;

    // conn object independently performs communication with database, so needs it's own task.
    // it will return when the client is dropped
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {}", e); // TODO: should we include this in a metric for alerting?
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

/// Using cached permits, check permit of a given timeseries
///
/// Returns None if no matching permit is found, which we treat as indicating the timeseries
/// is closed. Others (I think Vegar and Terje) have suggested we instead treat this as open, but
/// I (Ingrid) am personally not willing to be responsible for taking that risk
pub fn timeseries_get_permit(
    permit_tables: PermitTables,
    station_id: i32,
    type_id: i32,
    param_id: Option<i32>,
) -> Result<Option<i32>, Error> {
    let permit_tables = permit_tables
        .read()
        .map_err(|e| Error::Lock(e.to_string()))?;

    if let Some(param_id) = param_id {
        if let Some(param_permit_list) = permit_tables.0.get(&station_id) {
            for permit in param_permit_list {
                if (permit.type_id == 0 || permit.type_id == type_id)
                    && (permit.param_id == 0 || permit.param_id == param_id)
                {
                    return Ok(Some(permit.permit_id));
                }
            }
        }
    }

    if let Some(station_permit) = permit_tables.1.get(&station_id) {
        return Ok(Some(*station_permit));
    }

    Ok(None)
}
