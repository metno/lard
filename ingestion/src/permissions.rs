use crate::Error;
use chrono::{DateTime, Utc};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio_postgres::NoTls;

#[derive(Debug, Clone)]
pub struct Permit {
    type_id: i32, // TODO: make sure these types match the stinfo schema
    param_id: i32,
    level: i32,
    sensor: i32,
    from_time: DateTime<Utc>, // make sure UTC is correct for this
    to_time: Option<DateTime<Utc>>,   // make sure UTC is correct for this
}

type StationId = i32;
pub type PermitTable = HashMap<StationId, Vec<Permit>>;

pub async fn fetch_open_permits() -> Result<PermitTable, Error> {
    // get stinfo conn
    let (client, conn) = tokio_postgres::connect(&std::env::var("STINFO_STRING")?, NoTls).await?;

    // conn object independently performs communication with database, so needs it's own task.
    // it will return when the client is dropped
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e); // TODO: trace this?
        }
    });

    // query permit table
    let rows = client
        .query(
            "SELECT stationid, message_formatid, paramid, hlevel, sensor, fromtime, totime \
                FROM v_message_policy \
                /* we're only interested in open data, which is signalled by permitid = 1 */
                WHERE permitid = 1",
            &[],
        )
        .await?;

    // build hashmap
    let mut open_permits = HashMap::new();

    // indexing rows by number index instead of column name may be more performant
    // I'm not sure if this optimisation is worth the readability tradeoff though, especially
    // since the table if so small and this refresh is a background task
    for row in rows {
        open_permits
            .entry(row.get("stationid"))
            .or_insert_with(Vec::new)
            .push(Permit {
                type_id: row.get("message_formatid"),
                param_id: row.get("paramid"),
                level: row.get("hlevel"),
                sensor: row.get("sensor"),
                from_time: row.get("fromtime"),
                to_time: row.get("totime"),
            });
    }

    Ok(open_permits)
}

pub fn timeseries_is_open(
    open_permits: Arc<RwLock<PermitTable>>,
    station_id: i32,
    type_id: i32,
    param_id: i32,
    sensor_and_level: Option<(i32, i32)>,
    timestamp: DateTime<Utc>,
) -> Result<bool, Error> {
    if let Some(station_permits) = open_permits
        .read()
        .map_err(|e| Error::Lock(e.to_string()))?
        .get(&station_id)
    {
        // unwrapping these as 0 is fine because they're only checked if the values in the permit
        // are not zero. we could keep them as option, but it would make the boolean logic below
        // gnarlier 
        let (sensor, level) = sensor_and_level.unwrap_or((0, 0));

        // if any of the permits fit, return true
        return Ok(station_permits.iter().any(|permit| {
            // permit must apply to all type ids or this specific one
            (permit.type_id == 0 || permit.type_id == type_id) 
                // from_time must be in the past
                && (permit.from_time < timestamp)
                // to_time if exists, must be in the future
                && (permit.to_time.is_none() || (permit.to_time.unwrap() < timestamp))
                // param id 0 means permit covers all params, levels, and sensors
                && (permit.param_id == 0
                    || (permit.param_id == param_id
                        && (permit.sensor == 0 || permit.sensor == sensor)
                        && (permit.level == 0 || permit.level == level)))
        }));
    }

    Ok(false)
}
