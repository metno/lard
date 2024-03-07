use std::collections::HashMap;
use tokio_postgres::NoTls;

pub struct Permit {
    type_id: i32, // TODO: make sure these types match the stinfo schema
    param_id: i32,
    level: i32,
    sensor: i32,
}

type StationId = i32;
pub type PermitTable = HashMap<StationId, Vec<Permit>>;

pub async fn fetch_open_permits() -> Result<PermitTable, tokio_postgres::Error> {
    // get stinfo conn
    // TODO: real stinfo connstring
    let (client, conn) = tokio_postgres::connect("stinfo connstring here", NoTls).await?;

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
            "SELECT stationid, message_formatid, paramid, hlevel, sensor \
                FROM v_message_policy \
                /* we're only interested in open data, which is signalled by permitid = 1 */
                WHERE permitid = 1  \
                    /* if there is a totime, the open permit will stop applying at some point, so we
                       shouldn't ingest since we have currently have no mechanism to remove the data when 
                       the permit expires */
                    AND totime IS NULL \
                    /* if fromtime is in the future, the open permit does not apply now */
                    AND (fromtime IS NULL OR fromtime < NOW())",
            &[],
        )
        .await?;

    // build hashmap
    let mut open_permits = HashMap::new();

    for row in rows {
        open_permits
            .entry(row.get("stationid"))
            .or_insert_with(Vec::new)
            .push(Permit {
                type_id: row.get("message_formatid"),
                param_id: row.get("paramid"),
                level: row.get("hlevel"),
                sensor: row.get("sensor"),
            });
    }

    Ok(open_permits)
}

pub fn ts_is_open(
    open_permits: PermitTable,
    station_id: i32,
    type_id: i32,
    param_id: i32,
    sensor_and_level: Option<(i32, i32)>,
) -> bool {
    if let Some(station_permits) = open_permits.get(&station_id) {
        let (sensor, level) = sensor_and_level.unwrap_or((0, 0));

        // if any of the permits fit, return true
        return station_permits.iter().any(|permit| {
            // permit must apply to all type ids or this specific one
            (permit.type_id == 0 || permit.type_id == type_id)
                // param id 0 means permit covers all params, levels, and sensors
                && (permit.param_id == 0
                    || (permit.param_id == param_id
                        && (permit.sensor == 0 || permit.sensor == sensor)
                        && (permit.level == 0 || permit.level == level)))
        });
    }

    false
}
