// Code from ODA:
// https://gitlab.met.no/oda/oda/-/blob/main/internal/cron/filtergen/filtergen.go?ref_type=heads
use chrono::{DateTime, Utc};
use std::collections::hash_map::Entry;
use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, RwLock},
};
use thiserror::Error;
use tokio_postgres::NoTls;
use tracing::error;
use util::PooledPgConn;

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("RwLock was poisoned: {0}")]
    Lock(String),
}

#[derive(Debug, Clone)]
pub struct MessagePriority {
    priority: i32,
    time_resolution: Option<String>,
    from_time: Option<DateTime<Utc>>,
    to_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetLabel {
    id: i32,
    station_id: i32,
    param_id: i32,
    type_id: i32,
    level: i32,
    sensor: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterLabel {
    station_id: i32,
    param_id: i32,
    level: i32,
    sensor: i32,
}

/// This table is where to look for the timeseries priority
/// for a given parameter and typeid
pub type MessagePriorityDefaultTable = HashMap<(i32, i32), MessagePriority>;
/// This table contains more specific exceptions to the default table
pub type MessagePriorityExceptionTable = HashMap<FilterLabel, MessagePriority>;
/// This table contains the filtered timeseries
pub type FilterTimeseriesTable = HashMap<FilterLabel, Vec<i32>>;

/// Get a fresh cache of message priority from stinfosys
pub async fn fetch_message_priority_default(
    stinfo_conn_string: &str,
) -> Result<MessagePriorityDefaultTable, Error> {
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
            "SELECT 
			mpd.message_formatid,
			mpd.paramid,
			mpd.priority,
			CASE
				WHEN trd.time_resolution IS NOT NULL THEN trd.time_resolution
				WHEN trd2.time_resolution IS NOT NULL THEN trd2.time_resolution
				WHEN trd3.time_resolution IS NOT NULL THEN trd3.time_resolution
				ELSE null
			END AS time_resolution,
			mpd.fromtime,
			mpd.totime
		FROM message_priority_default mpd
		LEFT JOIN time_resolution_default trd ON (mpd.message_formatid = trd.message_formatid AND mpd.paramid = trd.paramid)
		LEFT JOIN time_resolution_default trd2 ON (trd2.message_formatid = 0 AND mpd.paramid = trd2.paramid)
		LEFT JOIN time_resolution_default trd3 ON (mpd.message_formatid = trd3.message_formatid AND trd3.paramid = 0)
		ORDER BY message_formatid, paramid",
            &[],
        )
        .await?;

    // build hashmap
    let mut message_priority = HashMap::new();

    for row in rows {
        message_priority.insert(
            (row.get(0), row.get(1)),
            MessagePriority {
                priority: row.get(2),
                time_resolution: row.get(3),
                from_time: row.get(4),
                to_time: row.get(5),
            },
        );
    }

    Ok(message_priority)
}

/// Get a fresh cache of message priority from stinfosys
pub async fn fetch_message_priority_exception(
    stinfo_conn_string: &str,
) -> Result<MessagePriorityExceptionTable, Error> {
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
            "SELECT 
			mpe.stationid,
			mpe.message_formatid,
			mpe.paramid,
			mpe.hlevel,
			mpe.sensor,
			mpe.priority,
			CASE
				WHEN trd.time_resolution IS NOT NULL THEN trd.time_resolution
				WHEN trd2.time_resolution IS NOT NULL THEN trd2.time_resolution
				WHEN trd3.time_resolution IS NOT NULL THEN trd3.time_resolution
				ELSE null
			END AS time_resolution,
			mpe.fromtime,
			mpe.totime
		FROM message_priority_exception mpe
		LEFT JOIN time_resolution_default trd ON (mpe.message_formatid = trd.message_formatid AND mpe.paramid = trd.paramid)
		LEFT JOIN time_resolution_default trd2 ON (trd2.message_formatid = 0 AND mpe.paramid = trd2.paramid)
		LEFT JOIN time_resolution_default trd3 ON (mpe.message_formatid = trd3.message_formatid AND trd3.paramid = 0)
		ORDER BY stationid, message_formatid, paramid",
            &[],
        )
        .await?;

    // build hashmap
    let mut message_priority = HashMap::new();

    for row in rows {
        message_priority.insert(
            FilterLabel {
                station_id: row.get(0),
                param_id: row.get(2),
                level: row.get(3),
                sensor: row.get(4),
            },
            MessagePriority {
                priority: row.get(5),
                time_resolution: row.get(6),
                from_time: row.get(7),
                to_time: row.get(8),
            },
        );
    }

    Ok(message_priority)
}

pub async fn create_filter_timeseries_list(
    conn: &PooledPgConn<'_>,
    default_table: Arc<RwLock<MessagePriorityDefaultTable>>,
    exception_table: Arc<RwLock<MessagePriorityExceptionTable>>,
) -> Result<FilterTimeseriesTable, Error> {
    // TODO: probably don't want to pass these tables in, but rather
    // call the functions to create them inside this function

    let data_results = conn
        .query(
            "SELECT timeseries, station_id, 
            param_id, type_id, lvl, sensor from labels.Met",
            &[],
        )
        .await?;

    let data = {
        let mut data = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push(MetLabel {
                id: row.get(0),
                station_id: row.get(1),
                param_id: row.get(2),
                type_id: row.get(3),
                level: row.get(4),
                sensor: row.get(5),
            });
        }
        data
    };

    let mut flatten_data: HashMap<FilterLabel, Vec<(i32, i32)>> = HashMap::default();
    for ts in data {
        let key = FilterLabel {
            station_id: ts.station_id,
            param_id: ts.param_id,
            level: ts.level,
            sensor: ts.sensor,
        };
        match flatten_data.entry(key) {
            Entry::Vacant(_) => {
                // insert a new value in map
                flatten_data.insert(
                    FilterLabel {
                        station_id: ts.station_id,
                        param_id: ts.param_id,
                        level: ts.level,
                        sensor: ts.sensor,
                    },
                    vec![(ts.type_id, ts.id)],
                );
            }
            Entry::Occupied(mut e) => {
                // append to the vector
                e.get_mut().push((ts.type_id, ts.id));
            }
        }
    }
    let default_table = default_table
        .read()
        .map_err(|e| Error::Lock(e.to_string()))?;
    let exception_table = exception_table
        .read()
        .map_err(|e| Error::Lock(e.to_string()))?;
    // declare the structure we will put the filter in
    let mut filter: FilterTimeseriesTable = HashMap::new();

    // loop over all the timeseries
    for (label, type_id_ts_id_list) in flatten_data {
        // make this into the filter list using the cached maps from stinfosys
        if type_id_ts_id_list.len() > 1 {
            // then actually have to filter, using the default and exception tables
            for (type_id, ts_id) in type_id_ts_id_list {
                let _default = default_table.get(&(label.param_id, type_id));
                let _exception = exception_table.get(&label);
                // TODO: implement filtering to right timeseries
            }
        } else if type_id_ts_id_list.len() == 1 {
            // just add it to the list
            filter.insert(label, vec![type_id_ts_id_list[0].1]);
        }
    }

    Ok(filter)
}
