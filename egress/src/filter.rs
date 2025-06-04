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
use tracing::{error, warn};
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
    _time_resolution: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterLabel {
    station_id: i32,
    param_id: i32,
    level: i32,
    sensor: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PriorityStruct {
    from_time: Option<DateTime<Utc>>,
    to_time: Option<DateTime<Utc>>,
    type_id: i32,
    ts_id: i32,
}

/// This table is where to look for the timeseries priority
/// for a given typeid and paramid
pub type MessagePriorityDefaultTable = HashMap<(i32, i32), MessagePriority>;
/// This table contains more specific exceptions to the default table
/// for a filter label and typeid
pub type MessagePriorityExceptionTable = HashMap<(FilterLabel, i32), MessagePriority>;
/// This table contains the filtered timeseries, mapping to typeid and timeseriesid?
pub type FilterTimeseriesTable = HashMap<FilterLabel, Vec<PriorityStruct>>;

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
                _time_resolution: row.get(3),
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
    let mut message_priority: HashMap<(FilterLabel, i32), MessagePriority> = HashMap::new();

    for row in rows {
        message_priority.insert(
            (
                FilterLabel {
                    station_id: row.get(0),
                    param_id: row.get(2),
                    level: row.get(3),
                    sensor: row.get(4),
                },
                row.get(1),
            ),
            MessagePriority {
                priority: row.get(5),
                _time_resolution: row.get(6),
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
    // call the functions to create them inside this function?

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
        match type_id_ts_id_list.len() {
            0 => {
                warn!("length of 0 for this label {:?}", label);
            }
            1 => {
                let default = default_table.get(&(label.param_id, type_id_ts_id_list[0].0));
                //let exception = exception_table.get(&(label,type_id_ts_id_list[0].0));

                // skip if no relevant match in message_priority_default
                // unsure if exceptions matter when there is only one?
                if let Some(def) = default {
                    // just add it to the list
                    filter.insert(
                        label,
                        vec![PriorityStruct {
                            from_time: def.from_time,
                            to_time: def.to_time,
                            type_id: type_id_ts_id_list[0].0,
                            ts_id: type_id_ts_id_list[0].1,
                        }],
                    );
                }
                // otherwise still filtered out
            }
            _ => {
                // create a temporary structure for ordering / sorting
                let mut temp_fromtime_priority: Vec<(Option<DateTime<Utc>>, i32, i32, i32)> =
                    vec![];

                for (type_id, ts_id) in type_id_ts_id_list {
                    // then actually have to filter, using the default and exception tables
                    let default = default_table.get(&(label.param_id, type_id));
                    let exception = exception_table.get(&(label, type_id));

                    // TODO: currently ignoring obspgm time ranges, should we also use those like in ODA or is this good enough?
                    // TODO: We need the actual timeseries from / to for a starting point?
                    if let Some(def) = default {
                        temp_fromtime_priority.push((def.from_time, def.priority, type_id, ts_id));
                    }
                    if let Some(ex) = exception {
                        temp_fromtime_priority.push((ex.from_time, ex.priority, type_id, ts_id));
                    }
                }
                // sort the list by time
                temp_fromtime_priority.sort_by(|a, b| a.0.cmp(&b.0));

                // go through from beginning to end comparing the priorities
                let mut previous_priority = 0;
                for (fromtime, priority, typeid, tsid) in temp_fromtime_priority {
                    match filter.entry(label) {
                        Entry::Vacant(_) => {
                            // insert a new value in map
                            filter.insert(
                                label,
                                vec![PriorityStruct {
                                    from_time: fromtime,
                                    to_time: None,
                                    type_id: typeid,
                                    ts_id: tsid,
                                }],
                            );
                            previous_priority = priority;
                        }
                        Entry::Occupied(mut e) => {
                            // append to the vector if priority is a lower number (aka better)
                            if previous_priority > priority {
                                e.get_mut().push(PriorityStruct {
                                    from_time: fromtime,
                                    to_time: None,
                                    type_id: typeid,
                                    ts_id: tsid,
                                });
                                previous_priority = priority;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(filter)
}
