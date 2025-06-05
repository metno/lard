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

#[cfg(feature = "integration_tests")]
impl MessagePriority {
    pub fn new(
        priority: i32,
        _time_resolution: Option<String>,
        from_time: Option<DateTime<Utc>>,
        to_time: Option<DateTime<Utc>>,
    ) -> MessagePriority {
        MessagePriority {
            priority,
            _time_resolution,
            from_time,
            to_time,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FromToTimes {
    from_time: Option<DateTime<Utc>>,
    to_time: Option<DateTime<Utc>>,
}

#[cfg(feature = "integration_tests")]
impl FromToTimes {
    pub fn new(from_time: Option<DateTime<Utc>>, to_time: Option<DateTime<Utc>>) -> FromToTimes {
        FromToTimes { from_time, to_time }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetLabel {
    id: i32,
    station_id: i32,
    param_id: i32,
    type_id: i32,
    level: i32,
    sensor: i32,
}

#[cfg(feature = "integration_tests")]
impl MetLabel {
    pub fn new(
        id: i32,
        station_id: i32,
        param_id: i32,
        type_id: i32,
        level: i32,
        sensor: i32,
    ) -> MetLabel {
        MetLabel {
            id,
            station_id,
            param_id,
            type_id,
            level,
            sensor,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterLabel {
    station_id: i32,
    param_id: i32,
    level: i32,
    sensor: i32,
}

#[cfg(feature = "integration_tests")]
impl FilterLabel {
    pub fn new(station_id: i32, param_id: i32, level: i32, sensor: i32) -> FilterLabel {
        FilterLabel {
            station_id,
            param_id,
            level,
            sensor,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PriorityStruct {
    from_time: Option<DateTime<Utc>>,
    to_time: Option<DateTime<Utc>>,
    type_id: i32,
    ts_id: i32,
}

#[cfg(feature = "integration_tests")]
impl PriorityStruct {
    pub fn new(
        from_time: Option<DateTime<Utc>>,
        to_time: Option<DateTime<Utc>>,
        type_id: i32,
        ts_id: i32,
    ) -> PriorityStruct {
        PriorityStruct {
            from_time,
            to_time,
            type_id,
            ts_id,
        }
    }
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

pub async fn fetch_timeseries_list_from_database(
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(MetLabel, FromToTimes)>, Error> {
    let data_results = conn
        .query(
            "SELECT l.timeseries, l.station_id, l.param_id, l.type_id, 
            l.lvl, l.sensor, t.fromtime, t.totime from labels.Met l 
            JOIN timeseries t on t.id=l.timeseries",
            &[],
        )
        .await?;

    let data: Vec<(MetLabel, FromToTimes)> = {
        let mut data = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push((
                MetLabel {
                    id: row.get(0),
                    station_id: row.get(1),
                    param_id: row.get(2),
                    type_id: row.get(3),
                    level: row.get(4),
                    sensor: row.get(5),
                },
                FromToTimes {
                    from_time: row.get(6),
                    to_time: row.get(7),
                },
            ));
        }
        data
    };
    Ok(data)
}

pub fn create_filter_timeseries_list(
    db_ts_list: Vec<(MetLabel, FromToTimes)>,
    default_table: Arc<RwLock<MessagePriorityDefaultTable>>,
    exception_table: Arc<RwLock<MessagePriorityExceptionTable>>,
) -> Result<FilterTimeseriesTable, Error> {
    let mut flatten_data: HashMap<FilterLabel, Vec<(i32, i32, FromToTimes)>> = HashMap::default();
    for ts in db_ts_list {
        // change from metlabel to filterlabel and flatten
        let key = FilterLabel {
            station_id: ts.0.station_id,
            param_id: ts.0.param_id,
            level: ts.0.level,
            sensor: ts.0.sensor,
        };
        flatten_data
            .entry(key)
            .and_modify(|v| v.push((ts.0.type_id, ts.0.id, ts.1)))
            .or_insert(vec![(ts.0.type_id, ts.0.id, ts.1)]);
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
    for (label, type_ts_time_list) in flatten_data {
        // make this into the filter list using the cached maps from stinfosys
        match type_ts_time_list.len() {
            0 => {
                // shouldn't happen since why would it be in the list?
                warn!("length of 0 for this label {:?}", label);
            }
            1 => {
                let default = default_table.get(&(type_ts_time_list[0].0, label.param_id));
                let default_0 = default_table.get(&(type_ts_time_list[0].0, 0));
                //let exception = exception_table.get(&(label,type_id_ts_id_list[0].0));
                let ts_ft: DateTime<Utc> = type_ts_time_list[0]
                    .2
                    .from_time
                    .unwrap_or("0000-01-01 00:00:00 +0000".to_string().parse().unwrap());
                // skip if no relevant match in message_priority_default
                // unsure if exceptions matter when there is only one?
                if let Some(def) = default {
                    // apply the more specific default (matching the actual typeid)
                    let ft: Option<DateTime<Utc>> = if def.from_time.is_some() {
                        if def.from_time.unwrap() > ts_ft {
                            def.from_time
                        } else {
                            Some(ts_ft)
                        }
                    } else {
                        type_ts_time_list[0].2.from_time
                    };
                    filter.insert(
                        label,
                        vec![PriorityStruct {
                            from_time: ft,
                            to_time: def.to_time,
                            type_id: type_ts_time_list[0].0,
                            ts_id: type_ts_time_list[0].1,
                        }],
                    );
                } else if let Some(def_0) = default_0 {
                    // apply where paramid is 0, aka "default"
                    let ft: Option<DateTime<Utc>> = if def_0.from_time.is_some() {
                        if def_0.from_time.unwrap() > ts_ft {
                            def_0.from_time
                        } else {
                            Some(ts_ft)
                        }
                    } else {
                        type_ts_time_list[0].2.from_time
                    };
                    filter.insert(
                        label,
                        vec![PriorityStruct {
                            from_time: ft,
                            to_time: def_0.to_time,
                            type_id: type_ts_time_list[0].0,
                            ts_id: type_ts_time_list[0].1,
                        }],
                    );
                }
                // otherwise still filtered out
            }
            _ => {
                // create a temporary structure for ordering / sorting
                let mut temp_fromtime_priority: Vec<(Option<DateTime<Utc>>, i32, i32, i32)> =
                    vec![];

                for (type_id, ts_id, fromto) in type_ts_time_list {
                    // then actually have to filter, using the default and exception tables
                    let default = default_table.get(&(type_id, label.param_id));
                    let default_0 = default_table.get(&(type_id, 0));
                    let exception = exception_table.get(&(label, type_id));

                    let ts_ft: DateTime<Utc> = fromto
                        .from_time
                        .unwrap_or("0000-01-01 00:00:00 +0000".to_string().parse().unwrap());

                    // TODO: currently ignoring obspgm time ranges, should we also use those like in ODA or is this good enough?
                    // TODO: We need the actual timeseries from / to for a starting point?
                    if let Some(def) = default {
                        let ft: Option<DateTime<Utc>> = if def.from_time.is_some() {
                            if def.from_time.unwrap() > ts_ft {
                                def.from_time
                            } else {
                                Some(ts_ft)
                            }
                        } else {
                            fromto.from_time
                        };
                        temp_fromtime_priority.push((ft, def.priority, type_id, ts_id));
                    }
                    if let Some(def_0) = default_0 {
                        // the generic default for paramid "0"
                        let ft: Option<DateTime<Utc>> = if def_0.from_time.is_some() {
                            if def_0.from_time.unwrap() > ts_ft {
                                def_0.from_time
                            } else {
                                Some(ts_ft)
                            }
                        } else {
                            fromto.from_time
                        };
                        temp_fromtime_priority.push((ft, def_0.priority, type_id, ts_id));
                    }
                    if let Some(ex) = exception {
                        temp_fromtime_priority.push((ex.from_time, ex.priority, type_id, ts_id));
                    }
                }
                // sort the list by time and by priority
                //temp_fromtime_priority.sort_by(|a, b| a.0.cmp(&b.0));
                temp_fromtime_priority.sort_by_key(|item| (item.0, item.1));
                //println!("{:?}", temp_fromtime_priority);

                // these initial previous values will end up being set properly
                // in the vacant part of the loop, to the first values in the list
                let mut previous_priority = 0;
                // go through from beginning to end comparing the priorities
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
    //println!("{:?}", filter);
    Ok(filter)
}
