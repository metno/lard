// Code from ODA:
// https://gitlab.met.no/oda/oda/-/blob/main/internal/cron/filtergen/filtergen.go?ref_type=heads
use crate::error::Error;
use chrono::{DateTime, Utc};
use std::collections::hash_map::Entry;
use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, RwLock},
};
use tokio_postgres::NoTls;
use tracing::{error, warn};
use util::PooledPgConn;

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

pub struct FilterData {
    _value: f64,
    _timestamp: DateTime<Utc>,
}

/// This table is where to look for the timeseries priority
/// for a given typeid and paramid
pub type MessagePriorityDefaultTable = HashMap<(i32, i32), MessagePriority>;
/// This table contains more specific exceptions to the default table
/// for a filter label and typeid
pub type MessagePriorityExceptionTable = HashMap<(FilterLabel, i32), MessagePriority>;
/// This table contains the filtered timeseries, mapping to typeid and timeseriesid
pub type FilterTimeseriesTable = HashMap<FilterLabel, Vec<PriorityStruct>>;

/// Get a fresh cache of message priority from stinfosys
/// this is the defaults for a typeid and paramid
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
/// this is the exceptions, so more specific and includes the station number as well as type id
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

/// Get all the timeseries with MET labels from LARD
/// including their from / to times
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

/// Used to cut the priorities to cover ranges that actually matter to a particular timeseries
/// Takes the from and to times of the timeseries as well as the from and to of the priority range
/// Returns an option, since it could be they do not overlapp at all (and thus it returns empty)
fn cut_from_to_based_on_ts(
    ts_times: FromToTimes,
    priority_times: FromToTimes,
) -> Option<FromToTimes> {
    // look at the fromtime
    let ft: Option<DateTime<Utc>> = if priority_times.from_time.is_some() {
        if ts_times.from_time.is_some() {
            if priority_times.from_time.unwrap() > ts_times.from_time.unwrap() {
                priority_times.from_time
            } else {
                ts_times.from_time
            }
        } else {
            priority_times.from_time
        }
    } else {
        priority_times.from_time
    };
    // look at the totime
    let tt: Option<DateTime<Utc>> = if priority_times.to_time.is_some() {
        if ts_times.to_time.is_some() {
            if priority_times.to_time.unwrap() > ts_times.to_time.unwrap() {
                priority_times.to_time
            } else {
                ts_times.to_time
            }
        } else {
            priority_times.to_time
        }
    } else {
        priority_times.to_time
    };
    // if they now cross, this time does not apply to the timeseries at all...
    if ft.is_some() && tt.is_some() && ft.unwrap() > tt.unwrap() {
        return None; // we return nothing
    }

    let final_times = FromToTimes {
        from_time: ft,
        to_time: tt,
    };
    Some(final_times)
}

/// This function is used once we have a list of potential priority periods that is sorted by fromtime and by priority
/// It iterates over the list until it manages to fill the holes
/// It adds to the current filter list, and then returns the new list
fn fill_holes(
    temp_sorted_list: Vec<(FromToTimes, i32, i32, i32)>,
    label: FilterLabel,
    overall_fromto: FromToTimes,
    current_filter_list: FilterTimeseriesTable,
) -> FilterTimeseriesTable {
    // copy the current, so we can add to it
    let mut filter = current_filter_list.clone();

    // these initial previous values will end up being set properly
    // in the "vacant" part of the loop, to the first values in the list
    let mut previous_struct = PriorityStruct {
        from_time: None,
        to_time: None,
        type_id: 0,
        ts_id: 0,
    };
    let mut previous_priority = 0;

    // need right while condition to fill in holes...
    // keep going if nothing is in the list for that key, if the priority is 0,
    // or if we have not reached the "end" of the overall timeseries
    while !filter.contains_key(&label)
        || previous_priority == 0
        || overall_fromto.to_time != previous_struct.to_time
    {
        // go through from beginning to end comparing the priorities
        for (fromtotimes, priority, typeid, tsid) in temp_sorted_list.as_slice() {
            match filter.entry(label) {
                Entry::Vacant(_) => {
                    let prios = PriorityStruct {
                        from_time: fromtotimes.from_time,
                        to_time: fromtotimes.to_time,
                        type_id: *typeid,
                        ts_id: *tsid,
                    };
                    // insert a new value in map, with the first applicable priority period
                    filter.insert(label, vec![prios]);
                    // update what we are keeping track of outside the loop
                    previous_struct = prios;
                    previous_priority = *priority;
                }
                Entry::Occupied(mut e) => {
                    // append to the vector if priority is a lower number (aka better)
                    if previous_priority > *priority {
                        let prios = PriorityStruct {
                            from_time: fromtotimes.from_time,
                            to_time: fromtotimes.to_time, // don't know yet where it will actuall stop, but current best guess
                            type_id: *typeid,
                            ts_id: *tsid,
                        };
                        // potentially modify the previous entry in the vector (totime)
                        // compare the times to see if need to replace the fromtime of the last entry
                        if previous_struct.to_time.is_some()
                            && fromtotimes.from_time.is_some()
                            && previous_struct.to_time.unwrap() > fromtotimes.from_time.unwrap()
                            || previous_struct.to_time.is_none()
                        // it was left open ended... so close it
                        {
                            // replace the totime
                            previous_struct.to_time = fromtotimes.from_time;
                            // remove last entry in vector and replace
                            e.get_mut().pop();
                            e.get_mut().push(previous_struct);
                        }
                        // append a new priority period
                        e.get_mut().push(prios);
                        // update what we are keeping track of outside the loop
                        previous_struct = prios;
                        previous_priority = *priority;
                    } else if previous_priority == 0 {
                        // there is a hole, so maybe this can fill it?
                        let prios = PriorityStruct {
                            from_time: previous_struct.to_time, // starting where the last one stopped
                            to_time: fromtotimes.to_time,
                            type_id: *typeid,
                            ts_id: *tsid,
                        };
                        if prios.ts_id != previous_struct.ts_id {
                            // don't insert the same one again
                            if previous_struct.to_time < prios.to_time || prios.to_time.is_none() {
                                // will this help us fill the hole?
                                // do not need to modify the previous in the case of a hole!
                                e.get_mut().push(prios);
                                // update what we are keeping track of outside the loop
                                previous_struct = prios;
                                previous_priority = *priority;
                            }
                        }
                    } else if previous_struct.to_time < fromtotimes.from_time {
                        // oh no a hole! backpedal...
                        previous_priority = 0;
                        // start again to loop over the possibilities
                        break; // break out of the FOR loop
                    }
                }
            }
        }
    }
    filter
}

/// This function actually creates the filter list that will be used to find one timeseries
/// when not relying on seperating them by typeid
pub fn create_filter_timeseries_table(
    db_ts_list: Vec<(MetLabel, FromToTimes)>,
    default_table: Arc<RwLock<MessagePriorityDefaultTable>>,
    exception_table: Arc<RwLock<MessagePriorityExceptionTable>>,
) -> Result<FilterTimeseriesTable, Error> {
    // create a list of timeseries with the filter label, which maps to a list of
    // typeid, tsid, and the from/to times of that timeseries
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
    // declare the structure we will keep the list filter in
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
                // this is the simple case where we only have one typeid for this label
                // therefore we either put it in the list, or we don't...
                let default = default_table.get(&(type_ts_time_list[0].0, label.param_id));
                let default_0 = default_table.get(&(type_ts_time_list[0].0, 0));
                //let exception = exception_table.get(&(label,type_id_ts_id_list[0].0));

                // TODO: unsure if exceptions matter when there is only one?
                // TODO: maybe need to splice these together if there is a default and an exception?
                if let Some(def) = default {
                    // apply the more specific default (matching the actual typeid)
                    let times = cut_from_to_based_on_ts(
                        type_ts_time_list[0].2,
                        FromToTimes {
                            from_time: def.from_time,
                            to_time: def.to_time,
                        },
                    );
                    if times.is_some() {
                        filter.insert(
                            label,
                            vec![PriorityStruct {
                                from_time: times.unwrap().from_time,
                                to_time: times.unwrap().to_time,
                                type_id: type_ts_time_list[0].0,
                                ts_id: type_ts_time_list[0].1,
                            }],
                        );
                    }
                } else if let Some(def_0) = default_0 {
                    // apply where paramid is 0, aka "default"
                    let times = cut_from_to_based_on_ts(
                        type_ts_time_list[0].2,
                        FromToTimes {
                            from_time: def_0.from_time,
                            to_time: def_0.to_time,
                        },
                    );
                    if times.is_some() {
                        filter.insert(
                            label,
                            vec![PriorityStruct {
                                from_time: times.unwrap().from_time,
                                to_time: times.unwrap().to_time,
                                type_id: type_ts_time_list[0].0,
                                ts_id: type_ts_time_list[0].1,
                            }],
                        );
                    }
                }
                // otherwise, skip if no relevant match in message_priority_default
            }
            _ => {
                // the more complicated case, have multiple timeseries!
                // create a temporary structure for ordering / sorting
                let mut temp_fromtime_priority: Vec<(FromToTimes, i32, i32, i32)> = vec![];

                for (type_id, ts_id, fromto) in type_ts_time_list {
                    // then actually have to filter, using the default and exception tables
                    let default = default_table.get(&(type_id, label.param_id));
                    let default_0 = default_table.get(&(type_id, 0));
                    let exception = exception_table.get(&(label, type_id));

                    // TODO: currently ignoring obspgm time ranges, should we also use those like in ODA or is this good enough?
                    if let Some(def) = default {
                        // use the actual timeseries from / to to cut down the range
                        let times = cut_from_to_based_on_ts(
                            fromto,
                            FromToTimes {
                                from_time: def.from_time,
                                to_time: def.to_time,
                            },
                        );
                        if let Some(t) = times {
                            temp_fromtime_priority.push((t, def.priority, type_id, ts_id));
                        }
                    }
                    // the generic default for paramid "0"
                    // paramid 0 applies to all paramids
                    if let Some(def_0) = default_0 {
                        // use the actual timeseries from / to to cut down the range
                        let times = cut_from_to_based_on_ts(
                            fromto,
                            FromToTimes {
                                from_time: def_0.from_time,
                                to_time: def_0.to_time,
                            },
                        );
                        if let Some(t) = times {
                            temp_fromtime_priority.push((t, def_0.priority, type_id, ts_id));
                        }
                    }
                    // the station specific exceptions
                    if let Some(ex) = exception {
                        // use the actual timeseries from / to to cut down the range
                        let times = cut_from_to_based_on_ts(
                            fromto,
                            FromToTimes {
                                from_time: ex.from_time,
                                to_time: ex.to_time,
                            },
                        );
                        if let Some(t) = times {
                            temp_fromtime_priority.push((t, ex.priority, type_id, ts_id));
                        }
                    }
                }
                // find the earliest and latest date of the whole list (aka all the timeseries)
                temp_fromtime_priority.sort_by_key(|item| (item.0.to_time));
                let last_time = if temp_fromtime_priority.first().unwrap().0.to_time.is_none() {
                    // open ended
                    temp_fromtime_priority.first().unwrap().0.to_time
                } else {
                    temp_fromtime_priority.last().unwrap().0.to_time
                };
                temp_fromtime_priority.sort_by_key(|item| (item.0.from_time));
                let first_time = temp_fromtime_priority.first().unwrap().0.from_time; // can assume this is not open ended?

                // sort the list by fromtime and by priority
                temp_fromtime_priority.sort_by_key(|item| (item.0.from_time, item.1));
                //println!("temp prioritites: {:?}", temp_fromtime_priority);

                // keep looping until no more holes...
                filter = fill_holes(
                    temp_fromtime_priority,
                    label,
                    FromToTimes {
                        from_time: first_time,
                        to_time: last_time,
                    },
                    filter,
                );
            }
        }
    }
    //println!("filter: {:?}", filter);
    Ok(filter)
}

pub async fn get_filter(
    conn: &PooledPgConn<'_>,
    from_time: DateTime<Utc>,
    to_time: DateTime<Utc>,
    filter_label: FilterLabel,
    filter_list: FilterTimeseriesTable,
) -> Result<Option<Vec<FilterData>>, tokio_postgres::Error> {
    // get the background filter list, and lookup this label
    let filter = filter_list.get(&filter_label);
    // create a structure to keep what is applicable
    let mut applicable_ts: Vec<(i32, DateTime<Utc>, DateTime<Utc>)> = vec![];
    // fill the structure
    match filter {
        Some(priorities) => {
            for prio in priorities {
                // is this applicable?
                let ft_t = cut_from_to_based_on_ts(
                    FromToTimes {
                        from_time: Some(from_time),
                        to_time: Some(to_time),
                    },
                    FromToTimes {
                        from_time: prio.from_time,
                        to_time: prio.to_time,
                    },
                );
                // have overlap
                if let Some(times) = ft_t {
                    if let Some(tt) = times.to_time {
                        applicable_ts.push((prio.ts_id, times.from_time.unwrap(), tt));
                    } else {
                        // open ended (to time from request)
                        applicable_ts.push((prio.ts_id, times.from_time.unwrap(), to_time));
                    }
                }
            }
        }
        None => return Ok(None), // no prioritized timeseries
    }

    // create sql to get right timeseries for filter
    let mut sql = String::from("SELECT obsvalue, obstime FROM data");
    let mut iter = applicable_ts.iter().peekable();
    while let Some(x) = iter.next() {
        let get_ts = format!(
            "WHERE (timeseries = {} \
                    AND obstime BETWEEN '{}' AND '{}')",
            x.0, x.1, x.1,
        );
        sql += &get_ts;
        if iter.peek().is_some() {
            sql.push_str(" AND ");
        }
    }
    let data_results = conn.query(&sql, &[]).await?;

    let data = {
        let mut data = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push(FilterData {
                _value: row.get(0),
                _timestamp: row.get(1),
            });
        }

        data
    };

    Ok(Some(data))
}
