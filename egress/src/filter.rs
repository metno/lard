// Code from ODA:
// https://gitlab.met.no/oda/oda/-/blob/main/internal/cron/filtergen/filtergen.go?ref_type=heads
// this is for reference since parts of it are reused in some way here. Most specifically the
// calls for metadata from stinfosys. The algorithm itself for creating a "filter" timeseries
// was redone in rust, but the idea remains the same - give the most recomended timeseries at
// any given time (and thus avoid giving multiple overlaping timeseries).
use crate::error::Error;
use chrono::{DateTime, NaiveDateTime, Utc};
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
    // these are here in the call because they were used in other part of the code in ODA
    // there is a chance we may need the information in the (near future)
    _time_resolution: Option<String>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
}

#[cfg(test)]
impl MessagePriority {
    pub fn new(
        priority: i32,
        // these are here in the call because they were used in other part of the code in ODA
        // there is a chance we may need the information in the (near future)
        _time_resolution: Option<String>,
        from: Option<NaiveDateTime>,
        to: Option<NaiveDateTime>,
    ) -> MessagePriority {
        MessagePriority {
            priority,
            _time_resolution,
            from,
            to,
        }
    }
}

// define these types for reuse
type TypeID = i32;
type ParamID = i32;
type TsID = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetLabel {
    id: TsID,
    station_id: i32,
    param_id: ParamID,
    type_id: TypeID,
    level: Option<i32>,
    sensor: Option<i32>,
}

#[cfg(test)]
impl MetLabel {
    pub fn new(
        id: TsID,
        station_id: i32,
        param_id: ParamID,
        type_id: TypeID,
        level: Option<i32>,
        sensor: Option<i32>,
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
// essentially removing the type_id from the label
pub struct FilterLabel {
    station_id: i32,
    param_id: ParamID,
    level: Option<i32>,
    sensor: Option<i32>,
}

#[cfg(test)]
impl FilterLabel {
    pub fn new(
        station_id: i32,
        param_id: ParamID,
        level: Option<i32>,
        sensor: Option<i32>,
    ) -> FilterLabel {
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
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    type_id: TypeID,
    ts_id: TsID,
}

#[cfg(test)]
impl PriorityStruct {
    pub fn new(
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        type_id: TypeID,
        ts_id: TsID,
    ) -> PriorityStruct {
        PriorityStruct {
            from,
            to,
            type_id,
            ts_id,
        }
    }
}

pub struct FilterData {
    _value: f64,
    _timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timerange {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

impl Timerange {
    pub fn new(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> Timerange {
        Timerange { from, to }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Fill {
    // TODO: I'm pretty sure this should never be NULL? In case we can put an Option
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    tsid: TsID,
}

#[cfg(test)]
impl Fill {
    pub fn new(from: DateTime<Utc>, to: Option<DateTime<Utc>>, tsid: TsID) -> Fill {
        Fill { from, to, tsid }
    }
}

const MAX_UTC: DateTime<Utc> = DateTime::<Utc>::MAX_UTC;
const MIN_UTC: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;

/// This table is where to look for the timeseries priority
/// for a given typeid and paramid
pub type MessagePriorityDefaultTable = HashMap<(TypeID, ParamID), MessagePriority>;
/// This table contains more specific exceptions to the default table
/// for a filter label and typeid
pub type MessagePriorityExceptionTable = HashMap<(FilterLabel, TypeID), MessagePriority>;
/// This table contains the filtered timeseries, mapping to typeid and timeseriesid
pub type FilterTimeseriesTable = HashMap<FilterLabel, Vec<Fill>>;

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
                from: row.get(4),
                to: row.get(5),
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
                from: row.get(7),
                to: row.get(8),
            },
        );
    }
    Ok(message_priority)
}

/// Get all the timeseries with MET labels from LARD
/// including their from / to times
pub async fn fetch_timeseries_list_from_database(
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(MetLabel, Timerange)>, Error> {
    let data_results = conn
        .query(
            "SELECT l.timeseries, l.station_id, l.param_id, l.type_id, 
            l.lvl, l.sensor, t.fromtime, t.totime from labels.Met l 
            JOIN timeseries t on t.id=l.timeseries",
            &[],
        )
        .await?;

    let data: Vec<(MetLabel, Timerange)> = {
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
                Timerange {
                    from: row.get(6),
                    to: row.get(7),
                },
            ));
        }
        data
    };
    eprintln!("length timeseries list: {:?}", data.len());
    Ok(data)
}

/// Used to cut the priorities to cover ranges that actually matter to a particular timeseries
/// Takes the from and to times of the timeseries as well as the from and to of the priority range
/// Returns an option, since it could be they do not overlapp at all (and thus it returns empty)
fn timerange_overlap(ts_times: Timerange, priority_times: Timerange) -> Option<Timerange> {
    let fromtime = match (ts_times.from, priority_times.from) {
        (Some(ts_ft), Some(pt_ft)) => Some(ts_ft.max(pt_ft)), // return the later one
        (Some(ts_ft), None) => Some(ts_ft),
        (None, Some(pt_ft)) => Some(pt_ft),
        (None, None) => None,
    };
    let totime = match (ts_times.to, priority_times.to) {
        (Some(ts_tt), Some(pt_tt)) => Some(ts_tt.min(pt_tt)), // return the earlier
        (Some(ts_tt), None) => Some(ts_tt),
        (None, Some(pt_tt)) => Some(pt_tt),
        (None, None) => None,
    };

    match (fromtime, totime) {
        (Some(ft), Some(tt)) => {
            if ft >= tt {
                None
            } else {
                Some(Timerange {
                    from: Some(ft),
                    to: Some(tt),
                })
            }
        }
        (ft, tt) => Some(Timerange { from: ft, to: tt }),
    }
}

fn fill_hole(hole: Timerange, cand: Timerange) -> Option<(Vec<Timerange>, Timerange)> {
    if let Some(overlap) = timerange_overlap(hole, cand) {
        let mut holes = Vec::new();
        if overlap.from != hole.from {
            holes.push(Timerange::new(hole.from, overlap.from));
        }
        if overlap.to != hole.to {
            holes.push(Timerange::new(overlap.to, hole.to));
        }
        Some((holes, overlap))
    } else {
        None
    }
}

fn fill_holes(
    temp_sorted_list: Vec<(Timerange, TypeID, ParamID, TsID)>,
    overall_fromto: Timerange,
) -> Vec<Fill> {
    let mut holes = vec![overall_fromto];

    // output order does not matter
    let mut fills: Vec<Fill> = vec![];

    // TODO: need to make sure temp sorted list is sorted by priority first
    for (candidate, _, _, tsid) in temp_sorted_list {
        let mut remaining_holes = vec![];

        for hole in holes {
            if let Some((new_holes, fill)) = fill_hole(hole, candidate) {
                fills.push(Fill {
                    from: fill.from.unwrap(),
                    to: fill.to,
                    tsid,
                });
                remaining_holes.extend(new_holes);
            } else {
                remaining_holes.push(hole)
            }
        }
        holes = remaining_holes;
    }
    // TODO: what to do if holes.len() > 0 here? Do we care?

    fills
}

/// This function actually creates the filter list that will be used to find one timeseries
/// when not relying on seperating them by typeid
pub fn create_filter_timeseries_table(
    db_ts_list: Vec<(MetLabel, Timerange)>,
    default_table: Arc<RwLock<MessagePriorityDefaultTable>>,
    exception_table: Arc<RwLock<MessagePriorityExceptionTable>>,
) -> Result<FilterTimeseriesTable, Error> {
    // create a list of timeseries with the filter label, which maps to a list of
    // typeid, tsid, and the from/to times of that timeseries
    let mut flatten_data: HashMap<FilterLabel, Vec<(TypeID, TsID, Timerange)>> = HashMap::default();
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
        if !type_ts_time_list.is_empty() {
            // 1 or more timeseries!
            // create a temporary structure for ordering / sorting
            let mut time_pri_typ_ts: Vec<(Timerange, i32, TypeID, TsID)> = vec![];

            for (type_id, ts_id, fromto) in type_ts_time_list {
                // then actually have to filter, using the default and exception tables
                let default = default_table.get(&(type_id, label.param_id));
                let default_0 = default_table.get(&(type_id, 0));
                let exception = exception_table.get(&(label, type_id));

                // TODO: currently ignoring obspgm time ranges, should we also use those like in ODA or is this good enough?
                // NOTE: repetive code, could probably be refactored
                if let Some(def) = default {
                    // use the actual timeseries from / to to cut down the range
                    let times = timerange_overlap(
                        fromto,
                        Timerange {
                            from: def.from.map(|x| x.and_utc()),
                            to: def.to.map(|x| x.and_utc()),
                        },
                    );
                    if let Some(t) = times {
                        // the station specific exceptions
                        // interweave with the other defaults (deleting parts of the default)
                        if let Some(ex) = exception {
                            let times_ex = fill_hole(
                                t,
                                Timerange {
                                    from: ex.from.map(|x| x.and_utc()),
                                    to: ex.to.map(|x| x.and_utc()),
                                },
                            );
                            if let Some((t_list, t_ex)) = times_ex {
                                time_pri_typ_ts.push((t_ex, ex.priority, type_id, ts_id));
                                for t_l in t_list {
                                    time_pri_typ_ts.push((t_l, def.priority, type_id, ts_id));
                                }
                            } else {
                                // no overlap just add
                                time_pri_typ_ts.push((t, def.priority, type_id, ts_id));
                            }
                        } else {
                            // no exceptions
                            time_pri_typ_ts.push((t, def.priority, type_id, ts_id));
                        }
                    }
                }
                // the generic default for paramid "0"
                // paramid 0 applies to all paramids
                // NOTE: only use if no specific default (?)
                else if let Some(def_0) = default_0 {
                    // use the actual timeseries from / to to cut down the range
                    let times = timerange_overlap(
                        fromto,
                        Timerange {
                            from: def_0.from.map(|x| x.and_utc()),
                            to: def_0.to.map(|x| x.and_utc()),
                        },
                    );
                    if let Some(t) = times {
                        // the station specific exceptions
                        // interweave with the other defaults (deleting parts of the default)
                        if let Some(ex) = exception {
                            let times_ex = fill_hole(
                                t,
                                Timerange {
                                    from: ex.from.map(|x| x.and_utc()),
                                    to: ex.to.map(|x| x.and_utc()),
                                },
                            );
                            if let Some((t_list, t_ex)) = times_ex {
                                time_pri_typ_ts.push((t_ex, ex.priority, type_id, ts_id));
                                for t_l in t_list {
                                    time_pri_typ_ts.push((t_l, def_0.priority, type_id, ts_id));
                                }
                            } else {
                                // no overlap just add
                                time_pri_typ_ts.push((t, def_0.priority, type_id, ts_id));
                            }
                        } else {
                            // no exceptions
                            time_pri_typ_ts.push((t, def_0.priority, type_id, ts_id));
                        }
                    }
                }
            }
            // get first and last
            let first_time = time_pri_typ_ts
                .iter()
                .min_by_key(|item| (item.0.from))
                .unwrap()
                .0
                .from;
            let last_time = time_pri_typ_ts
                .iter()
                .min_by_key(|item| (item.0.to))
                .unwrap()
                .0
                .to;

            // sort the list by priority
            time_pri_typ_ts.sort_by_key(|item| (item.1));
            eprintln!("sorted temp prioritites: {time_pri_typ_ts:?}");

            if time_pri_typ_ts.is_empty() {
                // should this happen?
                warn!("length of 0 for this label {:?}", label);
            } else {
                // keep looping until no more holes...
                filter.insert(
                    label,
                    fill_holes(
                        time_pri_typ_ts,
                        Timerange {
                            from: first_time,
                            to: last_time,
                        },
                    ),
                );
            }
        }
    }
    // sort by descending from time since otherwise unordered
    // removing this will cause tests to fail... if its ok for other stuff could potentially be moved into the test framework somehow?
    for list in filter.values_mut() {
        list.sort_by_key(|item| (item.from));
    }
    eprintln!("length filter: {:?}", filter.len());
    Ok(filter)
}

pub async fn get_filter(
    conn: &PooledPgConn<'_>,
    _from: DateTime<Utc>,
    _to: DateTime<Utc>,
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
            // TODO: repace this with logic consistent with the new struct
            applicable_ts.push((1, MIN_UTC, MAX_UTC));
            _ = priorities.first().unwrap().from;
            _ = priorities.first().unwrap().to;
            _ = priorities.first().unwrap().tsid;
            //for prio in priorities {
            //    // is this applicable?
            //    let ft_t = timerange_overlap(
            //        Timerange {
            //            from: Some(from),
            //            to: Some(to),
            //        },
            //        Timerange {
            //            from: prio.from,
            //            to: prio.to,
            //        },
            //    );
            //    // have overlap
            //    if let Some(times) = ft_t {
            //        if let Some(tt) = times.to {
            //            applicable_ts.push((prio.ts_id, times.from.unwrap(), tt));
            //        } else {
            //            // open ended (to time from request)
            //            applicable_ts.push((prio.ts_id, times.from.unwrap(), to));
            //        }
            //    }
            //}
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

// TESTS below here...
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone};

    pub fn mock_filter_default_table() -> Arc<RwLock<MessagePriorityDefaultTable>> {
        let t1: NaiveDateTime = NaiveDate::from_ymd_opt(1500, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let t2: NaiveDateTime = NaiveDate::from_ymd_opt(2006, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let filter_default = HashMap::from([
            (
                (501, 0),
                MessagePriority::new(11110, Some("PT1H".to_string()), Some(t2), None),
            ),
            (
                (330, 0),
                MessagePriority::new(11510, Some("PT1H".to_string()), Some(t2), None),
            ),
            (
                (308, 0),
                MessagePriority::new(14110, Some("PT6H".to_string()), Some(t2), None),
            ),
            (
                (316, 0),
                MessagePriority::new(14510, Some("PT6H".to_string()), Some(t2), None),
            ),
            (
                (3, 0),
                MessagePriority::new(11710, Some("PT1H".to_string()), Some(t2), None),
            ),
            (
                (1001, 0),
                MessagePriority::new(11040, Some("PT1H".to_string()), Some(t1), Some(t2)),
            ),
            (
                (1002, 0),
                MessagePriority::new(14040, Some("P1D".to_string()), Some(t1), Some(t2)),
            ),
        ]);

        Arc::new(RwLock::new(filter_default))
    }

    pub fn mock_filter_exception_table() -> Arc<RwLock<MessagePriorityExceptionTable>> {
        let t1: NaiveDateTime = NaiveDate::from_ymd_opt(1500, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let t2: NaiveDateTime = NaiveDate::from_ymd_opt(2006, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let t3: NaiveDateTime = NaiveDate::from_ymd_opt(2007, 9, 14)
            .unwrap()
            .and_hms_opt(6, 0, 0)
            .unwrap();
        let t4: NaiveDateTime = NaiveDate::from_ymd_opt(2014, 1, 13)
            .unwrap()
            .and_hms_opt(6, 0, 0)
            .unwrap();
        let t5: NaiveDateTime = NaiveDate::from_ymd_opt(2017, 8, 24)
            .unwrap()
            .and_hms_opt(6, 0, 0)
            .unwrap();
        let t6: NaiveDateTime = NaiveDate::from_ymd_opt(2021, 9, 7)
            .unwrap()
            .and_hms_opt(6, 0, 0)
            .unwrap();
        let filter_exception = HashMap::from([
            (
                (FilterLabel::new(99910, 112, Some(0), Some(0)), 501),
                MessagePriority::new(1060, Some("PT1H".to_string()), Some(t6), None), // stinfo: 2021-09-07 06:00:00 |
            ),
            (
                (FilterLabel::new(99910, 112, Some(0), Some(0)), 330),
                MessagePriority::new(99080, Some("PT1H".to_string()), Some(t1), Some(t5)), // stinfo: 1500-01-01 00:00:00 | 2017-08-24 06:00:00
            ),
            (
                (FilterLabel::new(99910, 112, Some(0), Some(0)), 3),
                MessagePriority::new(99090, Some("PT1H".to_string()), Some(t1), Some(t3)), // stinfo: 1500-01-01 00:00:00 | 2007-09-14 06:00:00
            ),
            (
                (FilterLabel::new(99910, 112, Some(0), Some(0)), 308),
                MessagePriority::new(1080, Some("PT6H".to_string()), Some(t3), Some(t4)), // stinfo: 2007-09-14 06:00:00 | 2014-01-13 06:00:00
            ),
            (
                (FilterLabel::new(99910, 112, Some(0), Some(0)), 316),
                MessagePriority::new(1070, Some("PT6H".to_string()), Some(t4), Some(t6)), // stinfo: 2014-01-13 06:00:00 | 2021-09-07 06:00:00
            ),
            (
                (FilterLabel::new(99910, 112, Some(0), Some(0)), 1002),
                MessagePriority::new(1100, Some("P1D".to_string()), Some(t1), Some(t2)), // stinfo: 1500-01-01 00:00:00 | 2006-01-01 06:00:00
            ),
        ]);

        Arc::new(RwLock::new(filter_exception))
    }

    pub fn mock_ts_list() -> Vec<(MetLabel, Timerange)> {
        let t1: DateTime<Utc> = "2021-09-06 13:00:00 +0000".to_string().parse().unwrap();
        let t2: DateTime<Utc> = "2017-08-24 07:00:00 +0000".to_string().parse().unwrap();
        let t3: DateTime<Utc> = "2022-06-20 13:00:00 +0000".to_string().parse().unwrap();
        let t4: DateTime<Utc> = "2007-09-17 08:00:00 +0000".to_string().parse().unwrap();
        let t5: DateTime<Utc> = "2009-12-18 18:00:00 +0000".to_string().parse().unwrap();
        let t6: DateTime<Utc> = "1994-09-04 11:00:00 +0000".to_string().parse().unwrap();
        let t7: DateTime<Utc> = "2005-12-31 23:00:00 +0000".to_string().parse().unwrap();
        let t8: DateTime<Utc> = "2014-01-13 06:00:00 +0000".to_string().parse().unwrap();
        let t9: DateTime<Utc> = "2007-09-14 06:00:00 +0000".to_string().parse().unwrap();

        let ts_list = vec![
            // real(ish) based on lard at some point...
            (
                MetLabel::new(491179, 99910, 112, 501, Some(0), Some(0)),
                Timerange::new(Some(t1), None),
            ),
            (
                MetLabel::new(477764, 99910, 112, 330, Some(0), Some(0)),
                Timerange::new(Some(t2), Some(t3)),
            ),
            (
                MetLabel::new(447225, 99910, 112, 3, Some(0), Some(0)),
                Timerange::new(Some(t4), Some(t5)),
            ),
            (
                MetLabel::new(34452, 99910, 112, 1001, Some(0), Some(0)),
                Timerange::new(Some(t6), Some(t7)),
            ),
            (
                MetLabel::new(70177, 99910, 112, 1002, Some(0), Some(0)),
                Timerange::new(Some(t6), Some(t7)),
            ),
            (
                MetLabel::new(477763, 99910, 112, 316, Some(0), Some(0)),
                Timerange::new(Some(t8), None),
            ),
            (
                MetLabel::new(447224, 99910, 112, 308, Some(0), Some(0)),
                Timerange::new(Some(t9), Some(t8)),
            ),
        ];
        ts_list
    }

    #[test]
    fn test_filter_timeseries_99910() {
        let t0: DateTime<Utc> = "1994-09-04 11:00:00 +0000".to_string().parse().unwrap();
        let t1: DateTime<Utc> = "2005-12-31 23:00:00 +0000".to_string().parse().unwrap();
        let t2: DateTime<Utc> = "2007-09-14 06:00:00 +0000".to_string().parse().unwrap();
        let t3: DateTime<Utc> = "2014-01-13 06:00:00 +0000".to_string().parse().unwrap();
        let t4: DateTime<Utc> = "2021-09-07 06:00:00 +0000".to_string().parse().unwrap();
        let cases = vec![(
            // real case, uses station specific exceptions
            FilterLabel::new(99910, 112, Some(0), Some(0)),
            vec![
                Fill::new(t0, Some(t1), 70177),
                //Fill::new(t1, Some(t2), None),
                Fill::new(t2, Some(t3), 447224),
                Fill::new(t3, Some(t4), 477763),
                Fill::new(t4, None, 491179),
            ],
        )];

        let default_table = mock_filter_default_table();
        let exception_table = mock_filter_exception_table();
        let ts_list = mock_ts_list();
        let output =
            create_filter_timeseries_table(ts_list, default_table.clone(), exception_table.clone())
                .unwrap();

        for (label, filter_list) in cases {
            assert_eq!(output.get(&label), Some(filter_list).as_ref());
        }
    }

    #[test]
    fn test_filter_timeseries_exceptions() {
        // manufactured case to test exception
        // 1 |----->
        // 2   |X-->
        //   0 1 2 3
        let t0: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t0_nd: NaiveDateTime = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let t1_nd: NaiveDateTime = NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let t2_nd: NaiveDateTime = NaiveDate::from_ymd_opt(2024, 1, 3)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let _t3: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();

        let label = FilterLabel::new(1, 1, Some(0), Some(0));
        let expected_output = vec![
            Fill::new(t0, Some(t1), 1),
            Fill::new(t1, Some(t2), 2),
            Fill::new(t2, None, 1),
        ];

        let ts_list = vec![
            (
                MetLabel::new(1, 1, 1, 1, Some(0), Some(0)),
                Timerange::new(Some(t0), None),
            ),
            (
                MetLabel::new(2, 1, 1, 2, Some(0), Some(0)),
                Timerange::new(Some(t1), None),
            ),
        ];

        let defaults = Arc::new(RwLock::new(HashMap::from([
            (
                (1, 0),
                MessagePriority::new(2, Some("PT1H".to_string()), Some(t0_nd), None),
            ),
            (
                (2, 0),
                MessagePriority::new(3, Some("PT1H".to_string()), Some(t0_nd), None),
            ),
        ])));

        let exceptions: Arc<RwLock<MessagePriorityExceptionTable>> =
            Arc::new(RwLock::new(HashMap::from([(
                (FilterLabel::new(1, 1, Some(0), Some(0)), 2),
                MessagePriority::new(1, Some("PT6H".to_string()), Some(t1_nd), Some(t2_nd)),
            )])));

        let output = create_filter_timeseries_table(ts_list, defaults, exceptions).unwrap();

        assert_eq!(output.get(&label), Some(expected_output).as_ref());
    }

    #[test]
    fn test_filter_timeseries() {
        // manufactured case to test hole filling where the first fill candidate is not the best
        // 1 |---|
        // 2   |--->
        // 3 |----->
        //   0 1 2 3
        let t0: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t0_nd: NaiveDateTime = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let _t3: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();

        let label = FilterLabel::new(1, 1, Some(0), Some(0));
        //let filter_list = vec![
        //    PriorityStruct::new(Some(t0), Some(t2), 1, 1),
        //    PriorityStruct::new(Some(t2), None, 2, 2),
        //];
        let expected_output = vec![Fill::new(t0, Some(t2), 1), Fill::new(t2, None, 2)];

        let ts_list = vec![
            (
                MetLabel::new(1, 1, 1, 1, Some(0), Some(0)),
                Timerange::new(Some(t0), Some(t2)),
            ),
            (
                MetLabel::new(2, 1, 1, 2, Some(0), Some(0)),
                Timerange::new(Some(t1), None),
            ),
            (
                MetLabel::new(3, 1, 1, 3, Some(0), Some(0)),
                Timerange::new(Some(t0), None),
            ),
        ];

        let defaults = Arc::new(RwLock::new(HashMap::from([
            (
                (1, 0),
                MessagePriority::new(1, Some("PT1H".to_string()), Some(t0_nd), None),
            ),
            (
                (2, 0),
                MessagePriority::new(2, Some("PT1H".to_string()), Some(t0_nd), None),
            ),
            (
                (3, 0),
                MessagePriority::new(3, Some("PT1H".to_string()), Some(t0_nd), None),
            ),
        ])));

        let exceptions: Arc<RwLock<MessagePriorityExceptionTable>> =
            Arc::new(RwLock::new(HashMap::new()));

        let output = create_filter_timeseries_table(ts_list, defaults, exceptions).unwrap();

        assert_eq!(output.get(&label), Some(expected_output).as_ref());
    }

    #[test]
    fn test_timerange_overlap() {
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let t3: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let t4: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();

        let cases = vec![
            (
                "ts times inside the priority range",
                // ts:             |-----|
                // priority: |-----------------|
                // output:         |-----|
                //           1     2     3     4
                Timerange {
                    from: Some(t2),
                    to: Some(t3),
                },
                Timerange {
                    from: Some(t1),
                    to: Some(t4),
                },
                Some(Timerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times outside the priority range",
                // ts:       |-----------------|
                // priority:       |-----|
                // output:         |-----|
                //           1     2     3     4
                Timerange {
                    from: Some(t1),
                    to: Some(t4),
                },
                Timerange {
                    from: Some(t2),
                    to: Some(t3),
                },
                Some(Timerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times overlapp to time of priority range",
                // ts:             |-----------|
                // priority: |-----------|
                // output:         |-----|
                //           1     2     3     4
                Timerange {
                    from: Some(t2),
                    to: Some(t4),
                },
                Timerange {
                    from: Some(t1),
                    to: Some(t3),
                },
                Some(Timerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times overlapp from time of priority range",
                // ts:       |-----------|
                // priority:       |-----------|
                // output:         |-----|
                //           1     2     3     4
                Timerange {
                    from: Some(t1),
                    to: Some(t3),
                },
                Timerange {
                    from: Some(t2),
                    to: Some(t4),
                },
                Some(Timerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "no overlapp ts first",
                // ts:       |-----|
                // priority:             |-----|
                // output:
                //           1     2     3     4
                Timerange {
                    from: Some(t1),
                    to: Some(t2),
                },
                Timerange {
                    from: Some(t3),
                    to: Some(t4),
                },
                None,
            ),
            (
                "no overlapp priority first",
                // ts:                   |-----|
                // priority: |-----|
                // output:
                //           1     2     3     4
                Timerange {
                    from: Some(t3),
                    to: Some(t4),
                },
                Timerange {
                    from: Some(t1),
                    to: Some(t2),
                },
                None,
            ),
            // handle open endedness!!!
            (
                "ts times inside the priority range, open ended priority",
                // ts:             |-----|
                // priority: <----------------->
                // output:         |-----|
                //           1     2     3     4
                Timerange {
                    from: Some(t2),
                    to: Some(t3),
                },
                Timerange {
                    from: None,
                    to: None,
                },
                Some(Timerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times outside the priority range, open ended ts",
                // ts:       <----------------->
                // priority:       |-----|
                // output:         |-----|
                //           1     2     3     4
                Timerange {
                    from: None,
                    to: None,
                },
                Timerange {
                    from: Some(t2),
                    to: Some(t3),
                },
                Some(Timerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times overlapp one end of priority range, open ended priority first",
                // ts:             |----------->
                // priority: <-----------|
                // output:         |-----|
                //           1     2     3     4
                Timerange {
                    from: Some(t2),
                    to: None,
                },
                Timerange {
                    from: None,
                    to: Some(t3),
                },
                Some(Timerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times overlapp one end of priority range, open ended ts first",
                // ts:       <-----------|
                // priority:       |----------->
                // output:         |-----|
                //           1     2     3     4
                Timerange {
                    from: None,
                    to: Some(t3),
                },
                Timerange {
                    from: Some(t2),
                    to: None,
                },
                Some(Timerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "no overlapp, open ended ts first",
                // ts:       <-----|
                // priority:             |----->
                // output:
                //           1     2     3     4
                Timerange {
                    from: None,
                    to: Some(t2),
                },
                Timerange {
                    from: Some(t3),
                    to: None,
                },
                None,
            ),
            (
                "no overlapp, open ended priority",
                // ts:                   |----->
                // priority: <-----|
                // output:
                //           1     2     3     4
                Timerange {
                    from: Some(t3),
                    to: None,
                },
                Timerange {
                    from: None,
                    to: Some(t2),
                },
                None,
            ),
            (
                "opposite ends touching, open ended",
                // ts:       <-----|
                // priority:       |----->
                // output:
                //           1     2     3     4
                Timerange {
                    from: None,
                    to: Some(t2),
                },
                Timerange {
                    from: Some(t2),
                    to: None,
                },
                None,
            ),
            (
                "same end touching",
                // ts:       |-----|
                // priority: |-----------|
                // output:   |-----|
                //           1     2     3     4
                Timerange {
                    from: Some(t1),
                    to: Some(t2),
                },
                Timerange {
                    from: Some(t1),
                    to: Some(t3),
                },
                Some(Timerange {
                    from: Some(t1),
                    to: Some(t2),
                }),
            ),
        ];

        for (description, ts_times, priority_times, expected_output) in cases {
            let output = timerange_overlap(ts_times, priority_times);
            assert_eq!(output, expected_output, "{}", description);
        }
    }

    #[test]
    fn test_fill_hole() {
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let t3: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let t4: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();

        // returns Option<(Vec<Timerange>, Timerange)>

        let cases = [
            (
                "Total overlap",
                // cand: |-----|
                // hole:   |-|
                // out:    |-|
                // time  1 2 3 4
                (Some(t1), Some(t4)),
                (Some(t2), Some(t3)),
                1,
                1,
                Some((
                    vec![],
                    Timerange {
                        from: Some(t2),
                        to: Some(t3),
                    },
                )),
            ),
            (
                "hole fully contains cand",
                // cand:   |-|
                // hole: |-----|
                // out:  |*|-|*|
                // time  1 2 3 4
                (Some(t2), Some(t3)),
                (Some(t1), Some(t4)),
                1,
                1,
                Some((
                    vec![
                        Timerange {
                            from: Some(t1),
                            to: Some(t2),
                        },
                        Timerange {
                            from: Some(t3),
                            to: Some(t4),
                        },
                    ],
                    Timerange {
                        from: Some(t2),
                        to: Some(t3),
                    },
                )),
            ),
            (
                "left overlap",
                // cand: |---|
                // hole:   |---|
                // out:    |-|*|
                // time  1 2 3 4
                (Some(t1), Some(t3)),
                (Some(t2), Some(t4)),
                1,
                1,
                Some((
                    vec![Timerange {
                        from: Some(t3),
                        to: Some(t4),
                    }],
                    Timerange {
                        from: Some(t2),
                        to: Some(t3),
                    },
                )),
            ),
            (
                "right overlap",
                // cand:   |---|
                // hole: |---|
                // out:  |*|-|
                // time  1 2 3 4
                (Some(t2), Some(t4)),
                (Some(t1), Some(t3)),
                1,
                1,
                Some((
                    vec![Timerange {
                        from: Some(t1),
                        to: Some(t2),
                    }],
                    Timerange {
                        from: Some(t2),
                        to: Some(t3),
                    },
                )),
            ),
            (
                "no overlap right",
                // cand:     |-|
                // hole: |-|
                // out:
                // time  1 2 3 4
                (Some(t3), Some(t4)),
                (Some(t1), Some(t2)),
                1,
                1,
                None,
            ),
            (
                "no overlap left",
                // cand: |-|
                // hole:     |-|
                // out:
                // time  1 2 3 4
                (Some(t1), Some(t2)),
                (Some(t3), Some(t4)),
                1,
                1,
                None,
            ),
            (
                "touching but no overlapp right",
                // cand:   |---|
                // hole: |-|
                // out:
                // time  1 2 3 4
                (Some(t2), Some(t4)),
                (Some(t1), Some(t2)),
                1,
                1,
                None,
            ),
            (
                "touching but no overlapp left",
                // cand: |---|
                // hole:     |-|
                // out:
                // time  1 2 3 4
                (Some(t1), Some(t3)),
                (Some(t3), Some(t4)),
                1,
                1,
                None,
            ),
        ];

        for (message, (cand_ft, cand_tt), (hole_ft, hole_tt), _index, _tsid, expected_output) in
            cases
        {
            let output = fill_hole(
                Timerange {
                    from: hole_ft,
                    to: hole_tt,
                },
                Timerange {
                    from: cand_ft,
                    to: cand_tt,
                },
            );
            assert_eq!(output, expected_output, "{}", message);
        }
    }
}
