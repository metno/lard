use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Utc};
use pg_interval::Interval;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::{Error, patchwork::Label};
use ::util::{
    DbPools, MetLabel, ParamId, PermitId, PooledPgConn, TsId, TypeId,
    stinfofacade::{
        self,
        message_priority::{DefaultTable, ExceptionTable, MessagePriority},
    },
    timerange,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Fill {
    // TODO: I'm pretty sure this should never be NULL? In case we can put an Option
    pub from: DateTime<Utc>,
    pub to: Option<DateTime<Utc>>,
    pub tsid: TsId,
    pub permit: PermitId,
    pub typeid: i32,
    pub timeresolution: Option<Interval>,
}

impl Fill {
    pub fn new(
        from: DateTime<Utc>,
        to: Option<DateTime<Utc>>,
        tsid: i64,
        permit: i32,
        typeid: i32,
        timeresolution: Option<Interval>,
    ) -> Fill {
        Fill {
            from,
            to,
            tsid,
            permit,
            typeid,
            timeresolution,
        }
    }
}

/// Contains patchworked timeseries, mapping to typeid and timeseriesid
pub type Table = HashMap<Label, Vec<Fill>>;

#[derive(Debug, Clone)]
pub struct Tables {
    pub open: Arc<RwLock<Table>>,
    pub restricted: Arc<RwLock<Table>>,
}

impl Tables {
    pub fn new(open: Table, restricted: Table) -> Self {
        Self {
            open: Arc::new(RwLock::new(open)),
            restricted: Arc::new(RwLock::new(restricted)),
        }
    }

    pub async fn setup(
        stinfo_conn_string: Option<&'static str>,
        pools: DbPools,
        mut refresh_interval: tokio::time::Interval,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<(Self, JoinHandle<()>), Error> {
        let loop_pools = pools.clone();
        let open_conn = pools.open.get().await?;
        let restricted_conn = pools.restricted.get().await?;
        let tables = Self::new(
            fetch_patchwork_table(&open_conn, stinfo_conn_string).await?,
            fetch_patchwork_table(&restricted_conn, stinfo_conn_string).await?,
        );
        let loop_tables = tables.clone();

        let handle = tokio::task::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    _ = refresh_interval.tick() => {
                        info!("Refreshing patchwork table");

                        let _ = async {
                            let open_conn = &loop_pools.open.get().await?;
                            let restricted_conn = &loop_pools.restricted.get().await?;

                            let new_open_table = fetch_patchwork_table(open_conn, stinfo_conn_string)
                                .await
                                ?;

                            let new_restricted_table =
                                fetch_patchwork_table(restricted_conn, stinfo_conn_string)
                                    .await
                                    ?;

                            let mut open_table = loop_tables.open.write()?;
                            *open_table = new_open_table;

                            let mut restricted_table = loop_tables.restricted.write()?;
                            *restricted_table = new_restricted_table;

                            Ok::<(), Error>(())
                        }.await.inspect_err(|err| warn!("failed to refresh patchwork table: {err}"));
                    }
                }
            }
        });

        Ok((tables, handle))
    }
}

/// Get all the timeseries with MET labels from LARD
/// including their from / to times
pub async fn fetch_timeseries_list_from_database(
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(MetLabel, PermitId, Option<Interval>, timerange::Open)>, Error> {
    // NOTE: currently skipping null param ids that we plan to remove in the future
    // NOTE: also avoiding timeseries with no permit (currently unaccessible)
    let data_results = conn
        .query(
            "SELECT \
                l.timeseries, \
                l.station_id, \
                l.param_id, \
                l.type_id, \
                l.lvl, \
                l.sensor, \
                t.fromtime, \
                t.totime, \
                t.permit, \
                t.timeresolution \
            FROM labels.met l \
            JOIN timeseries t \
                ON t.id = l.timeseries \
            WHERE l.param_id IS NOT NULL \
            AND t.permit IS NOT NULL",
            &[],
        )
        .await?;

    let data: Vec<(MetLabel, PermitId, Option<Interval>, timerange::Open)> = {
        let mut data = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push((
                MetLabel::new(
                    row.get(0),
                    row.get(1),
                    row.get(2),
                    row.get(3),
                    row.get(4),
                    row.get(5),
                ),
                row.get(8),
                row.get(9),
                timerange::Open {
                    from: row.get(6),
                    to: row.get(7),
                },
            ));
        }
        data
    };
    Ok(data)
}

pub async fn fetch_patchwork_table(
    conn: &PooledPgConn<'_>,
    stinfo_conn_string: Option<&str>,
) -> Result<Table, Error> {
    // TODO: this should be separate from the stinfosys stuff
    let db_ts_list = fetch_timeseries_list_from_database(conn).await?;

    let (default_table, exception_table) =
        stinfofacade::message_priority::fetch_message_priority(stinfo_conn_string).await?;

    create_patchwork_timeseries_table(db_ts_list, &default_table, &exception_table)
}

fn process_priorities(
    timerange: timerange::Open,
    default: Option<&MessagePriority>,
    exception: Option<&MessagePriority>,
) -> Option<Vec<(timerange::Open, i32)>> {
    let default = default?;

    let times = timerange.overlap(timerange::Open {
        from: default.timerange.from,
        to: default.timerange.to,
    })?;

    // this patches the exceptions (often station specific) over the defaults where applicable
    let out = patch_default(times, default.priority, exception)
        .unwrap_or(vec![(times, default.priority)]);

    Some(out)
}

fn patch_default(
    timerange: timerange::Open,
    priority: i32,
    exception: Option<&MessagePriority>,
) -> Option<Vec<(timerange::Open, i32)>> {
    let ex = exception?;

    // NOTE: here `fill_hole` is used to fill the timerange covered by the default prioriry
    // with the exceptions (ie, deleting parts of the default where necessary)
    let (t_list, t_ex) = fill_hole(
        timerange,
        timerange::Open {
            from: ex.timerange.from,
            to: ex.timerange.to,
        },
    )?;

    let mut ranges = vec![(t_ex, ex.priority)];

    for t_l in t_list {
        ranges.push((t_l, priority));
    }

    Some(ranges)
}

/// If the timeranges overlap, we return a vector of remaining holes and the overlap (ie, the portion of the input hole filled by the candidate)
fn fill_hole(
    hole: timerange::Open,
    cand: timerange::Open,
) -> Option<(Vec<timerange::Open>, timerange::Open)> {
    let overlap = hole.overlap(cand)?;

    let mut holes = Vec::new();
    if overlap.from != hole.from {
        holes.push(timerange::open(hole.from, overlap.from));
    }
    if overlap.to != hole.to {
        holes.push(timerange::open(overlap.to, hole.to));
    }

    Some((holes, overlap))
}

fn fill_holes(
    temp_sorted_list: Vec<(
        timerange::Open,
        TypeId,
        ParamId,
        TsId,
        PermitId,
        Option<Interval>,
    )>,
    overall_fromto: timerange::Open,
) -> Vec<Fill> {
    let mut holes = vec![overall_fromto];

    // output order does not matter
    let mut fills: Vec<Fill> = vec![];

    // TODO: need to make sure temp sorted list is sorted by priority first
    for (candidate, _priority, typeid, tsid, permit, timeresolution) in temp_sorted_list {
        let mut remaining_holes = vec![];

        for hole in holes {
            if let Some((new_holes, fill)) = fill_hole(hole, candidate) {
                fills.push(Fill {
                    from: fill.from.unwrap(),
                    to: fill.to,
                    tsid,
                    permit,
                    typeid,
                    timeresolution,
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

/// This function actually creates the patchwork list that will be used to find one timeseries
/// when not relying on seperating them by typeid
pub fn create_patchwork_timeseries_table(
    db_ts_list: Vec<(MetLabel, PermitId, Option<Interval>, timerange::Open)>,
    default_table: &DefaultTable,
    exception_table: &ExceptionTable,
) -> Result<Table, Error> {
    // create a list of timeseries with the patchwork label, which maps to a list of
    // typeid, tsid, and the from/to times of that timeseries
    let mut flatten_data = HashMap::new();
    for (label, permit, timeresolution, timerange) in db_ts_list {
        // change from metlabel to patchwork::Label and flatten
        let key = Label {
            station_id: label.key.station_id,
            param_id: label.key.param_id,
            level: label.key.level,
            sensor: label.key.sensor,
        };
        flatten_data.entry(key).or_insert_with(Vec::new).push((
            label.key.type_id,
            label.id,
            permit,
            timeresolution,
            timerange,
        ));
    }
    // declare the structure we will keep the patchwork list in
    let mut patchwork = HashMap::new();

    // loop over all the timeseries in groupings based on the patchwork label
    // this means that all the timeseries have the same label, but have different typeids
    // this is the essence of the patchwork, as it needs to decide which are prioritized for
    // different time ranges.
    for (label, type_ts_time_list) in flatten_data {
        if type_ts_time_list.is_empty() {
            continue;
        }

        // create a temporary structure for ordering / sorting
        let mut time_pri_typ_ts_perm_res: Vec<(
            timerange::Open,
            i32,
            TypeId,
            TsId,
            PermitId,
            Option<Interval>,
        )> = vec![];

        // NOTE: param 0 is used to represent all parameters rather than listing them all, therefore
        // it is the fall back in this code
        // NOTE: sensor 0 does not represent all sensors, those need to be listed specifically
        // NOTE: level 0 has been converted in the the import for the exeption table to the default
        // level for that paramid. Individual levels also need to be listed specifically.

        // make this into the patchwork list using the cached maps from stinfosys
        for (type_id, ts_id, permit, timeresolution, fromto) in type_ts_time_list {
            // then actually have to prioritize, using the default and exception tables
            let default = default_table.get(&(type_id, label.param_id));
            // if there's not a param specific default, the default for param 0 applies to all params on that station,
            // so we check that as a backup
            let default_0 = default_table.get(&(type_id, 0));
            let mut exception = exception_table.get(&(label, type_id));
            let exception_0 = exception_table.get(&(
                Label::new(label.station_id, 0, label.level, label.sensor),
                type_id,
            ));
            // fallback to the param 0 is default for all if nothing specific is found from the exception table
            if exception.is_none() && exception_0.is_some() {
                exception = exception_0;
            }

            // TODO: currently ignoring obspgm time ranges, should we also use those like in ODA or is this good enough?
            if let Some(tss) = process_priorities(fromto, default, exception) {
                for (range, priority) in tss {
                    time_pri_typ_ts_perm_res.push((
                        range,
                        priority,
                        type_id,
                        ts_id,
                        permit,
                        timeresolution,
                    ))
                }
            } else if let Some(tss) = process_priorities(fromto, default_0, exception) {
                for (range, priority) in tss {
                    time_pri_typ_ts_perm_res.push((
                        range,
                        priority,
                        type_id,
                        ts_id,
                        permit,
                        timeresolution,
                    ))
                }
            }
        }

        if time_pri_typ_ts_perm_res.is_empty() {
            // should this happen?
            warn!("no priorities found for this label {:?}", label);
            continue;
        }
        // get first and last
        let first_time = time_pri_typ_ts_perm_res
            .iter()
            .map(|item| item.0.from)
            .min()
            .unwrap();
        let last_time = if time_pri_typ_ts_perm_res
            .iter()
            .any(|item| item.0.to.is_none())
        {
            // if there is a None to time, that means the series is open ended,
            // which is the latest possible to time. but Option's Ord impl
            // counts None as less than Some. So we have this if check to
            // override that behaviour
            None
        } else {
            time_pri_typ_ts_perm_res
                .iter()
                .map(|item| item.0.to)
                .max()
                .unwrap()
        };

        // sort the list by priority
        time_pri_typ_ts_perm_res.sort_by_key(|item| item.1);

        // loop through timeseries in priority order to fill any remaining gaps in the target timerange
        // until we either fill everything, or run out of timeseries
        patchwork.insert(
            label,
            fill_holes(
                time_pri_typ_ts_perm_res,
                timerange::Open {
                    from: first_time,
                    to: last_time,
                },
            ),
        );
    }
    // sort by descending from time since otherwise unordered
    // removing this will cause tests to fail... if its ok for other stuff could potentially be moved into the test framework somehow?
    for list in patchwork.values_mut() {
        list.sort_by_key(|item| item.from);
    }
    Ok(patchwork)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    pub fn mock_default_table() -> DefaultTable {
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(1500, 1, 1, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2006, 1, 1, 0, 0, 0).unwrap();

        HashMap::from([
            (
                (501, 0),
                MessagePriority::new(11110, timerange::open(Some(t2), None)),
            ),
            (
                (330, 0),
                MessagePriority::new(11510, timerange::open(Some(t2), None)),
            ),
            (
                (308, 0),
                MessagePriority::new(14110, timerange::open(Some(t2), None)),
            ),
            (
                (316, 0),
                MessagePriority::new(14510, timerange::open(Some(t2), None)),
            ),
            (
                (3, 0),
                MessagePriority::new(11710, timerange::open(Some(t2), None)),
            ),
            (
                (1001, 0),
                MessagePriority::new(11040, timerange::open(Some(t1), Some(t2))),
            ),
            (
                (1002, 0),
                MessagePriority::new(14040, timerange::open(Some(t1), Some(t2))),
            ),
        ])
    }

    pub fn mock_exception_table() -> ExceptionTable {
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(1500, 1, 1, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2006, 1, 1, 0, 0, 0).unwrap();
        let t3: DateTime<Utc> = Utc.with_ymd_and_hms(2007, 9, 14, 6, 0, 0).unwrap();
        let t4: DateTime<Utc> = Utc.with_ymd_and_hms(2014, 1, 13, 6, 0, 0).unwrap();
        let t5: DateTime<Utc> = Utc.with_ymd_and_hms(2017, 8, 24, 6, 0, 0).unwrap();
        let t6: DateTime<Utc> = Utc.with_ymd_and_hms(2021, 9, 7, 6, 0, 0).unwrap();
        HashMap::from([
            (
                (Label::new(99910, 112, Some(0), Some(0)), 501),
                MessagePriority::new(1060, timerange::open(Some(t6), None)), // stinfo: 2021-09-07 06:00:00 |
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 330),
                MessagePriority::new(99080, timerange::open(Some(t1), Some(t5))), // stinfo: 1500-01-01 00:00:00 | 2017-08-24 06:00:00
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 3),
                MessagePriority::new(99090, timerange::open(Some(t1), Some(t3))), // stinfo: 1500-01-01 00:00:00 | 2007-09-14 06:00:00
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 308),
                MessagePriority::new(1080, timerange::open(Some(t3), Some(t4))), // stinfo: 2007-09-14 06:00:00 | 2014-01-13 06:00:00
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 316),
                MessagePriority::new(1070, timerange::open(Some(t4), Some(t6))), // stinfo: 2014-01-13 06:00:00 | 2021-09-07 06:00:00
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 1002),
                MessagePriority::new(1100, timerange::open(Some(t1), Some(t2))), // stinfo: 1500-01-01 00:00:00 | 2006-01-01 06:00:00
            ),
        ])
    }

    pub fn mock_ts_list() -> Vec<(MetLabel, PermitId, Option<Interval>, timerange::Open)> {
        let t1: DateTime<Utc> = "2021-09-06 13:00:00 +0000".to_string().parse().unwrap();
        let t2: DateTime<Utc> = "2017-08-24 07:00:00 +0000".to_string().parse().unwrap();
        let t3: DateTime<Utc> = "2022-06-20 13:00:00 +0000".to_string().parse().unwrap();
        let t4: DateTime<Utc> = "2007-09-17 08:00:00 +0000".to_string().parse().unwrap();
        let t5: DateTime<Utc> = "2009-12-18 18:00:00 +0000".to_string().parse().unwrap();
        let t6: DateTime<Utc> = "1994-09-04 11:00:00 +0000".to_string().parse().unwrap();
        let t7: DateTime<Utc> = "2005-12-31 23:00:00 +0000".to_string().parse().unwrap();
        let t8: DateTime<Utc> = "2014-01-13 06:00:00 +0000".to_string().parse().unwrap();
        let t9: DateTime<Utc> = "2007-09-14 06:00:00 +0000".to_string().parse().unwrap();

        vec![
            // real(ish) based on lard at some point...
            (
                MetLabel::new(491179, 99910, 112, 501, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t1), None),
            ),
            (
                MetLabel::new(477764, 99910, 112, 330, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t2), Some(t3)),
            ),
            (
                MetLabel::new(447225, 99910, 112, 3, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t4), Some(t5)),
            ),
            (
                MetLabel::new(34452, 99910, 112, 1001, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t6), Some(t7)),
            ),
            (
                MetLabel::new(70177, 99910, 112, 1002, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t6), Some(t7)),
            ),
            (
                MetLabel::new(477763, 99910, 112, 316, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t8), None),
            ),
            (
                MetLabel::new(447224, 99910, 112, 308, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t9), Some(t8)),
            ),
        ]
    }

    pub fn mock_ts_not_in_priorities_list()
    -> Vec<(MetLabel, PermitId, Option<Interval>, timerange::Open)> {
        let t1: DateTime<Utc> = "2021-09-06 13:00:00 +0000".to_string().parse().unwrap();
        // the type id does not exist...
        vec![(
            MetLabel::new(123456, 9999, 112, 1234, Some(0), Some(0)),
            1,
            Some(Interval::new(0, 0, 3600000000)),
            timerange::open(Some(t1), None),
        )]
    }

    #[test]
    fn test_patchwork_timeseries_99910() {
        let t0: DateTime<Utc> = "1994-09-04 11:00:00 +0000".to_string().parse().unwrap();
        let t1: DateTime<Utc> = "2005-12-31 23:00:00 +0000".to_string().parse().unwrap();
        let t2: DateTime<Utc> = "2007-09-14 06:00:00 +0000".to_string().parse().unwrap();
        let t3: DateTime<Utc> = "2014-01-13 06:00:00 +0000".to_string().parse().unwrap();
        let t4: DateTime<Utc> = "2021-09-07 06:00:00 +0000".to_string().parse().unwrap();
        let cases = vec![(
            // real case, uses station specific exceptions
            Label::new(99910, 112, Some(0), Some(0)),
            vec![
                Fill::new(
                    t0,
                    Some(t1),
                    70177,
                    1,
                    1002,
                    Some(Interval::new(0, 0, 3600000000)),
                ),
                //Fill::new(t1, Some(t2), None, 1),
                Fill::new(
                    t2,
                    Some(t3),
                    447224,
                    1,
                    308,
                    Some(Interval::new(0, 0, 3600000000)),
                ),
                Fill::new(
                    t3,
                    Some(t4),
                    477763,
                    1,
                    316,
                    Some(Interval::new(0, 0, 3600000000)),
                ),
                Fill::new(
                    t4,
                    None,
                    491179,
                    1,
                    501,
                    Some(Interval::new(0, 0, 3600000000)),
                ),
            ],
        )];

        let default_table = mock_default_table();
        let exception_table = mock_exception_table();
        let ts_list = mock_ts_list();
        let output =
            create_patchwork_timeseries_table(ts_list, &default_table, &exception_table).unwrap();

        for (label, patchwork_list) in cases {
            assert_eq!(output.get(&label), Some(patchwork_list).as_ref());
        }
    }

    #[test]
    fn test_patchwork_timeseries_not_found_in_priorities() {
        // try to see what happens if hit the warning "no priorities found for this label"
        let cases = vec![(Label::new(9999, 112, Some(0), Some(0)), None)];
        let default_table = mock_default_table();
        let exception_table = mock_exception_table();
        let ts_list = mock_ts_not_in_priorities_list();
        let output =
            create_patchwork_timeseries_table(ts_list, &default_table, &exception_table).unwrap();

        for (label, patchwork_option) in cases {
            assert_eq!(output.get(&label), patchwork_option);
        }
    }

    #[test]
    fn test_patchwork_timeseries_exceptions() {
        // manufactured case to test exception
        // 1 |----->
        // 2   |X-->
        //   0 1 2 3
        let t0: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let _t3: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();

        let label = Label::new(1, 1, Some(0), Some(0));
        let expected_output = vec![
            Fill::new(t0, Some(t1), 1, 1, 1, Some(Interval::new(0, 0, 3600000000))),
            Fill::new(t1, Some(t2), 2, 1, 2, Some(Interval::new(0, 0, 3600000000))),
            Fill::new(t2, None, 1, 1, 1, Some(Interval::new(0, 0, 3600000000))),
        ];

        let ts_list = vec![
            (
                MetLabel::new(1, 1, 1, 1, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t0), None),
            ),
            (
                MetLabel::new(2, 1, 1, 2, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t1), None),
            ),
        ];

        let defaults = HashMap::from([
            (
                (1, 0),
                MessagePriority::new(2, timerange::open(Some(t0), None)),
            ),
            (
                (2, 0),
                MessagePriority::new(3, timerange::open(Some(t0), None)),
            ),
        ]);

        let exceptions: ExceptionTable = HashMap::from([(
            (Label::new(1, 1, Some(0), Some(0)), 2),
            MessagePriority::new(1, timerange::open(Some(t1), Some(t2))),
        )]);

        let output = create_patchwork_timeseries_table(ts_list, &defaults, &exceptions).unwrap();

        assert_eq!(output.get(&label), Some(expected_output).as_ref());
    }

    #[test]
    fn test_patchwork_timeseries() {
        // manufactured case to test hole filling where the first fill candidate is not the best
        // 1 |---|
        // 2   |--->
        // 3 |----->
        //   0 1 2 3
        let t0: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let _t3: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();

        let label = Label::new(1, 1, Some(0), Some(0));
        let expected_output = vec![
            Fill::new(t0, Some(t2), 1, 1, 1, Some(Interval::new(0, 0, 3600000000))),
            Fill::new(t2, None, 2, 1, 2, Some(Interval::new(0, 0, 3600000000))),
        ];
        let ts_list = vec![
            (
                MetLabel::new(1, 1, 1, 1, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t0), Some(t2)),
            ),
            (
                MetLabel::new(2, 1, 1, 2, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t1), None),
            ),
            (
                MetLabel::new(3, 1, 1, 3, Some(0), Some(0)),
                1,
                Some(Interval::new(0, 0, 3600000000)),
                timerange::open(Some(t0), None),
            ),
        ];

        let defaults = HashMap::from([
            (
                (1, 0),
                MessagePriority::new(1, timerange::open(Some(t0), None)),
            ),
            (
                (2, 0),
                MessagePriority::new(2, timerange::open(Some(t0), None)),
            ),
            (
                (3, 0),
                MessagePriority::new(3, timerange::open(Some(t0), None)),
            ),
        ]);

        let exceptions: ExceptionTable = HashMap::new();

        let output = create_patchwork_timeseries_table(ts_list, &defaults, &exceptions).unwrap();

        assert_eq!(output.get(&label), Some(expected_output).as_ref());
    }

    #[test]
    fn test_patch_default() {
        let t0: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();

        let expected_output = vec![
            (timerange::open(Some(t0), Some(t1)), 2),
            (timerange::open(Some(t1), Some(t2)), 1), // the exception should be patched in
            (timerange::open(Some(t2), None), 2),
        ];

        let timerange = timerange::open(Some(t0), None);

        let exception = MessagePriority::new(1, timerange::open(Some(t1), Some(t2)));

        let mut output = patch_default(timerange, 2, Some(&exception)).unwrap();
        output.sort_by_key(|item| item.0.from);

        assert_eq!(output, expected_output);
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
                    timerange::Open {
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
                        timerange::Open {
                            from: Some(t1),
                            to: Some(t2),
                        },
                        timerange::Open {
                            from: Some(t3),
                            to: Some(t4),
                        },
                    ],
                    timerange::Open {
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
                    vec![timerange::Open {
                        from: Some(t3),
                        to: Some(t4),
                    }],
                    timerange::Open {
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
                    vec![timerange::Open {
                        from: Some(t1),
                        to: Some(t2),
                    }],
                    timerange::Open {
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
                timerange::Open {
                    from: hole_ft,
                    to: hole_tt,
                },
                timerange::Open {
                    from: cand_ft,
                    to: cand_tt,
                },
            );
            assert_eq!(output, expected_output, "{message}");
        }
    }
}
