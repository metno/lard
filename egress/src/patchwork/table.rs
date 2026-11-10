use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::{
    Error,
    patchwork::{Label, Patch, Patchwork, SourceTs},
};
use ::util::{
    DbPools, PooledPgConn,
    stinfofacade::{
        self,
        message_priority::{DefaultTable, ExceptionTable, MessagePriority},
    },
    timerange::{self, HalfOpen, half_open},
};

/// Lookup table for instructions to make a patchwork timeseries.
///
/// As these instructions are a little expensive to make, involving metadata
/// from multiple sources, we compute and cache them ahead of time. Used by
/// [`super::get_patchwork`] to construct the actual patchwork timeseries
pub type Table = HashMap<Label, Patchwork>;

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

// TODO: should this be in util? probably also it's needed in stinfofacade?
type Priority = i32;

/// Get all the timeseries with MET labels from LARD
/// including their from / to times
pub async fn fetch_timeseries_list_from_database(
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(Label, HalfOpen, SourceTs)>, Error> {
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

    let mut data = Vec::with_capacity(data_results.len());
    for row in data_results {
        data.push((
            Label::new(row.get(1), row.get(2), row.get(4), row.get(5)),
            half_open(row.get(6), row.get(7)),
            SourceTs::new(row.get(0), row.get(8), row.get(9), row.get(3)),
        ));
    }

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
    timerange: HalfOpen,
    default: Option<&MessagePriority>,
    exception: Option<&MessagePriority>,
) -> Option<Vec<(timerange::HalfOpen, Priority)>> {
    let default = default?;

    let times = timerange.overlap(default.timerange)?;

    // this patches the exceptions (often station specific) over the defaults where applicable
    let out = patch_default(times, default.priority, exception)
        .unwrap_or(vec![(times, default.priority)]);

    Some(out)
}

fn patch_default(
    default_timerange: HalfOpen,
    priority: i32,
    exception: Option<&MessagePriority>,
) -> Option<Vec<(timerange::HalfOpen, i32)>> {
    let ex = exception?;
    let overlap = default_timerange.overlap(ex.timerange)?;

    let mut ranges = vec![(overlap, ex.priority)];
    if overlap.from != default_timerange.from {
        ranges.push((
            half_open(default_timerange.from, Some(overlap.from)),
            priority,
        ));
    }
    if overlap.to != default_timerange.to {
        ranges.push((
            half_open(overlap.to.unwrap(), default_timerange.to),
            priority,
        ));
    }

    Some(ranges)
}

struct Fill {
    index: usize,
    patches: Vec<Patch>,
}

fn fill_hole(hole: HalfOpen, cand: HalfOpen, index: usize, source_ts: SourceTs) -> Option<Fill> {
    let overlap = hole.overlap(cand)?;

    let mut patches = Vec::new();
    if overlap.from != hole.from {
        patches.push(Patch::new(hole.from, None));
    }
    patches.push(Patch::new(overlap.from, Some(source_ts)));
    if overlap.to != hole.to {
        patches.push(Patch::new(overlap.to.unwrap(), None));
    }

    Some(Fill { index, patches })
}

fn fill_holes(
    candidates: Vec<(Priority, HalfOpen, SourceTs)>,
    overall_from: DateTime<Utc>,
) -> Patchwork {
    let mut patches = vec![Patch::new(overall_from, None)];

    // need to make sure temp sorted list is sorted by priority first
    for (_, cand_range, source_ts) in candidates {
        let mut fills: Vec<Fill> = Vec::new();

        for (i, hole) in patches
            .iter()
            .enumerate()
            .filter(|(_, patch)| patch.source.is_none())
        {
            let hole_range = half_open(hole.from, patches.get(i + 1).map(|next| next.from));

            if let Some(fill) = fill_hole(hole_range, cand_range, i, source_ts) {
                fills.push(fill);
            }
        }

        let mut offset = 0;
        for fill in fills {
            let i = fill.index + offset;
            let fill_len = fill.patches.len();

            patches.splice(i..i + 1, fill.patches);
            // need to offset next inserts, as the spot they want to insert into will be moved by
            // this splice. We need `-1` because we're also removing an element from `patches` (the
            // original hole)
            offset += fill_len - 1;
        }
    }

    patches
}

/// This function actually creates the patchwork list that will be used to find one timeseries
/// when not relying on seperating them by typeid
pub fn create_patchwork_timeseries_table(
    db_ts_list: Vec<(Label, timerange::HalfOpen, SourceTs)>,
    default_table: &DefaultTable,
    exception_table: &ExceptionTable,
) -> Result<Table, Error> {
    // create a list of timeseries with the patchwork label, which maps to a list of
    // typeid, tsid, and the from/to times of that timeseries
    let mut flatten_data = HashMap::new();
    for (label, timerange, source) in db_ts_list {
        flatten_data
            .entry(label)
            .or_insert_with(Vec::new)
            .push((timerange, source));
    }

    // declare the structure we will keep the patchwork list in
    let mut patchwork = HashMap::new();

    // loop over all the timeseries in groupings based on the patchwork label
    // this means that all the timeseries have the same label, but have different typeids
    // this is the essence of the patchwork, as it needs to decide which are prioritized for
    // different time ranges.
    for (label, pre_candidates) in flatten_data {
        if pre_candidates.is_empty() {
            continue;
        }

        // create a temporary structure for ordering / sorting
        let mut candidates: Vec<(Priority, HalfOpen, SourceTs)> = Vec::new();

        // NOTE: param 0 is used to represent all parameters rather than listing them all, therefore
        // it is the fall back in this code
        // NOTE: sensor 0 does not represent all sensors, those need to be listed specifically
        // NOTE: level 0 has been converted in the the import for the exeption table to the default
        // level for that paramid. Individual levels also need to be listed specifically.

        // make this into the patchwork list using the cached maps from stinfosys
        for (source_range, source) in pre_candidates {
            // then actually have to prioritize, using the default and exception tables
            let default = default_table.get(&(source.typeid, label.param_id));
            // if there's not a param specific default, the default for param 0 applies to all params on that station,
            // so we check that as a backup
            let default_0 = default_table.get(&(source.typeid, 0));
            let mut exception = exception_table.get(&(label, source.typeid));
            let exception_0 = exception_table.get(&(
                Label::new(label.station_id, 0, label.level, label.sensor),
                source.typeid,
            ));
            // fallback to the param 0 is default for all if nothing specific is found from the exception table
            if exception.is_none() && exception_0.is_some() {
                exception = exception_0;
            }

            // TODO: currently ignoring obspgm time ranges, should we also use those like in ODA or is this good enough?
            if let Some(tss) = process_priorities(source_range, default, exception) {
                for (range, priority) in tss {
                    candidates.push((priority, range, source))
                }
            } else if let Some(tss) = process_priorities(source_range, default_0, exception) {
                for (range, priority) in tss {
                    candidates.push((priority, range, source))
                }
            }
        }

        if candidates.is_empty() {
            // should this happen?
            warn!("no priorities found for this label {:?}", label);
            continue;
        }
        let first_time = candidates.iter().map(|item| item.1.from).min().unwrap();

        // sort the list by priority
        candidates.sort_by_key(|item| item.0);

        // loop through timeseries in priority order to fill any remaining gaps in the target timerange
        // until we either fill everything, or run out of timeseries
        patchwork.insert(label, fill_holes(candidates, first_time));
    }
    Ok(patchwork)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use pg_interval::Interval;

    pub fn mock_default_table() -> DefaultTable {
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(1500, 1, 1, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2006, 1, 1, 0, 0, 0).unwrap();

        HashMap::from([
            ((501, 0), MessagePriority::new(11110, half_open(t2, None))),
            ((330, 0), MessagePriority::new(11510, half_open(t2, None))),
            ((308, 0), MessagePriority::new(14110, half_open(t2, None))),
            ((316, 0), MessagePriority::new(14510, half_open(t2, None))),
            ((3, 0), MessagePriority::new(11710, half_open(t2, None))),
            (
                (1001, 0),
                MessagePriority::new(11040, half_open(t1, Some(t2))),
            ),
            (
                (1002, 0),
                MessagePriority::new(14040, half_open(t1, Some(t2))),
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
                MessagePriority::new(1060, half_open(t6, None)), // stinfo: 2021-09-07 06:00:00 |
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 330),
                MessagePriority::new(99080, half_open(t1, Some(t5))), // stinfo: 1500-01-01 00:00:00 | 2017-08-24 06:00:00
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 3),
                MessagePriority::new(99090, half_open(t1, Some(t3))), // stinfo: 1500-01-01 00:00:00 | 2007-09-14 06:00:00
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 308),
                MessagePriority::new(1080, half_open(t3, Some(t4))), // stinfo: 2007-09-14 06:00:00 | 2014-01-13 06:00:00
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 316),
                MessagePriority::new(1070, half_open(t4, Some(t6))), // stinfo: 2014-01-13 06:00:00 | 2021-09-07 06:00:00
            ),
            (
                (Label::new(99910, 112, Some(0), Some(0)), 1002),
                MessagePriority::new(1100, half_open(t1, Some(t2))), // stinfo: 1500-01-01 00:00:00 | 2006-01-01 06:00:00
            ),
        ])
    }

    pub fn mock_ts_list() -> Vec<(Label, HalfOpen, SourceTs)> {
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
                Label::new(99910, 112, Some(0), Some(0)),
                half_open(t1, None),
                SourceTs::new(491179, 1, Some(Interval::new(0, 0, 3600000000)), 501),
            ),
            (
                Label::new(99910, 112, Some(0), Some(0)),
                half_open(t2, Some(t3)),
                SourceTs::new(477764, 1, Some(Interval::new(0, 0, 3600000000)), 330),
            ),
            (
                Label::new(99910, 112, Some(0), Some(0)),
                half_open(t4, Some(t5)),
                SourceTs::new(447225, 1, Some(Interval::new(0, 0, 3600000000)), 3),
            ),
            (
                Label::new(99910, 112, Some(0), Some(0)),
                half_open(t6, Some(t7)),
                SourceTs::new(34452, 1, Some(Interval::new(0, 0, 3600000000)), 1001),
            ),
            (
                Label::new(99910, 112, Some(0), Some(0)),
                half_open(t6, Some(t7)),
                SourceTs::new(70177, 1, Some(Interval::new(0, 0, 3600000000)), 1002),
            ),
            (
                Label::new(99910, 112, Some(0), Some(0)),
                half_open(t8, None),
                SourceTs::new(477763, 1, Some(Interval::new(0, 0, 3600000000)), 316),
            ),
            (
                Label::new(99910, 112, Some(0), Some(0)),
                half_open(t9, Some(t8)),
                SourceTs::new(447224, 1, Some(Interval::new(0, 0, 3600000000)), 308),
            ),
        ]
    }

    pub fn mock_ts_not_in_priorities_list() -> Vec<(Label, HalfOpen, SourceTs)> {
        let t1: DateTime<Utc> = "2021-09-06 13:00:00 +0000".to_string().parse().unwrap();
        // the type id does not exist...
        vec![(
            Label::new(9999, 112, Some(0), Some(0)),
            half_open(t1, None),
            SourceTs::new(123456, 1, Some(Interval::new(0, 0, 3600000000)), 1234),
        )]
    }

    #[test]
    fn test_patchwork_timeseries_99910() {
        let t0: DateTime<Utc> = "1994-09-04 11:00:00 +0000".to_string().parse().unwrap();
        let t1: DateTime<Utc> = "2005-12-31 23:00:00 +0000".to_string().parse().unwrap();
        let t2: DateTime<Utc> = "2007-09-14 06:00:00 +0000".to_string().parse().unwrap();
        let t3: DateTime<Utc> = "2014-01-13 06:00:00 +0000".to_string().parse().unwrap();
        let t4: DateTime<Utc> = "2021-09-07 06:00:00 +0000".to_string().parse().unwrap();
        let timeres = Some(Interval::new(0, 0, 3600000000));
        let cases = [(
            // real case, uses station specific exceptions
            Label::new(99910, 112, Some(0), Some(0)),
            vec![
                Patch::new(t0, Some(SourceTs::new(70177, 1, timeres, 1002))),
                Patch::new(t1, None),
                Patch::new(t2, Some(SourceTs::new(447224, 1, timeres, 308))),
                Patch::new(t3, Some(SourceTs::new(477763, 1, timeres, 316))),
                Patch::new(t4, Some(SourceTs::new(491179, 1, timeres, 501))),
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
        let timeres = Some(Interval::new(0, 0, 3600000000));

        let label = Label::new(1, 1, Some(0), Some(0));
        let expected_output = vec![
            Patch::new(t0, Some(SourceTs::new(1, 1, timeres, 1))),
            Patch::new(t1, Some(SourceTs::new(2, 1, timeres, 2))),
            Patch::new(t2, Some(SourceTs::new(1, 1, timeres, 1))),
        ];

        let ts_list = vec![
            (
                Label::new(1, 1, Some(0), Some(0)),
                half_open(t0, None),
                SourceTs::new(1, 1, timeres, 1),
            ),
            (
                Label::new(1, 1, Some(0), Some(0)),
                half_open(t1, None),
                SourceTs::new(2, 1, timeres, 2),
            ),
        ];

        let defaults = HashMap::from([
            ((1, 0), MessagePriority::new(2, half_open(t0, None))),
            ((2, 0), MessagePriority::new(3, half_open(t0, None))),
        ]);

        let exceptions: ExceptionTable = HashMap::from([(
            (Label::new(1, 1, Some(0), Some(0)), 2),
            MessagePriority::new(1, half_open(t1, Some(t2))),
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
        let timeres = Some(Interval::new(0, 0, 3600000000));

        let label = Label::new(1, 1, Some(0), Some(0));
        let expected_output = vec![
            Patch::new(t0, Some(SourceTs::new(1, 1, timeres, 1))),
            Patch::new(t2, Some(SourceTs::new(2, 1, timeres, 2))),
        ];
        let ts_list = vec![
            (
                label,
                half_open(t0, Some(t2)),
                SourceTs::new(1, 1, timeres, 1),
            ),
            (label, half_open(t1, None), SourceTs::new(2, 1, timeres, 2)),
            (label, half_open(t0, None), SourceTs::new(2, 1, timeres, 2)),
        ];

        let defaults = HashMap::from([
            ((1, 0), MessagePriority::new(1, half_open(t0, None))),
            ((2, 0), MessagePriority::new(2, half_open(t0, None))),
            ((3, 0), MessagePriority::new(3, half_open(t0, None))),
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
            (half_open(t0, Some(t1)), 2),
            (half_open(t1, Some(t2)), 1), // the exception should be patched in
            (half_open(t2, None), 2),
        ];

        let timerange = half_open(t0, None);

        let exception = MessagePriority::new(1, half_open(t1, Some(t2)));

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
        let s = SourceTs::new(1, 1, Some(Interval::new(0, 0, 3600000000)), 1);

        // returns Option<(Vec<Timerange>, Timerange)>

        let cases = [
            (
                "total overlap",
                // cand: |-----|
                // hole:   |-|
                // out:    |-|
                // time  1 2 3 4
                (t1, Some(t4)),
                (t2, Some(t3)),
                Some(vec![(t2, Some(s))]),
            ),
            (
                "hole fully contains cand",
                // cand:   |-|
                // hole: |-----|
                // out:  |*|-|*|
                (t2, Some(t3)),
                (t1, Some(t4)),
                Some(vec![(t1, None), (t2, Some(s)), (t3, None)]),
            ),
            (
                "left overlap",
                // cand: |---|
                // hole:   |---|
                // out:    |-|*|
                (t1, Some(t3)),
                (t2, Some(t4)),
                Some(vec![(t2, Some(s)), (t3, None)]),
            ),
            (
                "right overlap",
                // cand:   |---|
                // hole: |---|
                // out:  |*|-|
                (t2, Some(t4)),
                (t1, Some(t3)),
                Some(vec![(t1, None), (t2, Some(s))]),
            ),
            (
                "no overlap right",
                // cand:     |-|
                // hole: |-|
                // out:
                (t3, Some(t4)),
                (t1, Some(t2)),
                None,
            ),
            (
                "no overlap left",
                // cand: |-|
                // hole:     |-|
                // out:
                (t1, Some(t2)),
                (t3, Some(t4)),
                None,
            ),
            (
                "touching but no overlap right",
                // cand:   |---|
                // hole: |-|
                // out:
                (t2, Some(t4)),
                (t1, Some(t2)),
                None,
            ),
            (
                "touching but no overlap left",
                // cand: |---|
                // hole:     |-|
                // out:
                (t1, Some(t3)),
                (t3, Some(t4)),
                None,
            ),
        ];

        for (message, cand, hole, expected) in cases {
            let output = fill_hole(half_open(hole.0, hole.1), half_open(cand.0, cand.1), 1, s);
            match (output, expected) {
                (Some(output), Some(expected)) => {
                    let expected: Vec<Patch> = expected
                        .into_iter()
                        .map(|(from, patch)| Patch::new(from, patch))
                        .collect();
                    assert_eq!(output.patches, expected, "{message}");
                }
                (None, None) => (),
                _ => panic!("{message}"),
            }
        }
    }
}
