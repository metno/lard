use crate::patchwork::{Error, PatchworkTables};
use crate::patchwork::{OpenTimerange, PermitId, TsId, TypeId, fill_hole, fill_holes};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::warn;
use util::stinfofacade::from_to_time::MAX_MIN_TIMESERIES_LEGACY_DATA_QUERY;
use util::stinfofacade::message_priority::{
    DefaultTable, ExceptionTable, MessagePriority, fetch_message_priority,
};
use util::{PatchworkLabel, PooledPgConn};

type PriorityList = Vec<(
    OpenTimerange,
    i32,
    TypeId,
    TsId,
    PermitId,
    PrioritySource,
    OpenTimerange, // obs coverage
)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum PrioritySource {
    /// From `default_table`, keyed by this station's own `param_id`.
    Default,
    /// From `default_table`, via the `param_id = 0` fallback entry.
    DefaultFallback,
    /// From a station-specific `exception_table` entry.
    Exception,
}

/// ts range, derived priority/source, selected patches,
/// and observed data coverage.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PatchworkViewRow {
    pub ts_range: OpenTimerange, // obs_pgm fromtime/totime
    pub priority: i32,
    pub priority_source: PrioritySource,
    pub type_id: TypeId,
    pub tsid: TsId,
    pub permit: PermitId,
    pub selected: Vec<(DateTime<Utc>, Option<DateTime<Utc>>)>,
    pub obs_coverage: OpenTimerange, // sql min/max(obstime)
}
pub(crate) type PatchworkView = Vec<PatchworkViewRow>;

/// Viewer-local replica of `mod.rs`'s `process_priorities`, additionally
/// returning the `PrioritySource` for each `(range, priority)` segment.
fn process_priorities_with_source(
    timerange: OpenTimerange,
    default: Option<&MessagePriority>,
    exception: Option<&MessagePriority>,
    default_source: PrioritySource,
) -> Option<Vec<(OpenTimerange, i32, PrioritySource)>> {
    let default = default?;

    let times = timerange.overlap(OpenTimerange {
        from: default.timerange.from,
        to: default.timerange.to,
    })?;

    // this patches the exceptions (often station specific) over the defaults where applicable
    let out = patch_default_with_source(times, default.priority, exception, default_source)
        .unwrap_or(vec![(times, default.priority, default_source)]);

    Some(out)
}

/// Viewer-local replica of `mod.rs`'s `patch_default`, additionally tagging
/// each returned segment with its `PrioritySource`.
fn patch_default_with_source(
    timerange: OpenTimerange,
    priority: i32,
    exception: Option<&MessagePriority>,
    default_source: PrioritySource,
) -> Option<Vec<(OpenTimerange, i32, PrioritySource)>> {
    let ex = exception?;

    // NOTE: here `fill_hole` is used to fill the timerange covered by the default prioriry
    // with the exceptions (ie, deleting parts of the default where necessary)
    let (t_list, t_ex) = fill_hole(
        timerange,
        OpenTimerange {
            from: ex.timerange.from,
            to: ex.timerange.to,
        },
    )?;

    let mut ranges = vec![(t_ex, ex.priority, PrioritySource::Exception)];

    for t_l in t_list {
        ranges.push((t_l, priority, default_source));
    }

    Some(ranges)
}

/// Entry point for endpoint: fetches Patches for the given station/
/// param and derives real priorities into a `PatchworkView`.
pub(crate) async fn view_patchwork(
    label: PatchworkLabel,
    patchwork_tables: PatchworkTables,
    conn: &PooledPgConn<'_>,
    stinfo_conn_string: Option<&str>,
) -> Result<PatchworkView, Error> {
    let (default_table, exception_table) = fetch_message_priority(stinfo_conn_string).await?;

    let priority_list =
        fetch_timeseries_with_priority_and_coverage(label, conn, &default_table, &exception_table)
            .await?;

    // check the contents of the open table for this patchwork label
    let t = patchwork_tables.open.read()?;
    let _patchwork_table_contents_for_label = t.get(&label);

    build_patchwork_view(priority_list)
}

async fn fetch_timeseries_with_priority_and_coverage(
    label: PatchworkLabel,
    conn: &PooledPgConn<'_>,
    default_table: &DefaultTable,
    exception_table: &ExceptionTable,
) -> Result<PriorityList, Error> {
    // NOTE: not implemented for restricted timeseries
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
                t.permit \
            FROM labels.met l \
            JOIN timeseries t \
                ON t.id = l.timeseries \
            WHERE l.param_id = $1 AND l.station_id = $2
            AND l.lvl IS NOT DISTINCT FROM $3 AND l.sensor IS NOT DISTINCT FROM $4",
            &[
                &label.param_id,
                &label.station_id,
                &label.level,
                &label.sensor,
            ],
        )
        .await?;

    let mut data: PriorityList = Vec::with_capacity(data_results.len());

    for row in data_results {
        let tsid: TsId = row.get("timeseries");
        let type_id: TypeId = row.get("type_id");
        let permit: PermitId = row.get("permit");
        let fromto = OpenTimerange {
            from: row.get("fromtime"),
            to: row.get("totime"),
        };

        // get observation range
        // NOTE: only legacy/scalar data is considered for now, not non scalar data
        let min_max_result = conn
            .query_one(MAX_MIN_TIMESERIES_LEGACY_DATA_QUERY, &[&tsid])
            .await?;
        let obs_coverage = OpenTimerange {
            from: min_max_result.get(0),
            to: min_max_result.get(1),
        };

        // get priority value and source
        let default = default_table.get(&(type_id, label.param_id));
        let default_0 = default_table.get(&(type_id, 0));
        let exception = exception_table.get(&(label, type_id));

        let priorities =
            process_priorities_with_source(fromto, default, exception, PrioritySource::Default)
                .or_else(|| {
                    process_priorities_with_source(
                        fromto,
                        default_0,
                        exception,
                        PrioritySource::DefaultFallback,
                    )
                });

        match priorities {
            Some(ranges) => {
                for (range, priority, source) in ranges {
                    data.push((range, priority, type_id, tsid, permit, source, obs_coverage));
                }
            }
            None => {
                warn!(
                    "no message priority found for tsid {tsid} (type_id {type_id}, param_id {:?})",
                    label.param_id
                );
            }
        }
    }

    Ok(data)
}

fn build_patchwork_view(priority_list: PriorityList) -> Result<PatchworkView, Error> {
    let mut priority_list = priority_list;
    priority_list.sort_by_key(|item| item.1);

    // `fill_holes` only needs the source range/tsid/permit, so drop `priority_source`/`obs_coverage`.
    let fill_input = priority_list
        .iter()
        .map(
            |&(range, priority, type_id, tsid, permit, _source, _obs_coverage)| {
                (range, priority, type_id, tsid, permit)
            },
        )
        .collect();

    let patches = fill_holes(
        fill_input,
        OpenTimerange {
            from: None,
            to: None,
        },
    );

    let mut view: PatchworkView = Vec::new();
    for &(ts_range, priority, type_id, tsid, permit, priority_source, obs_coverage) in
        priority_list.iter()
    {
        view.push(PatchworkViewRow {
            ts_range,
            priority,
            priority_source,
            type_id,
            tsid,
            permit,
            selected: vec![],
            obs_coverage,
        })
    }

    for patch in patches {
        for item in view.iter_mut() {
            if patch.tsid == item.tsid {
                item.selected.push((patch.from, patch.to));
            }
        }
    }

    Ok(view)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // Fixed timestamps shared by all tests below.
    fn t1() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap()
    }
    fn t2() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 2, 0, 0, 0).unwrap()
    }
    fn t3() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 3, 0, 0, 0).unwrap()
    }
    fn t4() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 4, 0, 0, 0).unwrap()
    }

    fn range(from: DateTime<Utc>, to: DateTime<Utc>) -> OpenTimerange {
        OpenTimerange {
            from: Some(from),
            to: Some(to),
        }
    }

    // looks up a row by ts; panics if missing, since every test expects
    // its input candidates to show up as a row
    fn row_for(view: &PatchworkView, tsid: TsId) -> &PatchworkViewRow {
        view.iter()
            .find(|item| item.tsid == tsid)
            .unwrap_or_else(|| panic!("no row found for tsid {tsid}"))
    }

    #[test]
    fn lower_priority_wins_full_overlap() {
        // two ts with same range, two candidates; the ts with priority 10
        // should take over the whole range
        // ts1 (pri 10): |----->
        // ts2 (pri 20): |----->
        //               t1    t3
        let priority_rows: PriorityList = vec![
            (
                range(t1(), t3()),
                10,
                330,
                1,
                1,
                PrioritySource::Default,
                OpenTimerange::default(),
            ),
            (
                range(t1(), t3()),
                20,
                501,
                2,
                1,
                PrioritySource::Default,
                OpenTimerange::default(),
            ),
        ];

        let view = build_patchwork_view(priority_rows).unwrap();

        assert_eq!(row_for(&view, 1).selected, vec![(t1(), Some(t3()))]);
        assert_eq!(row_for(&view, 2).selected, Vec::new());
    }

    #[test]
    fn lower_priority_candidate_fills_later_uncovered_portion() {
        // partial-overlap case
        // ts1 (pri 10): |----->
        // ts2 (pri 20):    |------->
        //               t1 t2  t3  t4
        let priority_rows: PriorityList = vec![
            (
                range(t1(), t3()),
                10,
                330,
                1,
                1,
                PrioritySource::Default,
                OpenTimerange::default(),
            ),
            (
                range(t2(), t4()),
                20,
                501,
                2,
                1,
                PrioritySource::Default,
                OpenTimerange::default(),
            ),
        ];

        let view = build_patchwork_view(priority_rows).unwrap();

        assert_eq!(row_for(&view, 1).selected, vec![(t1(), Some(t3()))]);
        assert_eq!(row_for(&view, 2).selected, vec![(t3(), Some(t4()))]);
    }

    #[test]
    fn disjoint_candidates_are_both_fully_selected() {
        // ts1: |--->
        // ts2:        |--->
        //      t1  t2 t3  t4
        let priority_rows: PriorityList = vec![
            (
                range(t1(), t2()),
                10,
                330,
                1,
                1,
                PrioritySource::Default,
                OpenTimerange::default(),
            ),
            (
                range(t3(), t4()),
                20,
                501,
                2,
                1,
                PrioritySource::Default,
                OpenTimerange::default(),
            ),
        ];

        let view = build_patchwork_view(priority_rows).unwrap();

        assert_eq!(row_for(&view, 1).selected, vec![(t1(), Some(t2()))]);
        assert_eq!(row_for(&view, 2).selected, vec![(t3(), Some(t4()))]);
    }

    #[test]
    fn prioritized_candidate_with_no_observed_data_has_empty_coverage() {
        // patch is fully selected but obs_coverage is `OpenTimerange::default()`
        // for its ts (e.g. no actual observations)
        // ts1: |----->
        //      t1    t3   (no observations)
        let priority_rows: PriorityList = vec![(
            range(t1(), t3()),
            10,
            330,
            1,
            1,
            PrioritySource::Default,
            OpenTimerange::default(),
        )];

        let view = build_patchwork_view(priority_rows).unwrap();

        let row = row_for(&view, 1);
        assert_eq!(row.selected, vec![(t1(), Some(t3()))]);
        assert_eq!(row.obs_coverage, OpenTimerange::default());
    }
}
