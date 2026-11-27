use chrono::{DateTime, Utc};

use crate::{
    Error,
    patchwork::{Patch, Truncated},
};
use util::{
    PermitId, TsId,
    stinfofacade::level::{LevelTable, param_get_level},
    timerange::closed,
};

#[derive(Debug, Clone, PartialEq)]
pub struct CalculationPatch {
    pub from: DateTime<Utc>,
    pub tsids: Option<Vec<TsId>>,
}

impl CalculationPatch {
    pub fn new(from: DateTime<Utc>, tsids: Option<Vec<TsId>>) -> Self {
        Self { from, tsids }
    }
}

impl From<Patch> for CalculationPatch {
    fn from(patch: Patch) -> Self {
        Self {
            from: patch.from,
            tsids: patch.source.map(|patch| vec![patch.tsid]),
        }
    }
}

pub type CalculationPatchwork = Vec<CalculationPatch>;

#[derive(Debug, Clone, PartialEq)]
pub struct CalculationTruncated {
    pub patchwork: CalculationPatchwork,
    pub to: DateTime<Utc>,
    pub permits: Vec<PermitId>,
}

// TODO: is this right?
fn merge_once(
    left: CalculationTruncated,
    (permit, right): (PermitId, Truncated),
) -> CalculationTruncated {
    let mut merged = CalculationPatchwork::new();

    // if right starts before left, there should be a hole from the start of
    // right until the start of left, that the loop below doesn't account for
    if let Some(r_first) = right.patchwork.first()
        && left
            .patchwork
            .first()
            .is_none_or(|l_first| r_first.from < l_first.from)
    {
        merged.push(CalculationPatch::new(r_first.from, None));
    }

    for (l_i, l_patch) in left.patchwork.iter().enumerate() {
        let Some(l_tsids) = l_patch.tsids.as_ref() else {
            // hole can't be merged
            merged.push(l_patch.clone());
            continue;
        };

        let l_range = closed(
            l_patch.from,
            left.patchwork
                .get(l_i + 1)
                .map(|next| next.from)
                .unwrap_or(left.to),
        );

        let mut patched_to = l_patch.from;
        // TODO: reuse?
        let mut patches = Vec::new();
        for (r_i, r_patch) in right.patchwork.iter().enumerate() {
            let r_range = closed(
                r_patch.from,
                right
                    .patchwork
                    .get(r_i + 1)
                    .map(|next| next.from)
                    .unwrap_or(right.to),
            );
            let Some(overlap) = l_range.overlap(r_range) else {
                continue;
            };

            let tsids = if let Some(r_source) = r_patch.source {
                let mut tsids = l_tsids.clone();
                tsids.push(r_source.tsid);
                Some(tsids)
            } else {
                None
            };

            patches.push(CalculationPatch::new(overlap.from, tsids));
            patched_to = overlap.to;
        }
        if patches
            .first()
            .is_none_or(|first| first.from != l_range.from)
        {
            merged.push(CalculationPatch::new(l_range.from, None));
        }
        for patch in patches {
            merged.push(patch);
        }
        if patched_to != l_range.to {
            merged.push(CalculationPatch::new(patched_to, None));
        }
    }

    // merge neighboring holes by filtering out holes that come immediately
    // after other holes
    let mut filtered = Vec::new();
    let mut in_hole = false;
    for patch in merged {
        if patch.tsids.is_some() {
            filtered.push(patch);
            in_hole = false;
        } else {
            if !in_hole {
                filtered.push(patch);
                in_hole = true;
            }
        }
    }

    let mut permits = left.permits;
    permits.push(permit);

    // TODO: make sure we can't get patches with `from` after `to`
    CalculationTruncated {
        patchwork: filtered,
        to: left.to.min(right.to),
        permits,
    }
}

/// Panics:
/// - if fed an empty `Vec` for `patchworks`
pub fn merge_patches(patchworks: Vec<(PermitId, Truncated)>) -> CalculationTruncated {
    let mut patchworks = patchworks.into_iter();
    let (permit, first) = patchworks.next().unwrap();
    let acc = CalculationTruncated {
        patchwork: first.patchwork.into_iter().map(|p| p.into()).collect(),
        to: first.to,
        permits: vec![permit],
    };
    patchworks.fold(acc, merge_once)
}

pub fn default_sensor_from_api_param(sensor: Option<i32>) -> i32 {
    sensor.unwrap_or(0)
}

pub fn default_level_from_api_param(
    level_table: LevelTable,
    level: Option<i32>,
    param_id: i32,
) -> Result<Option<i32>, Error> {
    match level {
        None => Ok(param_get_level(level_table, param_id, 0)?),
        x => Ok(x),
    }
}

#[cfg(test)]
mod test {
    use crate::patchwork::{Patch, SourceTs};
    use chrono::{Duration, TimeZone, Utc};

    use super::*;

    #[test]
    fn test_merge() {
        let t0 = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let t1 = t0 + Duration::days(10);
        let t2 = t0 + Duration::days(15);
        let t3 = t0 + Duration::days(20);
        let t4 = t0 + Duration::days(30);

        let ts1 = Some(SourceTs::new(1, None, 101));
        let ts2 = Some(SourceTs::new(2, None, 102));
        let ts3 = Some(SourceTs::new(3, None, 103));
        let ts4 = Some(SourceTs::new(4, None, 104));

        // TODO: needs more test cases
        let cases = [
            (
                "No overlap",
                (vec![(t0, ts1), (t1, ts2), (t2, None)], t4),
                (vec![(t2, ts3), (t3, ts4)], t4),
                (vec![(t0, None)], t4),
            ),
            (
                "Matching fromto",
                (vec![(t0, ts1), (t1, ts2)], t4),
                (vec![(t0, ts3), (t1, ts4)], t4),
                (vec![(t0, Some(vec![1, 3])), (t1, Some(vec![2, 4]))], t4),
            ),
            (
                "single left",
                (vec![(t0, ts1)], t4),
                (vec![(t0, ts3), (t1, ts4)], t4),
                (vec![(t0, Some(vec![1, 3])), (t1, Some(vec![1, 4]))], t4),
            ),
            (
                "single right",
                (vec![(t0, ts1), (t1, ts2)], t4),
                (vec![(t0, ts3)], t4),
                (vec![(t0, Some(vec![1, 3])), (t1, Some(vec![2, 3]))], t4),
            ),
            (
                "staggered middle point",
                (vec![(t0, ts1), (t1, ts2)], t4),
                (vec![(t0, ts3), (t3, ts4)], t4),
                (
                    vec![
                        (t0, Some(vec![1, 3])),
                        (t1, Some(vec![2, 3])),
                        (t3, Some(vec![2, 4])),
                    ],
                    t4,
                ),
            ),
            (
                "staggered start",
                (vec![(t1, ts1), (t3, ts2)], t4),
                (vec![(t0, ts3), (t2, ts4)], t4),
                (
                    vec![
                        (t0, None),
                        (t1, Some(vec![1, 3])),
                        (t2, Some(vec![1, 4])),
                        (t3, Some(vec![2, 4])),
                    ],
                    t4,
                ),
            ),
            (
                "staggered end",
                (vec![(t1, ts1), (t3, ts2), (t4, None)], t4),
                (vec![(t0, ts3), (t1, ts4), (t2, None)], t4),
                (vec![(t0, None), (t1, Some(vec![1, 4])), (t2, None)], t4),
            ),
        ];

        for (message, left, right, expected) in cases {
            let left = Truncated {
                patchwork: left
                    .0
                    .into_iter()
                    .map(|(from, src)| Patch::new(from, src))
                    .collect(),
                to: left.1,
            };
            let right = Truncated {
                patchwork: right
                    .0
                    .into_iter()
                    .map(|(from, src)| Patch::new(from, src))
                    .collect(),
                to: right.1,
            };
            let expected = CalculationTruncated {
                patchwork: expected
                    .0
                    .into_iter()
                    .map(|(from, tsids)| CalculationPatch::new(from, tsids))
                    .collect(),
                to: expected.1,
                permits: vec![1, 1],
            };
            let merged = merge_patches(vec![(1, left), (1, right)]);
            assert_eq!(merged, expected, "{}", message);
        }
    }
}
