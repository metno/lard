use crate::patchwork::Patch;
use util::{ClosedTimerange, TsId};

#[derive(Clone, Default, PartialEq, Debug)]
pub struct CalculationPatch {
    pub tsids: Vec<TsId>,
    pub timerange: ClosedTimerange,
}

impl From<Patch> for CalculationPatch {
    fn from(patch: Patch) -> Self {
        CalculationPatch {
            tsids: vec![patch.tsid],
            timerange: ClosedTimerange {
                from: patch.from,
                to: patch.to,
            },
        }
    }
}

fn merge_once(left: Vec<CalculationPatch>, right: Vec<Patch>) -> Vec<CalculationPatch> {
    left.iter()
        .flat_map(|l_patch| {
            right.iter().filter_map(|r_patch| {
                let timerange = l_patch.timerange.overlap(ClosedTimerange {
                    from: r_patch.from,
                    to: r_patch.to,
                })?;
                let mut tsids = l_patch.tsids.clone();
                tsids.push(r_patch.tsid);
                Some(CalculationPatch { tsids, timerange })
            })
        })
        .collect()
}

pub fn merge_patches(patchsets: Vec<Vec<Patch>>) -> Vec<CalculationPatch> {
    let mut patches = patchsets.into_iter();
    let acc: Vec<CalculationPatch> = patches
        .next()
        .unwrap_or_default()
        .into_iter()
        .map(|p| p.into())
        .collect();
    patches.fold(acc, merge_once)
}

#[cfg(test)]
mod test {
    use crate::patchwork::Patch;
    use chrono::{Duration, TimeZone, Utc};

    use super::*;

    #[test]
    fn test_merge() {
        struct Case<'a> {
            title: &'a str,
            left: Vec<Patch>,
            right: Vec<Patch>,
            expected: Vec<CalculationPatch>,
        }

        let from = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let first = from + Duration::days(10);
        let second = from + Duration::days(15);
        let third = from + Duration::days(20);
        let to = from + Duration::days(30);

        let cases = [
            Case {
                title: "No overlap",
                left: vec![
                    Patch {
                        tsid: 1,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 2,
                        from: first,
                        to: second,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from: second,
                        to: third,
                    },
                    Patch {
                        tsid: 4,
                        from: third,
                        to,
                    },
                ],
                expected: vec![],
            },
            Case {
                title: "Matching fromto",
                left: vec![
                    Patch {
                        tsid: 1,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 2,
                        from: first,
                        to,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 4,
                        from: first,
                        to,
                    },
                ],
                expected: vec![
                    CalculationPatch {
                        tsids: vec![1, 3],
                        timerange: ClosedTimerange { from, to: first },
                    },
                    CalculationPatch {
                        tsids: vec![2, 4],
                        timerange: ClosedTimerange { from: first, to },
                    },
                ],
            },
            Case {
                title: "single left",
                left: vec![Patch { tsid: 1, from, to }],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 4,
                        from: first,
                        to,
                    },
                ],
                expected: vec![
                    CalculationPatch {
                        tsids: vec![1, 3],
                        timerange: ClosedTimerange { from, to: first },
                    },
                    CalculationPatch {
                        tsids: vec![1, 4],
                        timerange: ClosedTimerange { from: first, to },
                    },
                ],
            },
            Case {
                title: "single right",
                right: vec![Patch { tsid: 1, from, to }],
                left: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 4,
                        from: first,
                        to,
                    },
                ],
                expected: vec![
                    CalculationPatch {
                        tsids: vec![3, 1],
                        timerange: ClosedTimerange { from, to: first },
                    },
                    CalculationPatch {
                        tsids: vec![4, 1],
                        timerange: ClosedTimerange { from: first, to },
                    },
                ],
            },
            Case {
                title: "staggered middle point",
                left: vec![
                    Patch {
                        tsid: 1,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 2,
                        from: first,
                        to,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: third,
                    },
                    Patch {
                        tsid: 4,
                        from: third,
                        to,
                    },
                ],
                expected: vec![
                    CalculationPatch {
                        tsids: vec![1, 3],
                        timerange: ClosedTimerange { from, to: first },
                    },
                    CalculationPatch {
                        tsids: vec![2, 3],
                        timerange: ClosedTimerange {
                            from: first,
                            to: third,
                        },
                    },
                    CalculationPatch {
                        tsids: vec![2, 4],
                        timerange: ClosedTimerange { from: third, to },
                    },
                ],
            },
            Case {
                title: "staggered start",
                left: vec![
                    Patch {
                        tsid: 1,
                        from: first,
                        to: third,
                    },
                    Patch {
                        tsid: 2,
                        from: third,
                        to,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: second,
                    },
                    Patch {
                        tsid: 4,
                        from: second,
                        to,
                    },
                ],
                expected: vec![
                    CalculationPatch {
                        tsids: vec![1, 3],
                        timerange: ClosedTimerange {
                            from: first,
                            to: second,
                        },
                    },
                    CalculationPatch {
                        tsids: vec![1, 4],
                        timerange: ClosedTimerange {
                            from: second,
                            to: third,
                        },
                    },
                    CalculationPatch {
                        tsids: vec![2, 4],
                        timerange: ClosedTimerange { from: third, to },
                    },
                ],
            },
            Case {
                title: "staggered end",
                left: vec![
                    Patch {
                        tsid: 1,
                        from: first,
                        to: third,
                    },
                    Patch {
                        tsid: 2,
                        from: third,
                        to,
                    },
                ],
                right: vec![
                    Patch {
                        tsid: 3,
                        from,
                        to: first,
                    },
                    Patch {
                        tsid: 4,
                        from: first,
                        to: second,
                    },
                ],
                expected: vec![CalculationPatch {
                    tsids: vec![1, 4],
                    timerange: ClosedTimerange {
                        from: first,
                        to: second,
                    },
                }],
            },
        ];

        for case in cases {
            let merged = merge_patches(vec![case.left, case.right]);
            assert_eq!(merged, case.expected, "{}", case.title);
        }
    }
}
