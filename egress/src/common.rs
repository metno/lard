use crate::patchwork::Patch;
use chrono::{DateTime, Utc};
use util::{ClosedTimerange, TsId};

#[derive(Clone, Default, PartialEq, Debug)]
pub struct CalculationPatch {
    pub tsids: Vec<TsId>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl Patch {
    pub fn into_calculation_patch(&self) -> CalculationPatch {
        CalculationPatch {
            tsids: vec![self.tsid],
            from: self.from,
            to: self.to,
        }
    }
}

fn merge_patch_into_calculation_patches(
    calculation_patches: Vec<CalculationPatch>,
    patch: Patch,
) -> Vec<CalculationPatch> {
    let time_calc_patch = ClosedTimerange {
        from: patch.from,
        to: patch.to,
    };
    calculation_patches
        .iter()
        .filter_map(|calculation_patch| {
            let time_patch = ClosedTimerange {
                from: calculation_patch.from,
                to: calculation_patch.to,
            };
            let overlap = time_calc_patch.overlap(time_patch)?;

            Some(CalculationPatch {
                tsids: {
                    let mut tsids = calculation_patch.tsids.clone();
                    tsids.push(patch.tsid);
                    tsids
                },
                from: overlap.from,
                to: overlap.to,
            })
        })
        .collect()
}

pub fn merge_patches(patches: Vec<Vec<Patch>>) -> Vec<CalculationPatch> {
    if patches.is_empty() || patches.len() < 2 {
        // TODO: return an error since incorrect input?
        return vec![];
    }

    let patches_param_1_2 = patches[0]
        .iter()
        .flat_map(|param1| {
            patches[1].iter().filter_map(|param2| {
                let overlap = param1.overlap(param2)?;

                Some(CalculationPatch {
                    tsids: vec![param1.tsid, param2.tsid],
                    from: overlap.from,
                    to: overlap.to,
                })
            })
        })
        .collect::<Vec<CalculationPatch>>();

    if patches.len() == 2 {
        return patches_param_1_2;
    }
    // if have a 3rd param ...
    let patches_3 = patches[2].clone().into_iter();
    patches_3.fold(patches_param_1_2, merge_patch_into_calculation_patches)
}

#[cfg(test)]
mod test {
    use crate::patchwork::Patch;
    use chrono::{Duration, TimeZone};

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
                        from,
                        to: first,
                    },
                    CalculationPatch {
                        tsids: vec![2, 4],
                        from: first,
                        to,
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
                        from,
                        to: first,
                    },
                    CalculationPatch {
                        tsids: vec![1, 4],
                        from: first,
                        to,
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
                        from,
                        to: first,
                    },
                    CalculationPatch {
                        tsids: vec![4, 1],
                        from: first,
                        to,
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
                        from,
                        to: first,
                    },
                    CalculationPatch {
                        tsids: vec![2, 3],
                        from: first,
                        to: third,
                    },
                    CalculationPatch {
                        tsids: vec![2, 4],
                        from: third,
                        to,
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
                        from: first,
                        to: second,
                    },
                    CalculationPatch {
                        tsids: vec![1, 4],
                        from: second,
                        to: third,
                    },
                    CalculationPatch {
                        tsids: vec![2, 4],
                        from: third,
                        to,
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
                    from: first,
                    to: second,
                }],
            },
        ];

        for case in cases {
            let merged = merge_patches(vec![case.left, case.right]);
            assert_eq!(merged, case.expected, "{}", case.title);
        }
    }
}
