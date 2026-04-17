use crate::patchwork::Patch;
use chrono::{DateTime, Utc};
use util::ClosedTimerange;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct CalculationPatch {
    pub tsid1: i64,
    pub tsid2: i64,
    pub tsid3: Option<i64>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

pub fn merge_patches(
    patch1: Vec<Patch>,
    patch2: Vec<Patch>,
    patch3: Option<Vec<Patch>>,
) -> Vec<CalculationPatch> {
    if patch1.is_empty() || patch2.is_empty() {
        return vec![];
    }

    let collect_patches1_patches2: Vec<CalculationPatch> = patch1
        .iter()
        .flat_map(|param1| {
            patch2.iter().filter_map(|param2| {
                let overlap2 = param1.overlap(param2)?;
                Some(CalculationPatch {
                    tsid1: param1.tsid,
                    tsid2: param2.tsid,
                    tsid3: None,
                    from: overlap2.from,
                    to: overlap2.to,
                })
            })
        })
        .collect();

    // check if also have to loop over the 3rd patch
    if let Some(patch3) = &patch3 {
        collect_patches1_patches2
            .iter()
            .flat_map(|overlap12| {
                patch3.iter().filter_map(|param3| {
                    let overlap12_timerange = ClosedTimerange::new(overlap12.from, overlap12.to);
                    let patch3_timerange = ClosedTimerange::new(param3.from, param3.to);
                    let overlap2 = patch3_timerange.overlap(overlap12_timerange)?;
                    Some(CalculationPatch {
                        tsid1: overlap12.tsid1,
                        tsid2: overlap12.tsid2,
                        tsid3: Some(param3.tsid),
                        from: overlap2.from,
                        to: overlap2.to,
                    })
                })
            })
            .collect()
    } else {
        // didn't have a 3rd patch, so just return the merged patches from patch1 and patch2
        collect_patches1_patches2
    }
}
