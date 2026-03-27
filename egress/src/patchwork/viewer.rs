use crate::patchwork::Error;
use crate::patchwork::{fill_holes, OpenTimerange, PermitId, TsId, TypeId};
use chrono::{DateTime, Utc};

type PriorityList = Vec<(OpenTimerange, i32, TypeId, TsId, PermitId)>;
type FinalTable = Vec<(
    OpenTimerange,
    i32,
    TypeId,
    TsId,
    PermitId,
    Vec<(DateTime<Utc>, Option<DateTime<Utc>>)>,
)>;

fn view_all_patches(priority_list: PriorityList) -> Result<FinalTable, Error> {
    let patches = fill_holes(
        priority_list.clone(),
        OpenTimerange {
            from: None,
            to: None,
        },
    );

    //    let final_table: FinalTable = priority_list.clone().into_iter().map(|(timerange, priority, type_id, ts_id, permit_id)| {
    //        (timerange, priority, type_id, ts_id, permit_id, vec![])
    //    }).collect();

    let mut final_table: FinalTable = Vec::new();
    for item in priority_list.iter() {
        final_table.push((item.0, item.1, item.2, item.3, item.4, vec![]))
    }

    for patch in patches {
        for item in final_table.iter_mut() {
            if patch.tsid == item.3 {
                item.5.push((patch.from, patch.to));
            }
        }
    }

    Ok(final_table)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_viewer() {
        let priority_list: PriorityList = vec![(
            OpenTimerange {
                from: None,
                to: None,
            },
            1,   // Priority!!
            501, // TypeId
            1,   // TsId
            1,   // PermitID
        )];

        let result = view_all_patches(priority_list);
        assert!(result.is_ok()); // Check I got a result

        let final_table = result.unwrap();
        assert_eq!(final_table[0].3, 1); // Check TsId matches
    }
}

// 0. get the types, imports
// 1. get the lists thatwe need
// 2. write a function to merge the priority list with the from and to from patches

// next:
// - make a test input in priority_list and expected output
// - write a viewer function  that generates the priority diagram
