use crate::patchwork::Error;
use crate::patchwork::{fill_holes, OpenTimerange, PermitId, TsId, TypeId};
use chrono::TimeZone;
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
        // Define times for priority_list
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2026, 3, 2, 0, 0, 0).unwrap();
        let t3: DateTime<Utc> = Utc.with_ymd_and_hms(2026, 3, 3, 0, 0, 0).unwrap();
        let t4: DateTime<Utc> = Utc.with_ymd_and_hms(2026, 3, 4, 0, 0, 0).unwrap();

        let p1 = (
            OpenTimerange {
                from: Some(t1),
                to: Some(t2),
            },
            1,
            330,
            1,
            1,
        );
        let p2 = (
            OpenTimerange {
                from: Some(t1),
                to: Some(t2),
            },
            2,
            501,
            1,
            1,
        );
        let p3 = (
            OpenTimerange {
                from: Some(t1),
                to: Some(t2),
            },
            3,
            504,
            1,
            1,
        );

        let priority_list: PriorityList = vec![p1, p2, p3];

        println!("Priority list: {:?}", priority_list);

        let result = view_all_patches(priority_list);
        assert!(result.is_ok()); // Check I got a result

        let final_table = result.unwrap();
        println!("Final Table: {:?}", final_table);
        assert_eq!(final_table[0].3, 1); // Check TsId matches
    }
}

// 0. get the types, imports
// 1. get the lists thatwe need
// 2. write a function to merge the priority list with the from and to from patches

// next:
// - make a test input in priority_list and expected output
// - write a viewer function  that generates the priority diagram
