use std::path::Path;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::stinfofacade::{
    permissions::{ParamPermit, ParamPermitTable, StationPermitTable},
    persistence::{read_from_csv, write_to_csv, Error},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StationPermitRecord {
    station_id: i32,
    permit_id: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ParamPermitRecord {
    station_id: i32,
    type_id: i32,
    param_id: i32,
    permit_id: i32,
}

// TODO: from env var? maybe just a base from the var?
const STATION_PATH: &str = "persistence/permissions/station.csv";
const PARAM_PATH: &str = "persistence/permissions/param.csv";

fn flatten_station_table(table: &StationPermitTable) -> Vec<StationPermitRecord> {
    table
        .iter()
        .map(|(station_id, permit_id)| StationPermitRecord {
            station_id: *station_id,
            permit_id: *permit_id,
        })
        .collect()
}

fn flatten_param_table(table: &ParamPermitTable) -> Vec<ParamPermitRecord> {
    table
        .iter()
        .flat_map(|(station_id, param_permits)| {
            param_permits.iter().map(
                |ParamPermit {
                     type_id,
                     param_id,
                     permit_id,
                 }| ParamPermitRecord {
                    station_id: *station_id,
                    type_id: *type_id,
                    param_id: *param_id,
                    permit_id: *permit_id,
                },
            )
        })
        .collect()
}

pub fn persist_to_path(
    tables: &(ParamPermitTable, StationPermitTable),
    param_path: impl AsRef<Path>,
    station_path: impl AsRef<Path>,
) -> Result<(), Error> {
    let (param_table, station_table) = tables;
    let param_records = flatten_param_table(param_table);
    let station_records = flatten_station_table(station_table);

    write_to_csv(param_records, param_path)?;
    write_to_csv(station_records, station_path)
}

pub fn persist(tables: &(ParamPermitTable, StationPermitTable)) -> Result<(), Error> {
    persist_to_path(tables, PARAM_PATH, STATION_PATH)
}

fn build_station_table(records: Vec<StationPermitRecord>) -> StationPermitTable {
    records
        .into_iter()
        .map(
            |StationPermitRecord {
                 station_id,
                 permit_id,
             }| (station_id, permit_id),
        )
        .collect()
}

fn build_param_table(records: Vec<ParamPermitRecord>) -> ParamPermitTable {
    records
        .into_iter()
        .chunk_by(|record| record.station_id)
        .into_iter()
        .map(|(station_id, chunk)| {
            (
                station_id,
                chunk
                    .map(|record| ParamPermit {
                        type_id: record.type_id,
                        param_id: record.param_id,
                        permit_id: record.permit_id,
                    })
                    .collect(),
            )
        })
        .collect()
}

fn load_persisted_from_path(
    param_path: impl AsRef<Path>,
    station_path: impl AsRef<Path>,
) -> Result<(ParamPermitTable, StationPermitTable), Error> {
    let param_records = read_from_csv(param_path)?;
    let station_records = read_from_csv(station_path)?;

    let param_table = build_param_table(param_records);
    let station_table = build_station_table(station_records);
    Ok((param_table, station_table))
}

pub fn load_persisted() -> Result<(ParamPermitTable, StationPermitTable), Error> {
    warn!("failed to load permit tables from stinfosys, loading from persisted cache");

    load_persisted_from_path(PARAM_PATH, STATION_PATH)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_roundtrip() {
        let param_file = NamedTempFile::new().unwrap();
        let station_file = NamedTempFile::new().unwrap();

        let cases = [
            ("Empty cache", (HashMap::new(), HashMap::new())),
            (
                "Occupied cache",
                (
                    HashMap::from([
                        (
                            2,
                            vec![
                                ParamPermit {
                                    type_id: 3,
                                    param_id: 4,
                                    permit_id: 5,
                                },
                                ParamPermit {
                                    type_id: 6,
                                    param_id: 7,
                                    permit_id: 8,
                                },
                                ParamPermit {
                                    type_id: 9,
                                    param_id: 10,
                                    permit_id: 11,
                                },
                            ],
                        ),
                        (
                            12,
                            vec![ParamPermit {
                                type_id: 13,
                                param_id: 14,
                                permit_id: 15,
                            }],
                        ),
                    ]),
                    HashMap::from([(1, 2), (3, 4), (5, 6)]),
                ),
            ),
            (
                "Shrunk cache",
                (
                    HashMap::from([(
                        2,
                        vec![ParamPermit {
                            type_id: 3,
                            param_id: 4,
                            permit_id: 5,
                        }],
                    )]),
                    HashMap::from([(3, 4)]),
                ),
            ),
        ];

        for (case_name, tables) in cases {
            persist_to_path(&tables, param_file.path(), station_file.path()).unwrap();
            let roundtripped =
                load_persisted_from_path(param_file.path(), station_file.path()).unwrap();

            assert_eq!(
                tables, roundtripped,
                "failed to roundtrip for case: {case_name}: before: {tables:#?}, after: {roundtripped:#?}",
            );
        }
    }
}
