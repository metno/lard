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

pub fn persist(tables: &(ParamPermitTable, StationPermitTable)) -> Result<(), Error> {
    let (param_table, station_table) = tables;
    let param_records = flatten_param_table(param_table);
    let station_records = flatten_station_table(station_table);

    write_to_csv(param_records, PARAM_PATH)?;
    write_to_csv(station_records, STATION_PATH)
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

pub fn load_persisted() -> Result<(ParamPermitTable, StationPermitTable), Error> {
    warn!("failed to load permit tables from stinfosys, loading from persisted cache");

    let param_records = read_from_csv(PARAM_PATH)?;
    let station_records = read_from_csv(STATION_PATH)?;

    let param_table = build_param_table(param_records);
    let station_table = build_station_table(station_records);
    Ok((param_table, station_table))
}
