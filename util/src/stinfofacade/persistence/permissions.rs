use serde::Serialize;

use crate::stinfofacade::{
    permissions::{ParamPermit, ParamPermitTable, StationPermitTable},
    persistence::{write_to_csv, Error},
};

#[derive(Clone, Debug, Serialize)]
struct StationPermitRecord {
    station_id: i32,
    permit_id: i32,
}

#[derive(Clone, Debug, Serialize)]
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
    let (station_records, param_records) = (
        flatten_station_table(station_table),
        flatten_param_table(param_table),
    );
    write_to_csv(station_records, STATION_PATH)?;
    write_to_csv(param_records, PARAM_PATH)
}
