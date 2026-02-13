use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    stinfofacade::{
        message_priority::{DefaultTable, ExceptionTable, MessagePriority},
        persistence::{read_from_csv, write_to_csv, Error},
    },
    OpenTimerange, ParamId, PatchworkLabel, StationId, TypeId,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DefaultRecord {
    type_id: TypeId,
    param_id: ParamId,
    priority: i32,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExceptionRecord {
    station_id: StationId,
    param_id: ParamId,
    level: Option<i32>,
    sensor: Option<i32>,
    type_id: TypeId,
    priority: i32,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

// TODO: from env var? maybe just a base from the var?
const DEFAULT_PATH: &str = "persistence/message_priority/default.csv";
const EXCEPION_PATH: &str = "persistence/message_priority/exception.csv";

fn flatten_default_table(table: &DefaultTable) -> Vec<DefaultRecord> {
    table
        .iter()
        .map(|((type_id, param_id), mp)| DefaultRecord {
            type_id: *type_id,
            param_id: *param_id,
            priority: mp.priority,
            from: mp.timerange.from,
            to: mp.timerange.to,
        })
        .collect()
}

fn flatten_exception_table(table: &ExceptionTable) -> Vec<ExceptionRecord> {
    table
        .iter()
        .map(|((pw, type_id), mp)| ExceptionRecord {
            station_id: pw.station_id,
            param_id: pw.param_id,
            level: pw.level,
            sensor: pw.sensor,
            type_id: *type_id,
            priority: mp.priority,
            from: mp.timerange.from,
            to: mp.timerange.to,
        })
        .collect()
}

pub async fn persist_to_path(
    default_table: &DefaultTable,
    exception_table: &ExceptionTable,
    default_path: impl AsRef<Path>,
    exception_path: impl AsRef<Path>,
) -> Result<(), Error> {
    let default_records = flatten_default_table(default_table);
    let exception_records = flatten_exception_table(exception_table);

    write_to_csv(default_records, default_path).await?;
    write_to_csv(exception_records, exception_path).await
}

pub async fn persist(
    default_table: &DefaultTable,
    exception_table: &ExceptionTable,
) -> Result<(), Error> {
    persist_to_path(default_table, exception_table, DEFAULT_PATH, EXCEPION_PATH).await
}

fn build_default_table(records: Vec<DefaultRecord>) -> DefaultTable {
    records
        .into_iter()
        .map(
            |DefaultRecord {
                 type_id,
                 param_id,
                 priority,
                 from,
                 to,
             }| {
                (
                    (type_id, param_id),
                    MessagePriority {
                        priority,
                        timerange: OpenTimerange { from, to },
                    },
                )
            },
        )
        .collect()
}

fn build_exception_table(records: Vec<ExceptionRecord>) -> ExceptionTable {
    records
        .into_iter()
        .map(
            |ExceptionRecord {
                 station_id,
                 param_id,
                 level,
                 sensor,
                 type_id,
                 priority,
                 from,
                 to,
             }| {
                (
                    (
                        PatchworkLabel {
                            station_id,
                            param_id,
                            level,
                            sensor,
                        },
                        type_id,
                    ),
                    MessagePriority {
                        priority,
                        timerange: OpenTimerange { from, to },
                    },
                )
            },
        )
        .collect()
}

async fn load_persisted_from_path(
    default_path: impl AsRef<Path>,
    exception_path: impl AsRef<Path>,
) -> Result<(DefaultTable, ExceptionTable), Error> {
    let default_records = read_from_csv(default_path).await?;
    let exception_records = read_from_csv(exception_path).await?;

    let default_table = build_default_table(default_records);
    let exception_table = build_exception_table(exception_records);
    Ok((default_table, exception_table))
}

pub async fn load_persisted() -> Result<(DefaultTable, ExceptionTable), Error> {
    warn!("failed to load message_priority tables from stinfosys, loading from persisted cache");

    load_persisted_from_path(DEFAULT_PATH, EXCEPION_PATH).await
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use chrono::{TimeZone, Utc};
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_roundtrip() {
        let default_file = NamedTempFile::new().unwrap();
        let exception_file = NamedTempFile::new().unwrap();

        let t1 = Some(Utc.with_ymd_and_hms(2000, 1, 1, 1, 15, 0).unwrap());
        let t2 = Some(Utc.with_ymd_and_hms(2001, 1, 1, 1, 15, 0).unwrap());
        let t3 = None;

        let cases = [
            ("Empty cache", (HashMap::new(), HashMap::new())),
            (
                "Occupied cache",
                (
                    HashMap::from([
                        (
                            (1, 2),
                            MessagePriority {
                                priority: 3,
                                timerange: OpenTimerange { from: t1, to: t2 },
                            },
                        ),
                        (
                            (3, 4),
                            MessagePriority {
                                priority: 5,
                                timerange: OpenTimerange { from: t2, to: t3 },
                            },
                        ),
                        (
                            (6, 7),
                            MessagePriority {
                                priority: 8,
                                timerange: OpenTimerange { from: t1, to: t3 },
                            },
                        ),
                    ]),
                    HashMap::from([
                        (
                            (
                                PatchworkLabel {
                                    station_id: 1,
                                    param_id: 2,
                                    level: Some(3),
                                    sensor: Some(4),
                                },
                                2,
                            ),
                            MessagePriority {
                                priority: 3,
                                timerange: OpenTimerange { from: t1, to: t2 },
                            },
                        ),
                        (
                            (
                                PatchworkLabel {
                                    station_id: 5,
                                    param_id: 6,
                                    level: Some(7),
                                    sensor: Some(8),
                                },
                                9,
                            ),
                            MessagePriority {
                                priority: 10,
                                timerange: OpenTimerange { from: t1, to: t1 },
                            },
                        ),
                        (
                            (
                                PatchworkLabel {
                                    station_id: 15,
                                    param_id: 16,
                                    level: Some(17),
                                    sensor: Some(18),
                                },
                                19,
                            ),
                            MessagePriority {
                                priority: 100,
                                timerange: OpenTimerange { from: t1, to: t3 },
                            },
                        ),
                    ]),
                ),
            ),
            (
                "Shrunk cache",
                (
                    HashMap::from([
                        (
                            (1, 2),
                            MessagePriority {
                                priority: 3,
                                timerange: OpenTimerange { from: t1, to: t2 },
                            },
                        ),
                        (
                            (6, 7),
                            MessagePriority {
                                priority: 8,
                                timerange: OpenTimerange { from: t1, to: t3 },
                            },
                        ),
                    ]),
                    HashMap::from([
                        (
                            (
                                PatchworkLabel {
                                    station_id: 1,
                                    param_id: 2,
                                    level: Some(3),
                                    sensor: Some(4),
                                },
                                2,
                            ),
                            MessagePriority {
                                priority: 3,
                                timerange: OpenTimerange { from: t1, to: t2 },
                            },
                        ),
                        (
                            (
                                PatchworkLabel {
                                    station_id: 15,
                                    param_id: 16,
                                    level: Some(17),
                                    sensor: Some(18),
                                },
                                19,
                            ),
                            MessagePriority {
                                priority: 100,
                                timerange: OpenTimerange { from: t1, to: t3 },
                            },
                        ),
                    ]),
                ),
            ),
        ];

        for (case_name, tables) in cases {
            persist_to_path(
                &tables.0,
                &tables.1,
                default_file.path(),
                exception_file.path(),
            )
            .await
            .unwrap();
            let roundtripped = load_persisted_from_path(default_file.path(), exception_file.path())
                .await
                .unwrap();

            assert_eq!(
                tables, roundtripped,
                "failed to roundtrip for case: {case_name}: before: {tables:#?}, after: {roundtripped:#?}",
            );
        }
    }
}
