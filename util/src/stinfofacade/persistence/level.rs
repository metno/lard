use std::{collections::HashMap, path::Path};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    ParamId,
    stinfofacade::{
        level::{Direction, Level, Unit},
        persistence::{Error, read_from_csv, write_to_csv},
    },
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Record {
    param_id: ParamId,
    default_hlevel: i32,
    unit: Unit,
    direction: Direction,
}

const PATH: &str = "persistence/level.csv";

fn flatten_table(table: &HashMap<ParamId, Level>) -> Vec<Record> {
    table
        .iter()
        .map(
            |(
                param_id,
                Level {
                    default_hlevel,
                    unit,
                    direction,
                },
            )| Record {
                param_id: *param_id,
                default_hlevel: *default_hlevel,
                unit: *unit,
                direction: *direction,
            },
        )
        .collect()
}

pub async fn persist_to_path(
    table: &HashMap<ParamId, Level>,
    path: impl AsRef<Path>,
) -> Result<(), Error> {
    let records = flatten_table(table);

    write_to_csv(records, path).await
}

pub async fn persist(table: &HashMap<ParamId, Level>) -> Result<(), Error> {
    persist_to_path(table, PATH).await
}

fn build_table(records: Vec<Record>) -> HashMap<ParamId, Level> {
    records
        .into_iter()
        .map(
            |Record {
                 param_id,
                 default_hlevel,
                 unit,
                 direction,
             }| {
                (
                    param_id,
                    Level {
                        default_hlevel,
                        unit,
                        direction,
                    },
                )
            },
        )
        .collect()
}

async fn load_persisted_from_path(
    path: impl AsRef<Path>,
) -> Result<HashMap<ParamId, Level>, Error> {
    let records = read_from_csv(path).await?;

    Ok(build_table(records))
}

pub async fn load_persisted() -> Result<HashMap<ParamId, Level>, Error> {
    warn!("failed to load level table from stinfosys, loading from persisted cache");

    load_persisted_from_path(PATH).await
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_roundtrip() {
        let file = NamedTempFile::new().unwrap();

        let cases = [
            ("Empty cache", HashMap::new()),
            (
                "Occupied cache",
                HashMap::from([
                    (
                        1,
                        Level {
                            default_hlevel: 0,
                            unit: Unit::Cm,
                            direction: Direction::Down,
                        },
                    ),
                    (
                        2,
                        Level {
                            default_hlevel: 10,
                            unit: Unit::M,
                            direction: Direction::Up,
                        },
                    ),
                    (
                        3,
                        Level {
                            default_hlevel: 0,
                            unit: Unit::Cm,
                            direction: Direction::Down,
                        },
                    ),
                ]),
            ),
            (
                "Shrunk cache",
                HashMap::from([
                    (
                        1,
                        Level {
                            default_hlevel: 0,
                            unit: Unit::Cm,
                            direction: Direction::Down,
                        },
                    ),
                    (
                        3,
                        Level {
                            default_hlevel: 0,
                            unit: Unit::Cm,
                            direction: Direction::Down,
                        },
                    ),
                ]),
            ),
        ];

        for (case_name, tables) in cases {
            persist_to_path(&tables, file.path()).await.unwrap();
            let roundtripped = load_persisted_from_path(file.path()).await.unwrap();

            assert_eq!(
                tables, roundtripped,
                "failed to roundtrip for case: {case_name}: before: {tables:#?}, after: {roundtripped:#?}",
            );
        }
    }
}
