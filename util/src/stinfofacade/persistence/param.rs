use std::{collections::HashMap, path::Path};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::stinfofacade::{
    param::{extract_scalar_paramids, ReferenceParam, Tables},
    persistence::{read_from_csv, write_to_csv, Error},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Record {
    pub code: String,
    pub id: i32,
    pub is_scalar: bool,
}

const PATH: &str = "persistence/param.csv";

fn flatten_table(table: &HashMap<String, ReferenceParam>) -> Vec<Record> {
    table
        .iter()
        .map(|(code, ReferenceParam { id, is_scalar })| Record {
            code: code.clone(),
            id: *id,
            is_scalar: *is_scalar,
        })
        .collect()
}

pub async fn persist_to_path(tables: &Tables, path: impl AsRef<Path>) -> Result<(), Error> {
    let records = flatten_table(&tables.code_table);

    write_to_csv(records, path).await
}

pub async fn persist(tables: &Tables) -> Result<(), Error> {
    persist_to_path(tables, PATH).await
}

fn build_table(records: Vec<Record>) -> Tables {
    let code_table = records
        .into_iter()
        .map(
            |Record {
                 code,
                 id,
                 is_scalar,
             }| (code, ReferenceParam { id, is_scalar }),
        )
        .collect();
    let scalar_paramids = extract_scalar_paramids(&code_table);

    Tables {
        code_table,
        scalar_paramids,
    }
}

async fn load_persisted_from_path(path: impl AsRef<Path>) -> Result<Tables, Error> {
    let records = read_from_csv(path).await?;

    Ok(build_table(records))
}

pub async fn load_persisted() -> Result<Tables, Error> {
    warn!("failed to load param tables from stinfosys, loading from persisted cache");

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
            (
                "Empty cache",
                Tables {
                    code_table: HashMap::new(),
                    scalar_paramids: vec![],
                },
            ),
            (
                "Occupied cache",
                Tables {
                    code_table: HashMap::from([
                        (
                            "TA".to_string(),
                            ReferenceParam {
                                id: 211,
                                is_scalar: true,
                            },
                        ),
                        (
                            "TJ".to_string(),
                            ReferenceParam {
                                id: 226,
                                is_scalar: true,
                            },
                        ),
                        (
                            "KLOBS".to_string(),
                            ReferenceParam {
                                id: 1022,
                                is_scalar: false,
                            },
                        ),
                    ]),
                    scalar_paramids: vec![211, 226],
                },
            ),
            (
                "Shrunk cache",
                Tables {
                    code_table: HashMap::from([
                        (
                            "TA".to_string(),
                            ReferenceParam {
                                id: 211,
                                is_scalar: true,
                            },
                        ),
                        (
                            "KLOBS".to_string(),
                            ReferenceParam {
                                id: 1022,
                                is_scalar: false,
                            },
                        ),
                    ]),
                    scalar_paramids: vec![211],
                },
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
