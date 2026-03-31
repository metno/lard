use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::stinfofacade::{
    param::{ReferenceParam, Tables, extract_scalar_paramids},
    persistence::{Error, read_from_csv, write_to_csv},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Record {
    pub code: Option<String>,
    pub id: i32,
    pub is_scalar: bool,
}

const PATH: &str = "persistence/param.csv";

pub async fn persist_to_path(records: Vec<Record>, path: impl AsRef<Path>) -> Result<(), Error> {
    write_to_csv(records, path).await
}

pub async fn persist(records: Vec<Record>) -> Result<(), Error> {
    persist_to_path(records, PATH).await
}

pub fn build_table(records: &[Record]) -> Tables {
    let code_table = records
        .iter()
        .filter(|record| record.code.is_some())
        .map(|record| {
            (
                record.code.clone().unwrap(),
                ReferenceParam {
                    id: record.id,
                    is_scalar: record.is_scalar,
                },
            )
        })
        .collect();

    let scalar_paramids = extract_scalar_paramids(records);

    Tables {
        code_table,
        scalar_paramids,
    }
}

async fn load_persisted_from_path(path: impl AsRef<Path>) -> Result<Tables, Error> {
    let records = read_from_csv(path).await?;

    Ok(build_table(&records))
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
                vec![],
                Tables {
                    code_table: HashMap::new(),
                    scalar_paramids: vec![],
                },
            ),
            (
                "Occupied cache",
                vec![
                    Record {
                        code: Some("TA".to_string()),
                        id: 211,
                        is_scalar: true,
                    },
                    Record {
                        code: Some("TJ".to_string()),
                        id: 226,
                        is_scalar: true,
                    },
                    Record {
                        code: Some("KLOBS".to_string()),
                        id: 1022,
                        is_scalar: false,
                    },
                ],
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
                vec![
                    Record {
                        code: Some("TA".to_string()),
                        id: 211,
                        is_scalar: true,
                    },
                    Record {
                        code: Some("KLOBS".to_string()),
                        id: 1022,
                        is_scalar: false,
                    },
                ],
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

        for (case_name, records, tables) in cases {
            persist_to_path(records, file.path()).await.unwrap();
            let roundtripped = load_persisted_from_path(file.path()).await.unwrap();

            assert_eq!(
                tables, roundtripped,
                "failed to roundtrip for case: {case_name}: before: {tables:#?}, after: {roundtripped:#?}",
            );
        }
    }
}
