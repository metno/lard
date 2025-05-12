use crate::{DbPools, PermitTables};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub mod checked;
pub mod raw;
pub mod util;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Checked(#[from] checked::Error),
    #[error(transparent)]
    Raw(#[from] raw::Error),
    #[error("Failed to join tasks: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub async fn run(
    pools: DbPools,
    brokers: String,
    group: String,
    topic: &'static str,
    cancel_token: CancellationToken,
    permit_table: PermitTables,
) -> Result<(), Error> {
    let raw_handle = tokio::spawn(raw::ingest());
    let checked_handle = tokio::spawn(checked::ingest(
        pools,
        brokers,
        group,
        topic,
        cancel_token,
        permit_table,
    ));

    let (raw_res, checked_res) = tokio::join!(raw_handle, checked_handle);
    raw_res??;
    checked_res??;

    Ok(())
}
