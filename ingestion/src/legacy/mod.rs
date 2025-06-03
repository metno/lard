use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{util::levels::LevelTable, DbPools, ParamConversions, PermitTables};

pub mod checked;
pub mod common;
pub mod raw;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Checked(#[from] checked::Error),
    #[error(transparent)]
    Raw(#[from] raw::Error),
    #[error("Failed to join tasks: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    pools: DbPools,
    brokers: String,
    group: String,
    raw_topic: &'static str,
    checked_topic: &'static str,
    checked_hist_topic: &'static str,
    cancel_token: CancellationToken,
    permit_table: PermitTables,
    level_table: LevelTable,
    param_conversions: ParamConversions,
) -> Result<(), Error> {
    let raw_handle = tokio::spawn(raw::ingest(
        pools.clone(),
        brokers.clone(),
        group.clone(),
        raw_topic,
        cancel_token.clone(),
        permit_table.clone(),
        level_table.clone(),
        param_conversions,
    ));
    let checked_handle = tokio::spawn(checked::ingest(
        pools.clone(),
        brokers.clone(),
        group.clone(),
        checked_topic,
        cancel_token.clone(),
        permit_table.clone(),
        level_table.clone(),
    ));
    let checked_hist_handle = tokio::spawn(checked::ingest(
        pools,
        brokers,
        group,
        checked_hist_topic,
        cancel_token,
        permit_table,
        level_table,
    ));

    let (raw_res, checked_res, checked_hist_res) =
        tokio::join!(raw_handle, checked_handle, checked_hist_handle);
    raw_res??;
    checked_res??;
    checked_hist_res??;

    Ok(())
}
