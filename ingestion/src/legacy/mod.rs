use crate::{DbPools, PermitTables};
use tokio_util::sync::CancellationToken;

pub mod kvkafka;

pub async fn run(
    pools: DbPools,
    brokers: &str,
    group: &str,
    topic: &str,
    cancel_token: CancellationToken,
    permit_table: PermitTables,
) -> Result<(), kvkafka::Error> {
    kvkafka::ingest_kvkafka(pools, brokers, group, topic, cancel_token, permit_table).await
}
