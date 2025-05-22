use rdkafka::{consumer::Consumer, error::KafkaError, Message};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    kldata::{parse_kldata, ObsinnChunk},
    util::{
        kafka::{create_consumer, Offset},
        levels::LevelTable,
        permissions::PermitTables,
    },
    DbPools, ParamConversions, KAFKA_RAW_FAILURES, KAFKA_RAW_MESSAGES_RECEIVED,
};

// The number of parsed kafka messages that can build up waiting for the DB task
const DB_BUFFER_SIZE: usize = 200;

#[derive(Error, Debug)]
pub enum Error {
    #[error("kafka returned an error: {0}")]
    Kafka(#[from] KafkaError),
}

#[allow(clippy::too_many_arguments)]
pub async fn ingest(
    _pools: DbPools,
    brokers: String,
    group: String,
    topic: &'static str,
    cancel_token: CancellationToken,
    _permit_table: PermitTables,
    _level_table: LevelTable,
    param_conversions: ParamConversions,
) -> Result<(), Error> {
    let consumer = create_consumer(brokers.as_str(), group.as_str(), topic);

    let (db_tx, mut _db_rx) =
        tokio::sync::mpsc::channel::<(Vec<ObsinnChunk>, Offset)>(DB_BUFFER_SIZE);
    let (_offset_tx, mut offset_rx) = tokio::sync::mpsc::channel::<Offset>(1);

    let _db_task = tokio::task::spawn(async move {
        todo!();
    });

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("Cancellation token triggered");
                // This will cause db_task to break and return
                drop(db_tx);
                break;
            }
            Some(Offset { partition, offset }) = offset_rx.recv() => {
                if let Err(e) = consumer.store_offset(topic, partition, offset) {
                    metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                    error!("failed to mark offset on raw queue: {}", e);
                }
            }
            poll_result = consumer.recv() => {
                match poll_result {
                    Err(e) => {
                        metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                        error!("failed to poll raw kafka: {}", Error::Kafka(e));
                    }
                    Ok(message) => {
                        metrics::counter!(KAFKA_RAW_MESSAGES_RECEIVED).increment(1);

                        match message.payload().map(std::str::from_utf8) {
                            Some(Ok(payload_str)) => {
                                let offset = Offset { partition:message.partition(), offset: message.offset() };

                                // TODO: handle error
                                // TODO: remove clone?
                                let (_, chunks) = parse_kldata(payload_str, param_conversions.clone()).unwrap();

                                db_tx.send((chunks, offset)).await.unwrap()
                            },
                            Some(Err(_)) => {
                                metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                                error!("failed to parse raw kafka payload as utf8. payload: {:?}",  message.payload());
                            },
                            None => warn!("Received empty message from raw kafka"),
                        }

                    }
                }
            }
        }
    }

    Ok(())
}
