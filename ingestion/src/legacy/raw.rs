use crate::util::kafka_consumer::create_consumer;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {}

pub async fn ingest(brokers: String, group: String, topic: &'static str) -> Result<(), Error> {
    let _consumer = create_consumer(brokers.as_str(), group.as_str(), topic);

    Ok(())
}
