use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    error::KafkaResult,
    ClientConfig, ClientContext, TopicPartitionList,
};
use tracing::error;

// A simple context to customize the consumer behavior and log when commits fail
pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => (),
            Err(e) => error!("Error while committing offsets: {}", e),
        };
    }
}

// Define a new type for convenience
pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

pub fn create_consumer(brokers: &str, group_id: &str, topic: &str) -> LoggingConsumer {
    let context = LoggingConsumerContext;

    // Documentation on the available config options can be found at
    // https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        // if we don't have a starting offset, or it's out of range, start from the earliest
        // available on the cluster
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    consumer
}
