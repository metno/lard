use chrono::NaiveDateTime;
use futures::{StreamExt, stream::FuturesUnordered};
use rdkafka::{Message, consumer::Consumer, error::KafkaError};
use std::str::Lines;
use thiserror::Error;
use tokio_postgres::Statement;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    DbPools, KAFKA_RAW_FAILURES, KAFKA_RAW_MESSAGES_RECEIVED, ObsType, PooledPgConn,
    kldata::{
        self, ObsinnHeader, ObsinnId, ParseError, parse_columns, parse_nonscalar, parse_scalar,
    },
    legacy::common::{
        self, Datum as CommonDatum, KvalobsId, Param, UnlabelledDatum as CommonUnlabelledDatum,
        filter_and_label,
    },
    util::kafka::{Offset, create_consumer},
};
use ::util::stinfofacade::{level::LevelTable, param::ParamTables, permissions::PermitTables};

// The number of parsed kafka messages that can build up waiting for the DB task
const DB_BUFFER_SIZE: usize = 200;

// TODO: should we reconsider these ON CONFLICT DO NOTHING? I'm seeing a lot
// of empty entries that are probably obsinn trying to tell us to delete things...
const QUERY_STR: &str = r#"
    INSERT INTO legacy.data
        (timeseries, obstime, original)
    VALUES($1, $2, $3)
    ON CONFLICT ON CONSTRAINT data_pkey
        DO NOTHING
"#;

const NONSCALAR_QUERY_STR: &str = r#"
    INSERT INTO public.nonscalar_data
        (timeseries, obstime, obsvalue)
    VALUES($1, $2, $3)
    ON CONFLICT ON CONSTRAINT nonscalar_data_pkey
        DO NOTHING
"#;

#[derive(Error, Debug)]
pub enum Error {
    #[error("kafka returned an error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("failed to determine format of kafka message")]
    Format,
    #[error("failed to decode kafka message as Utf8")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("failed to parse kldata message: {0}")]
    Parse(#[from] ParseError),
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Common(#[from] common::Error),
}

type Datum = CommonDatum<ObsType>;
type UnlabelledDatum = CommonUnlabelledDatum<ObsType>;

/// Checks that the first bytes (ignoring spaces) are "kldata/" or "kldata\n".
/// We have to do this with the u8 slice because some messages on the topic
/// (bufr) cannot be decoded as utf8
fn is_kldata_message(message: &[u8]) -> Result<bool, Error> {
    let format_raw = message
        .split(|elem| {
            let char = *elem as char;
            char == '\n' || char == '/'
        })
        .next()
        .ok_or(Error::Format)?;

    let format = std::str::from_utf8(format_raw)?;

    Ok(format.trim() == "kldata")
}

// modified version of kldata::parse_obs that returns RawDatum instead of ObsinnChunk
fn parse_obs(
    csv_body: Lines,
    columns: &[ObsinnId],
    reference_params: ParamTables,
    header: ObsinnHeader,
) -> Result<Vec<UnlabelledDatum>, ParseError> {
    let mut obs = Vec::new();
    let reference_params = reference_params.read()?;

    for row in csv_body {
        let (timestamp, vals) = {
            let mut vals = row.split(',').map(str::trim);

            let raw_timestamp = vals.next().ok_or(ParseError::EmptyRow)?;

            // TODO: timestamp parsing needs to handle milliseconds and truncated timestamps?
            let timestamp = NaiveDateTime::parse_from_str(raw_timestamp, "%Y%m%d%H%M%S")?.and_utc();

            (timestamp, vals)
        };

        for (i, val) in vals.enumerate() {
            // TODO: should we do some smart bounds-checking??
            let col = columns[i].clone();

            // rejection is acceptable here, because things we don't catch should
            // be covered by the checked queue
            let param_entry = reference_params.code_table.get(&col.param_code);

            let (sensor, level) = col.sensor_and_level.unwrap_or((0, 0));

            let param = match param_entry {
                Some(entry) => Param::Id(entry.id),
                None => Param::Code(col.param_code.clone()),
            };

            let value: ObsType = if param_entry.is_some() && param_entry.unwrap().is_scalar
                // things marked as scalar in stinfosys that are known not to be floats
                && !kldata::SPECIAL_CASES.contains(&col.param_code.as_str())
            {
                parse_scalar(val, &col)
            } else {
                parse_nonscalar(val)
            };

            obs.push(UnlabelledDatum {
                kvid: KvalobsId {
                    station: header.station_id,
                    param,
                    typeid: header.type_id,
                    sensor,
                    level,
                },
                obstime: timestamp,
                value,
            })
        }
    }

    Ok(obs)
}

// modified version of kldata::parse_kldata that returns RawDatum instead of ObsinnChunk
pub fn parse(msg: &str, reference_params: ParamTables) -> Result<Vec<UnlabelledDatum>, ParseError> {
    let (header, columns, csv_body) = {
        let mut csv_body = msg.lines();

        // parse the first two lines of the message as meta header, and csv column names,
        // leave the rest as an iter over the lines of csv body
        let header = ObsinnHeader::parse(csv_body.next().ok_or(ParseError::Lines)?)?;
        let columns = parse_columns(csv_body.next().ok_or(ParseError::Lines)?)?;

        (header, columns, csv_body)
    };

    parse_obs(csv_body, &columns, reference_params, header)
}

async fn insert(
    conn: &mut PooledPgConn<'_>,
    data: Vec<Datum>,
    query: &Statement,
    nonscalar_query: &Statement,
) -> Result<(), Error> {
    let transaction = conn.transaction().await?;

    // This lock (and the one in the equivalent function in checked) are needed
    // to prevent deadlocks between transactions from each db task.
    //
    // I think this (rare) deadlock happens because two transactions can try
    // to update the same two rows in a different order, although one part of
    // this that doesn't make sense is that the raw query doesn't update, and so
    // shouldn't acquire a row lock? The docs are unclear on this.
    //
    // Unfortunately, the introduction of this lock seems to cause a ~30%
    // slowdown If we want to reclaim that throughput, alternative approaches
    // might be:
    // - Just let the deadlocks happen, then catch them and retry
    // - Order the queries in each transaction in a consistent way (sort by
    //   timeseries then obstime?) so such a deadlock can't happen
    transaction
        .execute("LOCK TABLE legacy.data IN SHARE ROW EXCLUSIVE MODE", &[])
        .await?;

    transaction
        .execute(
            "LOCK TABLE public.nonscalar_data IN SHARE ROW EXCLUSIVE MODE",
            &[],
        )
        .await?;

    let mut futures = data
        .iter()
        .map(|datum| async {
            match &datum.value {
                ObsType::Scalar(value) => {
                    transaction
                        .execute(query, &[&datum.tsid, &datum.obstime, &value])
                        .await
                }
                ObsType::NonScalar(value) => {
                    transaction
                        .execute(nonscalar_query, &[&datum.tsid, &datum.obstime, &value])
                        .await
                }
            }
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(res) = futures.next().await {
        res?;
    }
    drop(futures);

    transaction.commit().await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_batch(
    open_conn: &mut PooledPgConn<'_>,
    restricted_conn: &mut PooledPgConn<'_>,
    raw_buffer: &[(Vec<UnlabelledDatum>, Offset)],
    permit_table: PermitTables,
    level_table: LevelTable,
    open_query: &Statement,
    restricted_query: &Statement,
    open_nonscalar_query: &Statement,
    restricted_nonscalar_query: &Statement,
) -> Result<(), Error> {
    let (open_data, restricted_data) = filter_and_label::<ObsType>(
        open_conn,
        restricted_conn,
        raw_buffer,
        permit_table,
        level_table,
    )
    .await?;

    let (res1, res2) = tokio::join!(
        insert(open_conn, open_data, open_query, open_nonscalar_query),
        insert(
            restricted_conn,
            restricted_data,
            restricted_query,
            restricted_nonscalar_query
        )
    );
    res1?;
    res2?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn ingest(
    pools: DbPools,
    brokers: String,
    group: String,
    topic: &'static str,
    cancel_token: CancellationToken,
    permit_table: PermitTables,
    level_table: LevelTable,
    param_conversions: ParamTables,
) -> Result<(), Error> {
    let consumer = create_consumer(brokers.as_str(), group.as_str(), topic);

    let (db_tx, mut db_rx) =
        tokio::sync::mpsc::channel::<(Vec<UnlabelledDatum>, Offset)>(DB_BUFFER_SIZE);
    let (offset_tx, mut offset_rx) = tokio::sync::mpsc::channel::<Offset>(1);

    let db_task = tokio::task::spawn(async move {
        let mut open_conn = pools
            .open
            .get()
            .await
            .expect("legacy::raw DB task could'nt connect to open DB");
        let mut restricted_conn = pools
            .restricted
            .get()
            .await
            .expect("legacy::raw DB task could'nt connect to restricted DB");

        let open_query = open_conn
            .prepare(QUERY_STR)
            .await
            .expect("legacy::raw DB task couldn't prepare open query");
        let restricted_query = restricted_conn
            .prepare(QUERY_STR)
            .await
            .expect("legacy::raw DB task couldn't prepare restricted query");
        let open_nonscalar_query = open_conn
            .prepare(NONSCALAR_QUERY_STR)
            .await
            .expect("legacy::raw DB task couldn't prepare open nonscalar query");
        let restricted_nonscalar_query = restricted_conn
            .prepare(NONSCALAR_QUERY_STR)
            .await
            .expect("legacy::raw DB task couldn't prepare restricted nonscalar query");

        let mut raw_buffer: Vec<(Vec<UnlabelledDatum>, Offset)> =
            Vec::with_capacity(DB_BUFFER_SIZE);

        while db_rx.recv_many(&mut raw_buffer, DB_BUFFER_SIZE).await != 0 {
            let offset = raw_buffer.last().unwrap().1.clone();

            if let Err(e) = insert_batch(
                &mut open_conn,
                &mut restricted_conn,
                &raw_buffer,
                permit_table.clone(),
                level_table.clone(),
                &open_query,
                &restricted_query,
                &open_nonscalar_query,
                &restricted_nonscalar_query,
            )
            .await
            {
                metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                error!(
                    "Failed to insert kafka messages: {}, offset: {:?}",
                    e, offset
                );
                continue;
            };

            if let Err(e) = offset_tx.send(offset).await {
                metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                error!("Failed to send offset: {}", e);
            };
            raw_buffer.clear();
        }
    });

    'consume_loop: loop {
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

                        match message.payload() {
                            Some(payload) => {
                                if let Err(e) = 'parse_block: {
                                    if !is_kldata_message(payload)? {
                                        // The raw queue contains messages from several sources and
                                        // formats we are only interested in "kldata" which comes
                                        // from obsinn.
                                        // Other formats I'm aware of:
                                        // - BUFR: base64 encoded format for some foreign data, ODA
                                        //   ingests this, but we decided not to, since it's covered
                                        //   by E-Soh
                                        // - SYNOP, COMOBS: Not sure what these are, but ODA ignored
                                        //   them so we will too unless given a reason not to
                                        continue 'consume_loop;
                                    }
                                    let payload_str = std::str::from_utf8(payload)?.trim();

                                    let offset = Offset { partition:message.partition(), offset: message.offset() };

                                    // TODO: remove clone?
                                    match parse(payload_str, param_conversions.clone()){
                                        Ok(data) => db_tx.send((data, offset)).await.unwrap(),
                                        Err(e) => {
                                            error!("failed kldata message:\n{:?}", payload_str);
                                            break 'parse_block Err(e.into())
                                        },
                                    }

                                    Ok::<(), Error>(())
                                } {
                                    metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                                    error!("failed to parse kldata message: {}", e);
                                }
                            },
                            None => warn!("Received empty message from raw kafka"),
                        }
                    }
                }
            }
        }
    }

    while let Some(Offset { partition, offset }) = offset_rx.recv().await {
        if let Err(e) = consumer.store_offset(topic, partition, offset) {
            metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
            error!("failed to mark offset on raw queue: {}", e);
        }
    }

    // Wait for message processing to finish before exiting
    if let Err(e) = db_task.await {
        error!("Failed to join kvkafka DB task: {}", e);
    }

    info!("Legacy raw ingestion terminated");

    Ok(())
}
