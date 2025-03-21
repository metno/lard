# Ingestion

Ingestion is broken down into a set of steps that should apply generically to all sources we ingest from:

1. Receiving: This can take a variety of forms. As concrete examples we have a Kafka consumer that reads messages from a queue, and an HTTP server that receives requests with data in a text format defined by Obsinn. To support a new data source, you should spawn a task in the main function that runs the appropriate reciever, and has a handler to pass the data through the remaining steps

2. Parsing: The raw message format may need some parsing to put it into a tractable form that reflects the structure of its source-specific [label](/docs/DATABASE.md#Labels)

3. Labelling: The parsed data need to be divided into open and restricted sets, and tagged with timeseries ID's determined using their source-specific label. If there is no matching source-specific label for a datum, a new timeseries should be created for it, along with source-specific and lookup labels in a transaction. Once labeled, the data should be in a provided generic format for insertion:
```rust
pub struct Datum<'a> {
    timeseries_id: i64,
    // needed for QC
    param_id: i32,
    value: ObsType<'a>,
    qc_usable: bool,
}

/// Generic container for a piece of data ready to be inserted into the DB
pub struct DataChunk<'a> {
    timestamp: DateTime<Utc>,
    time_resolution: Option<chronoutil::RelativeDuration>,
    data: Vec<Datum<'a>>,
}
```

4. Inserting: The data then needs to be QCed and inserted into the database. As it is already in a generic format, we provide a function that can be used for this purpose `qc_and_insert_data`

An example of this coming together in an HTTP handler:
```rust
async fn handle_kldata(
    State(pools): State<DbPools>,
    State(param_conversions): State<ParamConversions>,
    State(permit_table): State<Arc<RwLock<(ParamPermitTable, StationPermitTable)>>>,
    State(rove_connector): State<Arc<rove_connector::Connector>>,
    State(qc_pipelines): State<Arc<HashMap<(i32, RelativeDuration), rove::Pipeline>>>,
    body: String,
) -> Json<KldataResp> {
    metrics::counter!(KLDATA_MESSAGES_RECEIVED).increment(1);

    let result: Result<usize, Error> = async {
        let mut open_conn = pools.open.get().await?;
        let mut restricted_conn = pools.restricted.get().await?;

        let (message_id, obsinn_chunk) = parse_kldata(&body, param_conversions.clone())?;

        let (mut open_data, mut restricted_data) = filter_and_label_kldata(
            obsinn_chunk,
            &mut open_conn,
            &mut restricted_conn,
            param_conversions,
            permit_table,
        )
        .await?;

        qc_and_insert_data(
            &mut open_data,
            &rove_connector,
            &qc_pipelines,
            &mut open_conn,
        )
        .await?;
        qc_and_insert_data(
            &mut restricted_data,
            &rove_connector,
            &qc_pipelines,
            &mut restricted_conn,
        )
        .await?;

        Ok(message_id)
    }
    .await;

    match result {
        Ok(message_id) => Json(KldataResp {
            message: "".into(),
            message_id,
            res: 0,
            retry: false,
        }),
        Err(e) => {
            metrics::counter!(KLDATA_FAILURES).increment(1);
            error!("failed to ingest kldata message: {}, body: {}", e, body);
            // TODO: log errors?
            Json(KldataResp {
                message: e.to_string(),
                message_id: 0, // TODO: some clever way to get the message id still if possible?
                res: 1,
                retry: !matches!(e, Error::Parse(_)),
            })
        }
    }
}
```

## Kafka and legacy

Right now as we aren't ready with confident QC, we are ingesting from KvKafka into the `legacy.data` table that has a different structure. As a result, that ingestor has it's own equivalent to `Datum` and `insert_data` that match `legacy.data`.
