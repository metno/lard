# Integration tests

## Writing a new test

Here's a straighforward, commented example of an end-to-end test; it serves as a good starting point for writing your own test. The other tests in [/integration_tests/tests/end_to_end.rs](/integration_tests/tests/end_to_end.rs) build on this basic frame, and so serve as good examples of how to extend it.

```rust
#[tokio::test]
async fn test_example() {
    // This wrapper is discussed in detail below, sets up ingestion and egress servers
    // for the period that the closure you pass in runs
    e2e_test_wrapper(async {
        // Struct that specifies how to construct an obsinn message for the ingestor with some
        // fake data. This one makes 1 timeseries (air temperature on station 20001) starting at
        // 00:00 on 1/1/2024 consisting of 48 values spaced 1 hour apart
        let ts = TestData {
            station_id: 20001,
            params: vec![Param::new("TA")],
            start_time: Utc.with_ymd_and_hms(2024, 1, 1, 00, 00, 00).unwrap(),
            period: Duration::hours(1),
            type_id: 501,
            len: 48,
        };

        // As ingestion and egress are exposed as HTTP servers, we need an HTTP client to interact
        // with them
        let client = reqwest::Client::new();

        // Use helper function `ingest_data` to send a request with the obsinn message generated
        // by `ts` to the ingestion endpoint
        let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
        // Assert success response from ingestion
        assert_eq!(ingestor_resp.res, 0);

        // In this case we've only used a single param, so this loop isn't really necessary, but
        // I've included it to highlight that `TestData` accepts any number of params
        for param in ts.params {
            // URL to fetch a timeseries from egress based on station_id and param_id, with our
            // station and param templated in
            let url = format!(
                "http://localhost:3000/stations/{}/params/{}",
                ts.station_id, param.id
            );
            // GET that URL
            let resp = reqwest::get(url).await.unwrap();
            // Assert sucess response from egress 
            assert!(resp.status().is_success());
        }
    })
    .await
}
```

## Understanding e2e_test_wrapper

`e2e_test_wrapper` makes it easier to write integration tests by containing the boilerplate of setting up the services that comprise lard, and making them available to a closure, then cleaning up the database afterward. It does this setup by starting the HTTP servers as tokio tasks instead of independent processes:
```rust
let egress_server = tokio::spawn(async move {
    tokio::select! {
        output = lard_egress::run(api_pool, cancel_token2) => output,
        _ = init_shutdown_rx1.recv() => {
            api_shutdown_tx.send(()).unwrap();
        },
    }
});
```

It then selects across the join handles of the services, and the future of closure passed in:
```rust
tokio::select! {
    _ = egress_server => panic!("API server task terminated first"),
    _ = ingestor => panic!("Ingestor server task terminated first"),
    _ = sig_catcher => panic!("Signal catcher caught a shutdown signal"),
    // Clean up database even if test panics, to avoid test poisoning
    test_result = AssertUnwindSafe(test).catch_unwind() => {
        // For debugging a specific test, it might be useful to skip the cleanup process
        #[cfg(not(feature = "debug"))]
        for db_pool in [open_db_pool, restricted_db_pool] {
            let client = db_pool.get().await.unwrap();
            client
                .batch_execute(
                    // TODO: should clean public.timeseries_id_seq too? RESTART IDENTITY CASCADE?
                    "TRUNCATE public.timeseries, labels.met, labels.obsinn CASCADE",
                )
                .await
                .unwrap();
        }
        assert!(test_result.is_ok())
    }
}
```
Here we expect `test_result` to complete first, as the server tasks should run indefinitely unless they crash or until we tell them to shut down, so we panic and fail the test if this does not happen.

In the handler for the test closure, we make sure to clean up the database before moving on, so the next test can start on a blank slate. We make sure to catch any panics so we will perform this cleanup even if the closure fails.

It's worth noting that `e2e_test_wrapper` does not set up a postgres instance, just connects to it, so a postgres instance must be available to run these tests. To help with this, we've set up a justfile to use instead of running `cargo test` directly, which sets up a postgres instance in a docker container.
