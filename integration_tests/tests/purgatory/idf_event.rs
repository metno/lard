use chrono::{DateTime, Duration, TimeZone, Utc};
use lard_egress::reports::IdfEventAvailable;
use rdkafka::producer::FutureProducer;
use reqwest::Client;

use lard_egress::{
    patchwork::PatchworkTables,
    reports::{DEFAULT_DURATIONS, IdfEvent, IdfEventAvailabilityResp, IdfEventResp},
};
use util::{DbPools, mock::auth::bearer::create_mock_jwt};

pub mod common;
use common::{
    Param, TestData,
    legacy::{IngestData, ingest_raw},
};

#[ignore]
#[tokio::test]
async fn test_idf_event_availability() {
    // Message priority default times
    let priority_switch: DateTime<Utc> = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();

    let token_permitid59 = create_mock_jwt(vec![
        "read-permitid-5".to_string(),
        "read-permitid-9".to_string(),
    ])
    .unwrap_or_default();

    // Timeseries start time
    let start_time = Utc.with_ymd_and_hms(2024, 12, 31, 23, 50, 0).unwrap();
    let cases = [
        (
            Some(token_permitid59.as_str()),
            IdfEventAvailabilityResp {
                stations: vec![
                    IdfEventAvailable::new(10001, 1, start_time, Some(priority_switch)),
                    IdfEventAvailable::new(20001, 1, priority_switch, None),
                    IdfEventAvailable::new(99995, 5, priority_switch, None),
                ],
            },
        ),
        (
            None,
            IdfEventAvailabilityResp {
                stations: vec![
                    IdfEventAvailable::new(10001, 1, start_time, Some(priority_switch)),
                    IdfEventAvailable::new(20001, 1, priority_switch, None),
                ],
            },
        ),
    ];

    e2e_test_wrapper_legacy(
        &["RR_01"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let test_data = IngestData::new(vec![
                TestData {
                    station_id: 10001,
                    params: vec![Param::new("RR_01")],
                    start_time,
                    period: Duration::minutes(1),
                    type_id: 514,
                    len: 20,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("RR_01")],
                    start_time,
                    period: Duration::minutes(1),
                    type_id: 508,
                    len: 20,
                },
                TestData {
                    station_id: 99995,
                    params: vec![Param::new("RR_01")],
                    start_time,
                    period: Duration::minutes(1),
                    type_id: 508,
                    len: 20,
                },
            ]);

            ingest_raw(&test_data, producer, db_pools, tables).await;

            let url = "http://localhost:3000/reports/idf/event";

            for (token, expected) in cases {
                let client = Client::new();
                let request = match token {
                    Some(t) => client.get(url).bearer_auth(t),
                    None => client.get(url),
                };

                let resp = request.send().await.unwrap();
                assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

                let mut json: IdfEventAvailabilityResp = resp.json().await.unwrap();

                // Sort so that the stations are in order
                json.stations.sort_by_key(|s| s.station_id);

                assert_eq!(json, expected);
            }
        },
    )
    .await
}

#[ignore]
#[tokio::test]
async fn test_idf_event() {
    let start_time = Utc.with_ymd_and_hms(2024, 12, 31, 23, 40, 0).unwrap();
    let end_first_ts = Utc.with_ymd_and_hms(2024, 12, 31, 23, 49, 0).unwrap();
    let end_second_ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 9, 0).unwrap();

    let station_id = 10001;
    let test_data = IngestData::new(vec![
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 514,
            len: 30,
        },
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 508,
            len: 30,
        },
    ]);

    let cases = [
        (
            // Only extract 2 timestamps for simplicity
            "default durations",
            start_time,
            start_time + Duration::minutes(2),
            None,
            // Skip the first element (duration = 1), since that one can only return a single
            // timestamp
            vec![IdfEvent::new(1.0, 1, start_time, start_time)]
                .into_iter()
                .chain(
                    // All durations > 1 should return the same intensity and timestamps
                    DEFAULT_DURATIONS[1..].iter().map(|d| {
                        IdfEvent::new(2.0, *d, start_time, start_time + Duration::minutes(1))
                    }),
                )
                .collect(),
        ),
        (
            // Should only get the first timeseries
            "single duration",
            start_time,
            start_time + Duration::minutes(10),
            Some(vec![10]),
            vec![IdfEvent::new(10.0, 10, start_time, end_first_ts)],
        ),
        (
            // Should get both timeseries
            "multiple durations",
            start_time,
            start_time + Duration::minutes(50),
            Some(vec![10, 40]),
            vec![
                IdfEvent::new(10.0, 10, start_time, end_first_ts),
                IdfEvent::new(30.0, 40, start_time, end_second_ts),
            ],
        ),
    ];

    e2e_test_wrapper_legacy(
        &["RR_01"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            ingest_raw(&test_data, producer, db_pools.clone(), tables).await;

            // HACK: need to set corrected and quality_code to be able to compute idf event
            let open_conn = db_pools.open.get().await.unwrap();
            open_conn
                .execute(
                    "UPDATE legacy.data SET corrected = 1.0, quality_code = 1",
                    &[],
                )
                .await
                .unwrap();

            for (title, from, to, durations, expected) in cases {
                let duration_query = durations
                    .map(|v| {
                        let joined = v
                            .iter()
                            .map(|d| d.to_string())
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("&durations={joined}")
                    })
                    .unwrap_or("".to_string());

                let url = format!(
                    "http://localhost:3000/reports/idf/event/{station_id}\
                        ?fromtime={from}\
                        &totime={to}\
                        {duration_query}",
                );

                let resp = reqwest::get(url).await.unwrap();
                assert!(
                    resp.status().is_success(),
                    "{title}: {}",
                    resp.text().await.unwrap()
                );

                let json: IdfEventResp = resp.json().await.unwrap();
                assert_eq!(json.station_id, station_id, "{title}");
                assert_eq!(json.values.len(), expected.len(), "{title}");

                for (val, exp) in json.values.into_iter().zip(expected) {
                    assert_eq!(val, exp, "{title}")
                }
            }
        },
    )
    .await
}

#[ignore]
#[tokio::test]
async fn test_idf_event_failure() {
    let start_time = Utc.with_ymd_and_hms(2024, 12, 31, 23, 40, 0).unwrap();
    let end_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 9, 0).unwrap();

    let station_id = 10001;
    let test_data = IngestData::new(vec![
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 514,
            len: 30,
        },
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 508,
            len: 30,
        },
    ]);

    e2e_test_wrapper_legacy(
        &["RR_01"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            ingest_raw(&test_data, producer, db_pools, tables).await;

            let url = format!(
                "http://localhost:3000/reports/idf/event/{station_id}\
                        ?fromtime={start_time}\
                        &totime={end_time}"
            );

            let resp = reqwest::get(url).await.unwrap();

            // Since the data is not QCed we won't get a positive response (not found error)
            assert!(
                resp.status().is_client_error(),
                "{}",
                resp.text().await.unwrap()
            );
        },
    )
    .await
}

#[ignore]
#[tokio::test]
async fn test_idf_event_restricted() {
    let start_time = Utc.with_ymd_and_hms(2024, 12, 31, 23, 40, 0).unwrap();
    let station_id = 99995;
    let token_permitid59 = create_mock_jwt(vec![
        "read-permitid-5".to_string(),
        "read-permitid-9".to_string(),
    ])
    .unwrap_or_default();

    let test_data = IngestData::new(vec![
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 514,
            len: 30,
        },
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 508,
            len: 30,
        },
    ]);

    let cases = [(
        // Should only get the first timeseries
        "single duration",
        start_time,
        start_time + Duration::minutes(10),
        "10",
        vec![IdfEvent::new(
            10.0,
            10,
            start_time,
            start_time + Duration::minutes(9),
        )],
    )];

    e2e_test_wrapper_legacy(
        &["RR_01"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            ingest_raw(&test_data, producer, db_pools.clone(), tables).await;

            // HACK: need to set corrected and quality_code to be able to compute idf event
            let conn = db_pools.restricted.get().await.unwrap();
            conn.execute(
                "UPDATE legacy.data SET corrected = 1.0, quality_code = 1",
                &[],
            )
            .await
            .unwrap();

            for (title, from, to, duration, expected) in cases {
                let url = format!(
                    "http://localhost:3000/reports/idf/event/{station_id}\
                    ?fromtime={from}\
                    &totime={to}\
                    &durations={duration}",
                );

                let resp = Client::new()
                    .get(url)
                    .bearer_auth(token_permitid59.clone())
                    .send()
                    .await
                    .unwrap();
                assert!(
                    resp.status().is_success(),
                    "{title}: {}",
                    resp.text().await.unwrap()
                );

                let json: IdfEventResp = resp.json().await.unwrap();
                assert_eq!(json.station_id, station_id, "{title}");
                assert_eq!(json.values.len(), expected.len(), "{title}");

                for (val, exp) in json.values.into_iter().zip(expected) {
                    assert_eq!(val, exp, "{title}")
                }
            }
        },
    )
    .await
}

// NOTE: this test will fail if the patches in the patchwork table are not sorted
#[ignore]
#[tokio::test]
async fn test_idf_event_sorted() {
    let start_time = Utc.with_ymd_and_hms(2024, 12, 31, 22, 50, 0).unwrap();
    let priority_shift_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let station_id = 10001;

    let test_data = IngestData::new(vec![
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 514,
            len: 80,
        },
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 508,
            len: 80,
        },
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 501,
            len: 80,
        },
    ]);

    let cases = [(
        "single duration",
        priority_shift_time - Duration::minutes(3),
        priority_shift_time + Duration::minutes(1),
        "4",
        vec![IdfEvent::new(
            // We have three timeseries in three different patches (delimited by |)
            // | 1 1 1 ... | ... 1 \ 1 1 1 | 2 \ ... |
            // We are asking data in the interval delimited by \
            // If the patches are not sorted, the sum accumulation would stop before the first
            // observation with value 2
            5.0,
            4,
            priority_shift_time - Duration::minutes(3),
            priority_shift_time,
        )],
    )];

    e2e_test_wrapper_legacy(
        &["RR_01"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            ingest_raw(&test_data, producer, db_pools.clone(), tables).await;

            // HACK: need to set corrected and quality_code to be able to compute idf event
            let conn = db_pools.open.get().await.unwrap();
            conn.execute(
                "UPDATE legacy.data SET corrected = 1.0, quality_code = 1 WHERE obstime < $1",
                &[&priority_shift_time],
            )
            .await
            .unwrap();

            // Set a different value for the second timeseries
            conn.execute(
                "UPDATE legacy.data SET corrected = 2.0, quality_code = 1 WHERE obstime >= $1",
                &[&priority_shift_time],
            )
            .await
            .unwrap();

            for (title, from, to, duration, expected) in cases {
                let url = format!(
                    "http://localhost:3000/reports/idf/event/{station_id}\
                    ?fromtime={from}\
                    &totime={to}\
                    &durations={duration}",
                );

                let resp = Client::new().get(url).send().await.unwrap();
                assert!(
                    resp.status().is_success(),
                    "{title}: {}",
                    resp.text().await.unwrap()
                );

                let json: IdfEventResp = resp.json().await.unwrap();
                assert_eq!(json.station_id, station_id, "{title}");
                assert_eq!(json.values.len(), expected.len(), "{title}");

                for (val, exp) in json.values.into_iter().zip(expected) {
                    assert_eq!(val, exp, "{title}")
                }
            }
        },
    )
    .await
}
