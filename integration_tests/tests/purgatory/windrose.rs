use chrono::{Duration, TimeZone, Utc};
use rdkafka::producer::FutureProducer;
use reqwest::Client;

use ::util::{DbPools, mock::auth::bearer::create_mock_jwt};
use lard_egress::{
    patchwork::PatchworkTables,
    reports::{WindCategories, WindroseAvailabilityResp, WindroseAvailable, WindroseResp},
};

pub mod common;
use common::{
    Param, TestData,
    legacy::{IngestData, ingest_raw},
};

struct ExpectedWindrose {
    x_sum: Vec<f64>,
    y_sum: Vec<f64>,
    hist: Vec<Vec<f64>>,
    category: WindCategories,
}

fn is_close(a: f64, b: f64) -> bool {
    const DELTA: f64 = 1e-6;
    (a - b).abs() < DELTA
}

fn assert_values_and_sums(resp: WindroseResp, expected: ExpectedWindrose) {
    resp.windrose
        .speed_hist
        .into_iter()
        .zip(expected.x_sum)
        .for_each(|(val, exp)| assert!(is_close(val, exp), "{val} {exp}"));

    resp.windrose
        .direction_hist
        .iter()
        .zip(expected.y_sum)
        .for_each(|(val, exp)| assert!(is_close(*val, exp), "{val} {exp}"));

    resp.windrose
        .hist
        .iter()
        .zip(expected.hist)
        .for_each(|(x, x_exp)| {
            x.iter()
                .zip(x_exp)
                .for_each(|(val, exp)| assert!(is_close(*val, exp), "{val} {exp}"));
        });

    assert!(is_close(
        resp.windrose.wind_categories.silent_wind,
        expected.category.silent_wind
    ));

    assert!(is_close(
        resp.windrose.wind_categories.variable_wind,
        expected.category.variable_wind
    ));
}

#[ignore]
#[tokio::test]
async fn test_windrose() {
    let start_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let to_time = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
    let y_bins = 16;

    let token_permitid59 = create_mock_jwt(vec![
        "read-permitid-5".to_string(),
        "read-permitid-9".to_string(),
    ])
    .unwrap_or_default();

    let cases = [(
        Some(token_permitid59.as_str()),
        start_time,
        to_time,
        ExpectedWindrose {
            x_sum: vec![25., 0., 0., 0., 0., 0., 0., 0., 25., 0., 0., 25.],

            y_sum: vec![
                0., 50., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 25.,
            ],
            hist: vec![
                vec![
                    0., 25., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                ],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![
                    0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 25.,
                ],
                vec![0.; y_bins],
                vec![0.; y_bins],
                vec![
                    0., 25., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
                ],
            ],
            category: WindCategories::new(25.0, 0.0),
        },
    )];

    e2e_test_wrapper_legacy(
        &["FF", "DD"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let station_id = 10001;
            let test_data = IngestData::new(vec![TestData {
                station_id,
                params: vec![
                    // wind speed
                    Param::new("FF").with_values(vec![0.1, 0.4, 21.0, 37.0]),
                    // wind direction
                    Param::new("DD").with_values(vec![220.0, 30.0, 330.0, 15.0]),
                ],
                start_time,
                period: Duration::hours(1),
                type_id: 501,
                len: 4,
            }]);

            ingest_raw(&test_data, producer, db_pools.clone(), tables.clone()).await;

            // HACK: need to set corrected and quality_code to be able to compute windrose
            let open_conn = db_pools.open.get().await.unwrap();
            open_conn
                .execute(
                    "UPDATE legacy.data SET corrected = original, quality_code = 1",
                    &[],
                )
                .await
                .unwrap();

            for (token, from, to, expected) in cases {
                let url = format!(
                    "http://localhost:3000/reports/windrose/{station_id}\
                    ?fromtime={from}\
                    &totime={to}"
                );

                let client = Client::new();
                let request = match token {
                    Some(t) => client.get(&url).bearer_auth(t),
                    None => client.get(&url),
                };

                let resp = request.send().await.unwrap();
                assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

                let json: WindroseResp = resp.json().await.unwrap();

                assert_values_and_sums(json, expected);
            }
        },
    )
    .await
}

#[ignore]
#[tokio::test]
async fn test_windrose_availability() {
    let start_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();

    let test_data = IngestData::new(vec![
        TestData {
            station_id: 10001,
            params: vec![
                // wind speed
                Param::new("FF"),
                // wind direction
                Param::new("DD"),
            ],
            start_time,
            period: Duration::hours(1),
            type_id: 501,
            len: 20,
        },
        TestData {
            station_id: 99995,
            params: vec![
                // wind speed
                Param::new("FF"),
                // wind direction
                Param::new("DD"),
            ],
            start_time,
            period: Duration::hours(1),
            type_id: 501,
            len: 30,
        },
        TestData {
            station_id: 10002,
            params: vec![
                // only wind speed, therefore it can't show up in available
                Param::new("FF"),
            ],
            start_time,
            period: Duration::hours(1),
            type_id: 501,
            len: 20,
        },
    ]);
    let token_permitid59 = create_mock_jwt(vec![
        "read-permitid-5".to_string(),
        "read-permitid-9".to_string(),
    ])
    .unwrap_or_default();

    let cases = [
        (
            Some(token_permitid59.as_str()),
            WindroseAvailabilityResp {
                stations: vec![
                    WindroseAvailable::new(10001, 1, start_time, None),
                    WindroseAvailable::new(99995, 5, start_time, None),
                ],
            },
        ),
        (
            None,
            WindroseAvailabilityResp {
                stations: vec![WindroseAvailable::new(10001, 1, start_time, None)],
            },
        ),
    ];

    e2e_test_wrapper_legacy(
        &["FF", "DD"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            ingest_raw(&test_data, producer, db_pools.clone(), tables.clone()).await;

            for (token, expected) in cases {
                let url = "http://localhost:3000/reports/windrose";

                let client = Client::new();
                let request = match token {
                    Some(t) => client.get(url).bearer_auth(t),
                    None => client.get(url),
                };

                let resp = request.send().await.unwrap();
                assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

                let json: WindroseAvailabilityResp = resp.json().await.unwrap();
                assert_eq!(json, expected);
            }
        },
    )
    .await
}
