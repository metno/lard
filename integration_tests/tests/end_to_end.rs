use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Duration, DurationRound, TimeDelta, TimeZone, Utc};
use chronoutil::RelativeDuration;
use rove::data_switch::{DataConnector, SpaceSpec, TimeSpec, Timestamp};
use tokio_postgres::NoTls;

use lard_egress::{timeseries::Timeseries, LatestResp, TimeseriesResp, TimesliceResp};
use lard_ingestion::{util::tsupdate::set_deactivated, KldataResp};

pub mod common;
use common::{e2e_test_wrapper, mocks::MetadataMock, Param, TestData};
use util::PooledPgConn;

async fn ingest_data(client: &reqwest::Client, obsinn_msg: String) -> KldataResp {
    let resp = client
        .post("http://localhost:3001/kldata")
        .body(obsinn_msg)
        .send()
        .await
        .unwrap();

    resp.json().await.unwrap()
}

#[tokio::test]
async fn test_stations_endpoint_irregular() {
    e2e_test_wrapper(async |_| {
        let ts = TestData {
            station_id: 20001,
            params: vec![Param::new("TGM"), Param::new("TGX")],
            start_time: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            period: Duration::hours(1),
            type_id: 501,
            len: 48,
        };

        let client = reqwest::Client::new();
        let ingestor_resp = ingest_data(&client, ts.obsinn_zeros()).await;
        assert_eq!(ingestor_resp.res, 0);

        for param in ts.params {
            let url = format!(
                "http://localhost:3000/stations/{}/params/{}",
                ts.station_id, param.id
            );
            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: TimeseriesResp = resp.json().await.unwrap();
            assert_eq!(json.tseries.len(), 1);

            let Timeseries::Irregular(series) = &json.tseries[0] else {
                panic!("Expected irrregular timeseries")
            };

            assert_eq!(series.data.len(), ts.len);
        }
    })
    .await
}

#[tokio::test]
async fn test_stations_endpoint_regular() {
    let cases = vec![
        // Scalar params
        TestData {
            station_id: 20001,
            params: vec![Param::new("TA"), Param::new("TGX")],
            start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap()
                - Duration::hours(11),
            period: Duration::hours(1),
            type_id: 501,
            len: 12,
        },
        // TODO: probably write a separate test, so we can check actual sensor and level
        // With sensor and level
        TestData {
            station_id: 20001,
            params: vec![Param::with_sensor_level("TA", (1, 1)), Param::new("TGX")],
            start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap()
                - Duration::hours(11),
            period: Duration::hours(1),
            type_id: 501,
            len: 12,
        },
        // Scalar and non-scalar
        TestData {
            station_id: 20001,
            params: vec![Param::new("KLOBS"), Param::new("TA")],
            start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap()
                - Duration::hours(11),
            period: Duration::hours(1),
            type_id: 501,
            len: 12,
        },
    ];

    for ts in cases {
        e2e_test_wrapper(async |_| {
            let client = reqwest::Client::new();
            let ingestor_resp = ingest_data(&client, ts.obsinn_zeros()).await;
            assert_eq!(ingestor_resp.res, 0);

            let resolution = "PT1H";
            for param in ts.params {
                let url = format!(
                    "http://localhost:3000/stations/{}/params/{}?time_resolution={}",
                    ts.station_id, param.id, resolution
                );
                let resp = reqwest::get(url).await.unwrap();
                assert!(resp.status().is_success());

                let json: TimeseriesResp = resp.json().await.unwrap();
                assert_eq!(json.tseries.len(), 1);

                let Timeseries::Regular(series) = &json.tseries[0] else {
                    panic!("Expected regular timeseries")
                };
                assert_eq!(series.data.len(), ts.len);
            }
        })
        .await
    }
}

// TODO: we should implement an availability endpoint?
async fn get_totime(conn: &PooledPgConn<'_>) -> Vec<Option<DateTime<Utc>>> {
    conn.query(
        "SELECT timeseries.totime FROM timeseries \
        JOIN labels.met \
            ON timeseries.id = met.timeseries \
        ORDER BY station_id",
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| row.get(0))
    .collect()
}

#[tokio::test]
async fn test_totime_update() {
    e2e_test_wrapper(async |db_pools| {
        let timeseries = vec![
            // Scalar and non-scalar
            TestData {
                station_id: 10001,
                params: vec![Param::new("KLOBS"), Param::new("TA")],
                start_time: Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 503,
                len: 12,
            },
            TestData {
                station_id: 20001,
                params: vec![Param::new("TA")],
                start_time: Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 501,
                len: 12,
            },
        ];

        // test "untwisting" from / to
        let fromtime = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let totime = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();

        let metadata_mock = MetadataMock {
            station: 10001,
            fromtime,
            totime,
        };

        let expected = vec![
            // Both timeseries on station 10001 should be deactivated
            Some(totime),
            Some(totime),
            // timeseries on station 20001 is not
            None,
        ];

        for ts in timeseries {
            let client = reqwest::Client::new();
            let ingestor_resp = ingest_data(&client, ts.obsinn_zeros()).await;
            assert_eq!(ingestor_resp.res, 0);
        }

        let mut conn = db_pools.open.get().await.unwrap();

        // totimes should be empty
        for totime in get_totime(&conn).await {
            assert_eq!(totime, None);
        }

        let (station_fromtime, station_totime, obs_pgm_fromtime, obs_pgm_totime) =
            metadata_mock.cache_deactivated_stinfosys().await.unwrap();

        set_deactivated(
            &mut conn,
            &obs_pgm_fromtime,
            &obs_pgm_totime,
            &station_fromtime,
            &station_totime,
        )
        .await
        .unwrap();

        let after = get_totime(&conn).await;

        // Now the totime for station 10001 should be set
        for (totime, end_time) in after.into_iter().zip(expected) {
            assert_eq!(totime, end_time);
        }
    })
    .await
}

#[tokio::test]
async fn test_stations_endpoint_errors() {
    let cases = vec![
        //missing station
        (99999, 211),
        //missing param
        (20001, 999),
    ];

    for (station_id, param_id) in cases {
        e2e_test_wrapper(async |_| {
            let ts = TestData {
                station_id: 20001,
                params: vec![Param::new("TA")],
                start_time: Utc.with_ymd_and_hms(2024, 1, 1, 00, 00, 00).unwrap(),
                period: Duration::hours(1),
                type_id: 501,
                len: 48,
            };

            let client = reqwest::Client::new();
            let ingestor_resp = ingest_data(&client, ts.obsinn_zeros()).await;
            assert_eq!(ingestor_resp.res, 0);

            for _ in ts.params {
                let url = format!("http://localhost:3000/stations/{station_id}/params/{param_id}");
                let resp = reqwest::get(url).await.unwrap();
                // TODO: resp.status() returns 500, maybe it should return 404?
                assert!(!resp.status().is_success());
            }
        })
        .await
    }
}

// We insert 4 timeseries, 2 with new data (UTC::now()) and 2 with old data (2020)
#[tokio::test]
async fn test_latest_endpoint() {
    let cases = vec![
        // without query
        ("", 2),
        // latest max age 1
        ("?latest_max_age=2021-01-01T00:00:00Z", 2),
        // latest max age 2
        ("?latest_max_age=2019-01-01T00:00:00Z", 4),
    ];
    for (query, n_timeseries_found) in cases {
        e2e_test_wrapper(async |_| {
            let test_data = [
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TA"), Param::new("TGX")],
                    start_time: Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap()
                        - Duration::hours(3),
                    period: Duration::minutes(1),
                    type_id: 508,
                    len: 180,
                },
                TestData {
                    station_id: 20002,
                    params: vec![Param::new("TA"), Param::new("TGX")],
                    start_time: Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
                    period: Duration::minutes(1),
                    type_id: 508,
                    len: 180,
                },
            ];

            let client = reqwest::Client::new();
            for ts in test_data {
                let ingestor_resp = ingest_data(&client, ts.obsinn_zeros()).await;
                assert_eq!(ingestor_resp.res, 0);
            }

            let url = format!("http://localhost:3000/latest{query}");
            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: LatestResp = resp.json().await.unwrap();
            assert_eq!(json.data.len(), n_timeseries_found);
        })
        .await
    }
}

#[tokio::test]
async fn test_timeslice_endpoint() {
    e2e_test_wrapper(async |_| {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap();
        let params = vec![Param::new("TA")];

        let test_data = [
            TestData {
                station_id: 20001,
                params: params.clone(),
                start_time: timestamp - Duration::hours(1),
                period: Duration::hours(1),
                type_id: 501,
                len: 2,
            },
            TestData {
                station_id: 20002,
                params: params.clone(),
                start_time: timestamp - Duration::hours(1),
                period: Duration::minutes(1),
                type_id: 508,
                len: 120,
            },
        ];

        let client = reqwest::Client::new();
        for ts in &test_data {
            let ingestor_resp = ingest_data(&client, ts.obsinn_zeros()).await;
            assert_eq!(
                ingestor_resp.res, 0,
                "ingestor_resp.message: {}",
                ingestor_resp.message
            );
        }

        for param in &params {
            let url = format!(
                "http://localhost:3000/timeslices/{}/params/{}",
                timestamp, param.id
            );

            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: TimesliceResp = resp.json().await.unwrap();
            assert!(json.tslices.len() == 1);

            let slice = &json.tslices[0];
            assert_eq!(slice.param_id, param.id);
            assert_eq!(slice.timestamp, timestamp);
            assert_eq!(slice.data.len(), test_data.len());

            for (data, ts) in slice.data.iter().zip(&test_data) {
                assert_eq!(data.station_id, ts.station_id);
            }
        }
    })
    .await
}

#[tokio::test]
async fn test_rove_connector() {
    let ts = TestData {
        station_id: 20001,
        params: vec![Param::new("TA"), Param::new("TGX")],
        start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap() - Duration::hours(11),
        period: Duration::hours(1),
        type_id: 501,
        len: 12,
    };

    e2e_test_wrapper(async |_| {
        let client = reqwest::Client::new();

        let manager = PostgresConnectionManager::new_from_stringlike(
            std::env::var("LARD_CONN_STRING").unwrap(),
            NoTls,
        )
        .unwrap();
        let pool = bb8::Pool::builder().build(manager).await.unwrap();
        let connector = rove_connector::Connector { pool };

        let ingestor_resp = ingest_data(&client, ts.obsinn_zeros()).await;
        assert_eq!(ingestor_resp.res, 0);

        let resolution = "PT1H";
        for param in ts.params {
            let url = format!(
                "http://localhost:3000/stations/{}/params/{}?time_resolution={}",
                ts.station_id, param.id, resolution
            );
            let resp = reqwest::get(url).await.unwrap();

            let json: TimeseriesResp = resp.json().await.unwrap();

            let Timeseries::Regular(series) = &json.tseries[0] else {
                panic!("Expected regular timeseries")
            };

            // feels kinda silly we had to use the API just to get the ts_id, but what can you do?
            let ts_id = series.header.ts_id.to_string();

            let data_cache_single = connector
                .fetch_data(
                    &SpaceSpec::One(ts_id.clone()),
                    &TimeSpec::new(
                        Timestamp(ts.start_time.timestamp()),
                        Timestamp((ts.start_time + Duration::hours(2)).timestamp()),
                        RelativeDuration::hours(1),
                    ),
                    1,
                    1,
                    None,
                )
                .await
                .unwrap();

            assert_eq!(
                data_cache_single.data,
                vec![rove::data_switch::Timeseries {
                    tag: ts_id.clone(),
                    values: vec![None, Some(0.), Some(0.), Some(0.), Some(0.)]
                }],
            );
            assert_eq!(
                data_cache_single.start_time,
                Timestamp(ts.start_time.timestamp())
            );
            assert_eq!(data_cache_single.period, RelativeDuration::hours(1));
            assert_eq!(data_cache_single.num_leading_points, 1);
            assert_eq!(data_cache_single.num_trailing_points, 1);

            let data_cache_all = connector
                .fetch_data(
                    &SpaceSpec::All,
                    &TimeSpec::new(
                        Timestamp(ts.start_time.timestamp()),
                        Timestamp((ts.start_time + Duration::hours(2)).timestamp()),
                        RelativeDuration::hours(1),
                    ),
                    1,
                    1,
                    // TODO: this should probably go in SpaceSpec::All?
                    Some(&param.id.to_string()),
                )
                .await
                .unwrap();

            assert_eq!(
                data_cache_all.data,
                // vec![rove::data_switch::Timeseries {
                //     tag: ts_id,
                //     values: vec![None, Some(0.), Some(0.), Some(0.), Some(0.)]
                // }],
                // TODO: replace below with above when we fix the location situation
                vec![],
            );
            assert_eq!(
                data_cache_all.start_time,
                Timestamp(ts.start_time.timestamp())
            );
            assert_eq!(data_cache_all.period, RelativeDuration::hours(1));
            assert_eq!(data_cache_all.num_leading_points, 1);
            assert_eq!(data_cache_all.num_trailing_points, 1);
        }
    })
    .await
}
