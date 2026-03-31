use chrono::{DateTime, Duration, DurationRound, TimeDelta, TimeZone, Utc};
use rdkafka::producer::FutureProducer;

use lard_egress::{
    LatestResp, TimeseriesResp, TimesliceResp, patchwork::PatchworkTables, timeseries::Timeseries,
};
use lard_ingestion::KldataResp;
use util::{
    DbPools, PooledPgConn,
    stinfofacade::{self, from_to_time::update_from_to},
};

pub mod common;
use common::{
    Param, TestData, e2e_test_wrapper,
    legacy::{IngestData, e2e_test_wrapper_legacy, ingest_raw},
    mocks::MetadataMock,
};

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
    e2e_test_wrapper(&["TGM", "TGX"], async |_| {
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
            params: vec![
                Param::new("TA").with_sensor_level((1, 1)),
                Param::new("TGX"),
            ],
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
        e2e_test_wrapper(&["TA", "TGX", "KLOBS"], async |_| {
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
async fn get_fromtotime(
    conn: &PooledPgConn<'_>,
) -> Vec<(Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
    conn.query(
        "SELECT timeseries.fromtime, timeseries.totime FROM timeseries \
        JOIN labels.met \
            ON timeseries.id = met.timeseries \
        ORDER BY station_id",
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

#[tokio::test]
async fn test_fromtotime_update() {
    e2e_test_wrapper_legacy(
        &["KLOBS", "TA"],
        async |producer: FutureProducer, db_pools: DbPools, patchwork_tables: PatchworkTables| {
            let timeseries = IngestData::new(vec![
                TestData {
                    station_id: 10001,
                    params: vec![Param::new("KLOBS")],
                    start_time: Utc.with_ymd_and_hms(1980, 12, 31, 12, 0, 0).unwrap(),
                    period: Duration::hours(1),
                    type_id: 503,
                    len: 14, // metadata should cut off the last part of this that goes into 1981
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TA")],
                    start_time: Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap(),
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 12,
                },
            ]);
            ingest_raw(&timeseries, producer, db_pools.clone(), patchwork_tables).await;

            let fromtime = Utc.with_ymd_and_hms(1980, 12, 1, 0, 0, 0).unwrap();
            let totime: DateTime<Utc> = Utc.with_ymd_and_hms(1981, 1, 1, 0, 0, 0).unwrap();

            let metadata_mock = MetadataMock {
                station: 10001,
                fromtime,
                totime,
            };

            let expected = vec![
                // timeseries on station 10001 should be closed based on metadata
                (
                    Some(Utc.with_ymd_and_hms(1980, 12, 31, 12, 0, 0).unwrap()),
                    Some(totime),
                ),
                // timeseries on station 20001 is not, so it is left open
                (
                    Some(Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap()),
                    None,
                ),
            ];

            let mut conn = db_pools.open.get().await.unwrap();

            // totimes should be empty
            for fromtotimes in get_fromtotime(&conn).await {
                assert_eq!(fromtotimes.1, None); // to time
            }

            let (obs_pgm_times_map, station_times_map) =
                metadata_mock.cache_closed_stinfosys().await.unwrap();

            let param_tables = stinfofacade::param::from_codes(&["TA", "KLOBS"]);

            update_from_to(
                &mut conn,
                &obs_pgm_times_map,
                &station_times_map,
                param_tables,
                tokio_util::sync::CancellationToken::new(),
            )
            .await
            .unwrap();

            let after = get_fromtotime(&conn).await;

            // Now the totime for station 10001 should be set (and the to time for station 20001 should be its first observation time)
            for (db, expect) in after.into_iter().zip(expected) {
                assert_eq!(db.0, expect.0);
                assert_eq!(db.1, expect.1);
            }
        },
    )
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
        e2e_test_wrapper(&["TA"], async |_| {
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
        e2e_test_wrapper(&["TA", "TGX"], async |_| {
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
    e2e_test_wrapper(&["TA"], async |_| {
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
