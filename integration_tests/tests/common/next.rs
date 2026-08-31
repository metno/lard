use chrono::{Duration, TimeZone, Utc};

use lard_egress::{LatestResp, TimeseriesResp, TimesliceResp, timeseries::Timeseries};
use lard_ingestion::KldataResp;

use super::{Param, TestData};

async fn ingest_data(client: &reqwest::Client, obsinn_msg: String) -> KldataResp {
    let resp = client
        .post("http://localhost:3001/kldata")
        .body(obsinn_msg)
        .send()
        .await
        .unwrap();

    resp.json().await.unwrap()
}

// TODO: custom sensor and level?
pub async fn ensure_next_ingestion_and_stations_irregular() {
    let ts = TestData {
        station_id: 30001,
        params: vec![Param::new("TGM"), Param::new("KLOBS")],
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
            "http://localhost:3000/station/{}/param/{}",
            ts.station_id, param.id
        );
        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: TimeseriesResp = resp.json().await.unwrap();
        assert_eq!(json.tseries.len(), 1);

        let Timeseries::Irregular(series) = &json.tseries[0] else {
            panic!("Expected irrregular timeseries")
        };

        // FIXME: klobs not detected for now because stations endpoint
        // doesn't query nonscalar_data
        if param.id == 222 {
            assert_eq!(series.data.len(), ts.len);
        }
    }

    eprintln!("next_ingestion_and_stations_irregular ok");
}

pub async fn ensure_stations_endpoint_regular() {
    // TA, TGX, KLOBS
    let params = [211, 225, 1022];
    let station = 20001;
    let resolution = "PT1H";
    let end_time = "2024-01-01T11:59:59Z";

    for param in params {
        let url = format!(
            "http://localhost:3000/station/{}/param/{}?time_resolution={}&end_time={}",
            station, param, resolution, end_time
        );
        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: TimeseriesResp = resp.json().await.unwrap();
        assert_eq!(json.tseries.len(), 1);

        let Timeseries::Regular(series) = &json.tseries[0] else {
            panic!("Expected regular timeseries")
        };
        assert_eq!(series.data.len(), 12);
    }

    eprintln!("stations_endpoint_regular ok");
}

pub async fn ensure_stations_endpoint_errors() {
    let cases = vec![
        //missing station
        (99999, 211),
        //missing param
        (20001, 999),
    ];

    for (station_id, param_id) in cases {
        let url = format!("http://localhost:3000/station/{station_id}/param/{param_id}");
        let resp = reqwest::get(url).await.unwrap();
        // TODO: resp.status() returns 500, maybe it should return 404?
        assert!(!resp.status().is_success());
    }

    eprintln!("stations_endpoint_errors ok");
}

pub async fn ensure_latest_endpoint() {
    let cases = vec![
        // without query (defaults to (now - 3h))
        ("", 0),
        // latest max age 1
        ("?latest_max_age=2021-01-01T00:00:00Z", 2),
        // latest max age 2
        ("?latest_max_age=2019-01-01T00:00:00Z", 3),
    ];
    for (query, n_timeseries_found) in cases {
        let url = format!("http://localhost:3000/latest{query}");
        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: LatestResp = resp.json().await.unwrap();
        assert_eq!(json.data.len(), n_timeseries_found);
    }

    eprintln!("latest_endpoint ok");
}

pub async fn ensure_timeslice_endpoint() {
    let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap();
    let param = 211; //TA

    let url = format!(
        "http://localhost:3000/timeslice/{}/param/{}",
        timestamp, param
    );

    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success());

    let json: TimesliceResp = resp.json().await.unwrap();
    assert!(json.tslices.len() == 1);

    let slice = &json.tslices[0];
    assert_eq!(slice.param_id, param);
    assert_eq!(slice.timestamp, timestamp);
    assert_eq!(slice.data.len(), 1);

    for data in slice.data.iter() {
        assert!([20001].contains(&data.station_id));
    }

    eprintln!("timeslice_endpoint ok");
}
