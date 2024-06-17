use chrono::{TimeZone, Utc};
use test_case::test_case;

use lard_ingestion::{insert_data, permissions::timeseries_is_open, Data, Datum};

pub mod common;

#[tokio::test]
async fn test_insert_data() {
    let pool = common::init_db_pool().await.unwrap();
    let mut conn = pool.get().await.unwrap();

    // Timeseries ID 2 has hourly data
    let id: i32 = 2;
    let count_before_insertion = common::number_of_data_rows(&conn, id).await;

    let data: Data = (1..10)
        .map(|i| {
            let timestamp = Utc.with_ymd_and_hms(2012, 2, 16, i, 0, 0).unwrap();
            let value = 49. + i as f32;
            Datum::new(id, timestamp, value)
        })
        .collect();
    let data_len = data.len();

    insert_data(data, &mut conn).await.unwrap();

    let count_after_insertion = common::number_of_data_rows(&conn, id).await;
    let rows_inserted = count_after_insertion - count_before_insertion;

    // NOTE: The assert will fail locally if the database hasn't been cleaned up between runs
    assert_eq!(rows_inserted, data_len);
}

#[test_case(0, 0, 0 => false; "stationid not in permit_tables")]
#[test_case(3, 0, 0 => false; "stationid in StationPermitTable, timeseries closed")]
#[test_case(1, 0, 0 => false; "stationid in ParamPermitTable, timeseries closed")]
#[test_case(4, 0, 1 => true; "stationid in StationPermitTable, timeseries open")]
#[test_case(2, 0, 0 => true; "stationid in ParamPermitTable, timeseries open")]
fn test_timeseries_is_open(station_id: i32, type_id: i32, permit_id: i32) -> bool {
    let permit_tables = common::mock_permit_tables();
    timeseries_is_open(permit_tables, station_id, type_id, permit_id).unwrap()
}

#[tokio::test]
async fn test_kldata_endpoint() {
    let ingestor = common::init_ingestion_server().await;

    let test = async {
        let station_id = 12000;
        let obsinn_msg = format!(
            "kldata/nationalnr={}/type=508/messageid=23
TA,TWD(0,0)
20240607134900,25.0,18.2
20240607135100,25.1,18.3
20240607135200,25.0,18.3
20240607135300,24.9,18.1
20240607135400,25.2,18.2
20240607135500,25.1,18.2
",
            station_id
        );

        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:3001/kldata")
            .body(obsinn_msg)
            .send()
            .await
            .unwrap();

        let json: common::IngestorResponse = resp.json().await.unwrap();

        assert_eq!(json.res, 0);
        assert_eq!(json.message_id, 23)
    };

    tokio::select! {
        _ = ingestor => panic!("Server task terminated first"),
        _ = test => {}
    }
}
