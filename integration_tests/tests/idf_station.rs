use bb8_postgres::PostgresConnectionManager;
use chrono::DateTime;
use common::e2e_test_wrapper;
use lard_egress::{IdfStationAvailability, IdfStationResp};
use tokio::sync::OnceCell;
use tokio_postgres::NoTls;
pub mod common;

struct TestData {
    stations: Vec<i32>,
    // durations: Vec<i32>,
    // frequencies: Vec<i32>,
    intensity: Vec<f64>,
    lower: f64,
    upper: f64,
}

// NOTE: The e2e_test_wrapper does not truncate this data
// TODO: maybe we should not truncate the data at all and just insert once for "normal" tests too?
static TEST_DATA: OnceCell<TestData> = OnceCell::const_new();

// TODO: not ideal?
async fn init_idf_data() -> TestData {
    // NOTE: there's a bug in rust-analyzer with ToSql types, so we need to be explicit here
    // when declaring variables

    // metadata
    let stations = vec![1, 2, 3];
    let number_of_seasons = [5, 10, 15];
    let quality_class = [1, 2, 3];
    let seed_parameter: i32 = 1;
    let fromtime = DateTime::parse_from_rfc3339("2001-01-01T00:00:00Z").unwrap();
    let totime = DateTime::parse_from_rfc3339("2021-01-01T00:00:00Z").unwrap();
    let updated_at = DateTime::parse_from_rfc3339("2021-01-31T00:00:00Z").unwrap();

    // data
    let durations: Vec<i32> = vec![4, 5, 6];
    let frequencies: Vec<i32> = vec![7, 8, 9];
    let intensity: Vec<f64> = vec![2.1, 2.5, 2.9];
    let lower: f64 = 1.0;
    let upper: f64 = 5.0;

    let open_manager =
        PostgresConnectionManager::new_from_stringlike(common::CONNECT_STRING_LARD, NoTls).unwrap();
    let open_db_pool = bb8::Pool::builder().build(open_manager).await.unwrap();
    let conn = open_db_pool.get().await.unwrap();

    let timeseries_query = conn
        .prepare(
            "INSERT INTO reports.idf_station_timeseries ( \
            station_id, number_of_seasons, quality_class, seed_parameter, fromtime, totime, updated_at \
        ) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
        )
        .await
        .unwrap();

    let data_query = conn
        .prepare(
            "INSERT INTO reports.idf_station_data ( \
                timeseries, duration, frequency, intensity, lower_interval, upper_interval \
        ) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .await
        .unwrap();

    for i in 0..stations.len() {
        let station: i32 = stations[i];
        let seasons: i32 = number_of_seasons[i];
        let qc: i32 = quality_class[i];

        let tsid: i32 = conn
            .query_one(
                &timeseries_query,
                &[
                    &station,
                    &seasons,
                    &qc,
                    &seed_parameter,
                    &fromtime,
                    &totime,
                    &updated_at,
                ],
            )
            .await
            .unwrap()
            .get(0);

        for duration in durations.iter() {
            for freq in frequencies.iter() {
                for val in intensity.iter() {
                    conn.execute(&data_query, &[&tsid, duration, freq, val, &lower, &upper])
                        .await
                        .unwrap();
                }
            }
        }
    }

    TestData {
        stations,
        // durations,
        // frequencies,
        intensity,
        lower,
        upper,
    }
}

#[tokio::test]
async fn test_idf_station_availability() {
    e2e_test_wrapper(async {
        let expected = TEST_DATA.get_or_init(init_idf_data).await;

        // Returns results sorted by station ID
        let url = "http://localhost:3000/reports/idf/station";

        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let resp: IdfStationAvailability = resp.json().await.unwrap();
        assert_eq!(resp.stations.len(), expected.stations.len());

        for (station, exp) in resp.stations.iter().zip(expected.stations.iter()) {
            assert_eq!(station.station_id, *exp)
        }
    })
    .await
}

#[tokio::test]
async fn test_idf_station_endpoint() {
    e2e_test_wrapper(async {
        let data = TEST_DATA.get_or_init(init_idf_data).await;

        let station_id = data.stations[0];
        let url = format!("http://localhost:3000/reports/idf/station/{}", station_id);

        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let resp: IdfStationResp = resp.json().await.unwrap();
        assert_eq!(resp.station_id, station_id);
    })
    .await
}

#[tokio::test]
async fn test_idf_station_endpoint_with_unit() {
    e2e_test_wrapper(async {
        let data = TEST_DATA.get_or_init(init_idf_data).await;

        let station_id = data.stations[1];
        let url = format!(
            "http://localhost:3000/reports/idf/station/{}?unit=lsha",
            station_id,
        );

        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let resp: IdfStationResp = resp.json().await.unwrap();
        assert_eq!(resp.station_id, station_id);

        for val in resp.values {
            // Since the values get converted from 'mm' to 'lsha'
            // we should not find them in the response
            assert!(!data.intensity.contains(&val.intensity));
            assert_ne!(val.lower_interval, data.lower);
            assert_ne!(val.upper_interval, data.upper);
        }
    })
    .await
}
