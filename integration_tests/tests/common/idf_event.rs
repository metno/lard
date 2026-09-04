use std::sync::LazyLock;

use chrono::{DateTime, Duration, TimeZone, Utc};
use lard_egress::reports::IdfEventAvailable;
use reqwest::Client;

use lard_egress::reports::{DEFAULT_DURATIONS, IdfEvent, IdfEventAvailabilityResp, IdfEventResp};

const STATION: i32 = 20001;

static T1: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.with_ymd_and_hms(2024, 12, 31, 23, 40, 0).unwrap());
static T2: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.with_ymd_and_hms(2024, 12, 31, 23, 49, 0).unwrap());
static T3: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.with_ymd_and_hms(2025, 1, 1, 0, 9, 0).unwrap());

pub async fn ensure_idf_event_available() {
    let expected = IdfEventAvailabilityResp {
        stations: vec![IdfEventAvailable::new(STATION, 1, *T1, None)],
    };

    let url = "http://localhost:3000/reports/idf/event";

    let client = Client::new();
    let resp = client.get(url).send().await.unwrap();
    assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

    let json: IdfEventAvailabilityResp = resp.json().await.unwrap();

    assert_eq!(json, expected);

    eprintln!("idf_event_available ok");
}

pub async fn ensure_idf_event() {
    let cases = [
        (
            // Only extract 2 timestamps for simplicity
            "default durations",
            *T1,
            *T1 + Duration::minutes(2),
            None,
            // Skip the first element (duration = 1), since that one can only return a single
            // timestamp
            vec![IdfEvent::new(1.0, 1, *T1, *T1)]
                .into_iter()
                .chain(
                    // All durations > 1 should return the same intensity and timestamps
                    DEFAULT_DURATIONS[1..]
                        .iter()
                        .map(|d| IdfEvent::new(2.0, *d, *T1, *T1 + Duration::minutes(1))),
                )
                .collect(),
        ),
        (
            // Should only get the first timeseries
            "single duration",
            *T1,
            *T1 + Duration::minutes(10),
            Some(vec![10]),
            vec![IdfEvent::new(10.0, 10, *T1, *T2)],
        ),
        (
            // Should get both timeseries
            "multiple durations",
            *T1,
            *T1 + Duration::minutes(50),
            Some(vec![10, 40]),
            vec![
                IdfEvent::new(10.0, 10, *T1, *T2),
                IdfEvent::new(30.0, 40, *T1, *T3),
            ],
        ),
    ];

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
            "http://localhost:3000/reports/idf/event/{STATION}\
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
        assert_eq!(json.station_id, STATION, "{title}");
        assert_eq!(json.values.len(), expected.len(), "{title}");

        for (val, exp) in json.values.into_iter().zip(expected) {
            assert_eq!(val, exp, "{title}")
        }
    }

    eprintln!("idf_event ok");
}
