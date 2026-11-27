use std::sync::LazyLock;

use chrono::{DateTime, Duration, TimeZone, Utc};
use reqwest::Client;

use lard_egress::{
    http::reports::idf_event::{AvailabilityResp, Available, Metadata, Resp},
    patchwork::{Patch, SourceTs},
    reports::idf_event::{DEFAULT_DURATIONS, IdfEvent},
};

const STATION: i32 = 20001;

static T1: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.with_ymd_and_hms(2024, 12, 31, 23, 40, 0).unwrap());
static T2: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.with_ymd_and_hms(2024, 12, 31, 23, 49, 0).unwrap());
static T3: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.with_ymd_and_hms(2025, 1, 1, 0, 9, 0).unwrap());

pub async fn ensure_idf_event_available() {
    // NOTE: the tsids and permits are 0 because they skipped when serialising,
    // and so come as default values from the deserialiser
    let expected = AvailabilityResp {
        stations: vec![Available {
            station_id: STATION,
            metadata: Metadata {
                sources: vec![
                    Patch::new(*T1, Some(SourceTs::new(0, None, 514))),
                    Patch::new(
                        Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
                        Some(SourceTs::new(0, None, 508)),
                    ),
                ],
                permit: 1,
            },
        }],
    };

    let url = "http://localhost:3000/reports/idf/event";

    let client = Client::new();
    let resp = client.get(url).send().await.unwrap();
    assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

    let json: AvailabilityResp = resp.json().await.unwrap();

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

        let json: Resp = resp.json().await.unwrap();
        assert_eq!(json.station_id, STATION, "{title}");
        assert_eq!(json.values.len(), expected.len(), "{title}");

        for (val, exp) in json.values.into_iter().zip(expected) {
            assert_eq!(val, exp, "{title}")
        }
    }

    eprintln!("idf_event ok");
}
