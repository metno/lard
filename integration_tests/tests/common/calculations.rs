use chrono::{SecondsFormat, TimeZone, Utc};

use lard_egress::http::calculations::{Available, Resp};

pub async fn ensure_calculations_available() {
    let url = "http://localhost:3000/calculations/param".to_string();

    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success());

    let json: Vec<Available> = resp.json().await.unwrap();
    // this should just list the available param ids and their endpoints
    assert!(
        !json.is_empty(),
        "Expected a list of available calculations param ids"
    );

    eprintln!("calculations_availability ok");
}

pub async fn ensure_calculations_specific_humidity() {
    let t1 = Utc
        .with_ymd_and_hms(2024, 1, 1, 23, 0, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let t2 = Utc
        .with_ymd_and_hms(2024, 1, 2, 0, 0, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let t3 = Utc
        .with_ymd_and_hms(2024, 1, 2, 2, 0, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);

    let cases = vec![
        (
            "get dew point temperature for single patchset",
            20001,
            217,
            format!("?level=200&sensor=0&from={}&to={}", t1, t2),
            200,
            vec![-49.108794052814694],
        ),
        (
            // [CalculationPatch { tsids: [1, 10, 12], timerange: Closed { from: 2024-01-01T23:00:00Z, to: 2024-01-02T00:00:00Z } },
            // CalculationPatch { tsids: [1, 11, 12], timerange: Closed { from: 2024-01-02T00:00:00Z, to: 2024-01-02T02:00:00Z } }]
            "get relative humidity across multiple patchsets", // 262 should have 2 halves in patchwork
            20001,
            3123,
            format!("?level=200&sensor=0&from={}&to={}", t1, t3),
            200,
            vec![
                0.04189121914060844,
                0.04189121914060844,
                0.04189121914060844,
            ],
        ),
    ];

    for (description, station_id, param_id, params, expected_status, expected_values) in cases {
        // get the specific_humidity of station 20001
        let url = format!(
            "http://localhost:3000/calculations/station/{station_id}/param/{param_id}{params}",
        );
        println!("{}", description);
        let resp = reqwest::get(url).await.unwrap();
        let status = resp.status().as_u16();
        assert_eq!(
            status, expected_status,
            "Expected status {} but got {} for case: {}",
            expected_status, status, description
        );
        if expected_status == 200 {
            let body: Resp = resp.json().await.unwrap();
            println!("body: {:?}", body);
            assert!(
                !body.data.is_empty(),
                "Expected at least one aggregation for case: {}",
                description
            );
            assert!(
                expected_values.len() == body.data.len(),
                "Expected length of data {} but got {}, for case: {}",
                expected_values.len(),
                body.data.len(),
                description
            );
            // zip the two vecs
            for (datum, expect) in body.data.iter().zip(expected_values.iter()) {
                assert!(
                    *expect == datum.1,
                    "Expected value {:?} but got {:?} timeseries for case: {}",
                    expect,
                    datum.1,
                    description
                );
            }
        }
    }
    eprintln!("calculations_specific_humidity ok");
}
