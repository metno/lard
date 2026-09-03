use chrono::{TimeZone, Utc};
use reqwest::Client;

use lard_egress::reports::{
    WindCategories, WindroseAvailabilityResp, WindroseAvailable, WindroseResp,
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

pub async fn ensure_windrose() {
    let start_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let to_time = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
    let y_bins = 16;

    let cases = [(
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

    let station_id = 20001;

    let client = Client::new();
    for (from, to, expected) in cases {
        let url = format!(
            "http://localhost:3000/reports/windrose/{station_id}\
                    ?fromtime={from}\
                    &totime={to}"
        );

        let resp = client.get(&url).send().await.unwrap();
        assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

        let json: WindroseResp = resp.json().await.unwrap();

        assert_values_and_sums(json, expected);
    }

    eprintln!("windrose ok");
}

pub async fn ensure_windrose_available() {
    let start_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();

    let cases = [WindroseAvailabilityResp {
        stations: vec![WindroseAvailable::new(20001, 1, start_time, None)],
    }];

    let client = Client::new();
    for expected in cases {
        let url = "http://localhost:3000/reports/windrose";

        let resp = client.get(url).send().await.unwrap();
        assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

        let json: WindroseAvailabilityResp = resp.json().await.unwrap();
        assert_eq!(json, expected);
    }

    eprintln!("windrose_available ok");
}
