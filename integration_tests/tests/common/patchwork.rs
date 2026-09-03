use reqwest::{Client, StatusCode};

use lard_egress::{PatchworkAvailableResp, PatchworkResp};
use util::mock::auth::bearer::create_mock_jwt;

pub async fn ensure_patchwork_available() {
    let n_labels = 6;

    let url = "http://localhost:3000/patchwork/available";
    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success());

    let json: PatchworkAvailableResp = resp.json().await.unwrap();
    assert_eq!(json.available.len(), n_labels);

    eprintln!("patchwork_available ok");
}

pub async fn ensure_patchwork() {
    let token_permitid5 = create_mock_jwt(vec!["read-permitid-5".to_string()]).unwrap_or_default();
    let token_stationid40002 =
        create_mock_jwt(vec!["read-stationid-40002".to_string()]).unwrap_or_default();
    let token_both = create_mock_jwt(vec![
        "read-permitid-5".to_string(),
        "read-stationid-40002".to_string(),
    ])
    .unwrap_or_default();
    let token_nothing = create_mock_jwt(vec![]).unwrap_or_default();

    let cases = vec![
        (
            20001,
            // default level for 211 is 200
            // we also default to sensor 0
            "?paramid=211\
            &from=2024-01-01T00:00:00Z\
            &to=2024-01-01T01:30:00Z",
            None,
            200,
            2,
        ),
        // default level for grass param is actually None
        (
            20001,
            "?paramid=225\
            &from=2024-01-01T00:00:00Z\
            &to=2024-01-01T01:30:00Z",
            None,
            200,
            2,
        ),
        // 40001 has permitid 5, so is restricted
        (
            40001,
            "?paramid=211\
            &from=2024-01-01T00:00:00Z\
            &to=2024-01-01T01:30:00Z",
            None, // no token, no data access
            404,  // just don't see it...
            0,
        ),
        (
            40001,
            "?paramid=211\
            &from=2024-01-01T00:00:00Z\
            &to=2024-01-01T01:30:00Z",
            Some(token_permitid5), // token with permitid 5, should have access
            200,
            2,
        ),
        // check functionality to open for a specific station (that we don't have a permit for)
        (
            40002,
            "?paramid=211\
            &from=2024-01-01T00:00:00Z\
            &to=2024-01-01T01:30:00Z",
            Some(token_nothing), // token with no stationid access, should not have access
            404,                 // just don't see it...
            0,
        ),
        (
            40002,
            "?paramid=211\
            &from=2024-01-01T00:00:00Z\
            &to=2024-01-01T01:30:00Z",
            Some(token_stationid40002),
            200,
            2,
        ),
        (
            40002,
            // leave the sensor and level here to check if also works
            // even if they would default to the same values
            "?paramid=211\
            &level=200\
            &sensor=0\
            &from=2024-01-01T00:00:00Z\
            &to=2024-01-01T01:30:00Z",
            Some(token_both), // should still work if we have both stationid and permitid access
            200,
            2,
        ),
    ];

    for (station_id, params, token, status, n_data_found) in cases {
        let url = format!(
            "http://localhost:3000/patchwork/station/{}{}",
            station_id, params
        );
        let client = Client::new();
        let request = match token {
            Some(t) => client.get(url).bearer_auth(t),
            None => client.get(url).basic_auth("test", Some("test")),
        };

        let resp = request.send().await.unwrap();
        assert!(resp.status() == status);

        if status == StatusCode::OK {
            let json: PatchworkResp = resp.json().await.unwrap();
            assert_eq!(json.data.len(), n_data_found);
        }
    }

    eprintln!("patchwork ok");
}
