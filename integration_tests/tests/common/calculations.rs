use chrono::{SecondsFormat, TimeZone, Utc};

use lard_egress::calculations::{CalculationAvailable, CalculationResp};

pub async fn ensure_calculations_available() {
    let url = "http://localhost:3000/calculations/param".to_string();

    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success());

    let json: Vec<CalculationAvailable> = resp.json().await.unwrap();
    // this should just list the available param ids and their endpoints
    assert!(
        !json.is_empty(),
        "Expected a list of available calculations param ids"
    );

    eprintln!("calculations_availability ok");
}

pub async fn ensure_calculations_specific_humidity() {
    let from = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let to = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    let station_id = "20001";
    let params = format!(
        "?level=200\
                &sensor=0\
                &from={}&to={}&accepted_qc=-1,0,1,2,3", // list includes -1 which maps to null, which means we accept rows with null quality code
        from.to_rfc3339_opts(SecondsFormat::Secs, true),
        to.to_rfc3339_opts(SecondsFormat::Secs, true)
    );

    // get the specific_humidity of station 20001
    let url =
        format!("http://localhost:3000/calculations/station/{station_id}/param/3123{params}",);

    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success());

    let json: CalculationResp = resp.json().await.unwrap();
    assert!(
        !json.data.is_empty(),
        "Expected at least one calculation result"
    );

    eprintln!("calculations_specific_humidity ok");
}
