use std::path::Path;

use chrono::{DateTime, Utc};
use pg_interval::Interval;
use serde::Deserialize;

use crate::{
    MetTimeseriesKey,
    interval::de_interval_iso8601,
    stinfofacade::permissions::{PermitTables, timeseries_get_permit},
};

#[derive(Deserialize, Debug)]
#[serde(rename = "repeated")]
struct Repeated {
    value: f64,
    start: DateTime<Utc>,
    stop: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "de_interval_iso8601")]
    interval: Interval,
}

#[derive(Deserialize, Debug)]
//#[serde(untagged)]
enum MockDataSpec {
    #[serde(rename = "repeated")]
    Repeated(Repeated),
    // Suggested Extensions:
    // Regular(([f64], Interval, Option<Datetime>))
    // Irregular([f64, Datetime])
}

impl MockDataSpec {
    fn derive_fromtime(&self) -> DateTime<Utc> {
        match self {
            Self::Repeated(repeated) => repeated.start,
        }
    }
}

#[derive(Deserialize, Debug)]
struct MockTs {
    label: MetTimeseriesKey,
    data: MockDataSpec,
}

#[derive(Deserialize, Debug)]
struct MockDataset {
    ts: Vec<MockTs>,
}

async fn label_mock_data(
    label: MetTimeseriesKey,
    fromtime: &DateTime<Utc>,
    permit: Option<i32>,
    client: &tokio_postgres::Client,
) -> i64 {
    let timeseries_id = client
        .query_one(
            "INSERT INTO public.timeseries (fromtime, permit) VALUES ($1, $2) RETURNING id",
            &[fromtime, &permit],
        )
        .await
        .unwrap()
        .get(0);

    client
        .execute(
            "INSERT INTO labels.met \
                (timeseries, station_id, param_id, type_id, lvl, sensor) \
                VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &timeseries_id,
                &label.station_id,
                &label.param_id,
                &label.type_id,
                &label.level,
                &label.sensor,
            ],
        )
        .await
        .unwrap();

    timeseries_id
}

async fn insert_mock_data(tsid: i64, data_spec: MockDataSpec, client: &tokio_postgres::Client) {
    match data_spec {
        MockDataSpec::Repeated(Repeated {
            value,
            start,
            stop,
            interval,
        }) => client
            .execute(
                "INSERT INTO public.data \
                (timeseries, obstime, obsvalue) \
                SELECT \
                    $1 AS timeseries, \
                    obstime, \
                    $2 AS obsvalue \
                FROM generate_series($3::timestamptz, $4::timestamptz, $5) AS obstime",
                &[
                    &tsid,
                    &value,
                    &start,
                    &stop.unwrap_or_else(Utc::now),
                    &interval,
                ],
            )
            .await
            .unwrap(),
    };
}

pub async fn load_mock_data(
    toml_path: impl AsRef<Path>,
    open_client: &tokio_postgres::Client,
    restricted_client: &tokio_postgres::Client,
    permit_tables: PermitTables,
) {
    if let Ok(file_content) = std::fs::read_to_string(toml_path) {
        let mock_dataset: MockDataset = toml::from_str(&file_content).unwrap();

        for ts in mock_dataset.ts {
            let permit = timeseries_get_permit(
                permit_tables.clone(),
                ts.label.station_id,
                ts.label.type_id,
                Some(ts.label.param_id),
            )
            .unwrap();
            let client = if permit == Some(1) {
                open_client
            } else {
                restricted_client
            };
            let fromtime = ts.data.derive_fromtime();

            let tsid = label_mock_data(ts.label, &fromtime, permit, client).await;

            insert_mock_data(tsid, ts.data, client).await;
        }
    }
}
