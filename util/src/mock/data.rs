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
#[serde(rename = "repeated_nonscalar")]
struct RepeatedNonscalar {
    value: String,
    start: DateTime<Utc>,
    stop: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "de_interval_iso8601")]
    interval: Interval,
}

#[derive(Deserialize, Debug)]
#[serde(rename = "irregular_value")]
struct IrregularValue {
    value: f64,
    time: DateTime<Utc>,
}

#[derive(Deserialize, Debug)]
#[serde(rename = "irregular")]
struct Irregular {
    values: Vec<IrregularValue>,
}

#[derive(Deserialize, Debug)]
//#[serde(untagged)]
enum MockDataSpec {
    #[serde(rename = "repeated")]
    Repeated(Repeated),
    #[serde(rename = "repeated_nonscalar")]
    RepeatedNonscalar(RepeatedNonscalar),
    #[serde(rename = "irregular")]
    Irregular(Irregular),
}

impl MockDataSpec {
    fn derive_fromtime(&self) -> DateTime<Utc> {
        match self {
            Self::Repeated(repeated) => repeated.start,
            Self::RepeatedNonscalar(repeated) => repeated.start,
            Self::Irregular(irregular) => irregular.values.first().unwrap().time,
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

async fn insert_repeated(
    tsid: i64,
    Repeated {
        value,
        start,
        stop,
        interval,
    }: Repeated,
    client: &tokio_postgres::Client,
) {
    client
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
        .unwrap();
    let qc = 1;
    client
        .execute(
            "INSERT INTO legacy.data \
                (timeseries, obstime, original, corrected, quality_code) \
                SELECT \
                    $1 AS timeseries, \
                    obstime, \
                    $2 AS original, \
                    $6 AS corrected, \
                    $7 AS quality_code \
                FROM generate_series($3::timestamptz, $4::timestamptz, $5) AS obstime",
            &[
                &tsid,
                &value,
                &start,
                &stop.unwrap_or_else(Utc::now),
                &interval,
                &value,
                &qc,
            ],
        )
        .await
        .unwrap();
}

async fn insert_repeated_nonscalar(
    tsid: i64,
    RepeatedNonscalar {
        value,
        start,
        stop,
        interval,
    }: RepeatedNonscalar,
    client: &tokio_postgres::Client,
) {
    client
        .execute(
            "INSERT INTO public.nonscalar_data \
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
        .unwrap();
}

// If you try to insert a large amount of data with this, you should make it more efficient with
// a transaction and FuturesUnordered
async fn insert_irregular(
    tsid: i64,
    Irregular { values }: Irregular,
    client: &tokio_postgres::Client,
) {
    for value in values.iter() {
        client
            .execute(
                "INSERT INTO public.data \
                (timeseries, obstime, obsvalue) \
                VALUES ($1, $2, $3)",
                &[&tsid, &value.time, &value.value],
            )
            .await
            .unwrap();
    }
    let qc = 1;
    for value in values.iter() {
        client
            .execute(
                "INSERT INTO legacy.data \
                (timeseries, obstime, original, corrected, quality_code) \
                VALUES ($1, $2, $3, $4, $5)",
                &[&tsid, &value.time, &value.value, &value.value, &qc],
            )
            .await
            .unwrap();
    }
}

async fn insert_mock_data(tsid: i64, data_spec: MockDataSpec, client: &tokio_postgres::Client) {
    match data_spec {
        MockDataSpec::Repeated(repeated) => insert_repeated(tsid, repeated, client).await,
        MockDataSpec::RepeatedNonscalar(repeated) => {
            insert_repeated_nonscalar(tsid, repeated, client).await
        }
        MockDataSpec::Irregular(irregular) => insert_irregular(tsid, irregular, client).await,
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
