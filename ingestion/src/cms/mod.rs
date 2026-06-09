use crate::{DbPools, Error, IngestorState};
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
};
use maud::{DOCTYPE, Markup, html};
use serde::Deserialize;
use tower_http::services::ServeDir;
use util::{MetLabel, MetTimeseriesKey, PooledPgConn, http_error::internal};

// NOTE: if you make changes to the stylesheet, you need to update the version
// number here, otherwise clients will continue using their cached sheet
const STYLESTEET_COMMON: &str = "common.css?v=1.0";

fn head(title: &str, stylesheet: &str) -> Markup {
    html! {
        (DOCTYPE)
        head {
            meta charset="utf-8";
            meta name="viewport" content="width=device-width, initial-scale=1.0";
            title { (title) " | Lard CMS" }
            // TODO: description?
            link rel="stylesheet" href={ "/cms/assets/css/" (stylesheet) };
            script type="text/javascript" src="/cms/assets/js/script.js" {}
        }
    }
}

fn number_field(name: &str, label: &str, value: Option<&str>, required: bool) -> Markup {
    html! {
        div.form-field {
            label for=(name) { (label) }
            input
                type="number"
                name=(name)
                id=(name)
                value=[value]
                required[required];
        }
    }
}

fn submit_button(text: &str) -> Markup {
    html! {
        div.form-submit {
            button type="submit" { (text) }
        }
    }
}

fn search_form(params: Option<&SearchParams>) -> Markup {
    html! {
        form action="/cms/search_ts" method="get" .search-ts {
            (number_field("station_id", "Station ID:", params.map(|p| p.station_id.as_ref()), false))
            (number_field("param_id", "Param ID:", params.map(|p| p.param_id.as_ref()), false))
            (number_field("type_id", "Type ID:", params.map(|p| p.type_id.as_ref()), false))
            (number_field("level", "Level:", params.map(|p| p.level.as_ref()), false))
            (number_field("sensor", "Sensor:", params.map(|p| p.sensor.as_ref()), false))
            (submit_button("Search!"))
        }
    }
}

fn render_option(opt: Option<i32>) -> Markup {
    html! {
        @match opt {
            Some(inner) => (inner),
            None => ("NULL")
        }
    }
}

fn render_ts_field(name: &str, value: impl maud::Render, class: &str) -> Markup {
    html! {
        div .key.(class) {
            div .label { (name) }
            div .value { (value) }
        }
    }
}

#[derive(Deserialize)]
struct SearchParams {
    station_id: String,
    param_id: String,
    type_id: String,
    level: String,
    sensor: String,
}

// TODO: make this better version work
//fn parse_optional_field<T: FromStr>(input: String) -> Result<Option<T>, AppError>
//where
//    <T as FromStr>::Err: Send,
//    <T as FromStr>::Err: Sync,
//    <T as FromStr>::Err: std::error::Error,
//    <T as FromStr>::Err: 'static,
//{
//    if input.is_empty() {
//        Ok(None)
//    } else {
//        Ok(Some(
//            input
//                .parse()
//                .map_err(|e: <T as FromStr>::Err| AppError(anyhow!(e)))?,
//        ))
//    }
//}

fn parse_optional_field(input: &String) -> Option<i32> {
    if input.is_empty() {
        None
    } else {
        Some(input.parse().unwrap())
    }
}

async fn get_ts_list(
    conn: &mut PooledPgConn<'_>,
    station_id: Option<i32>,
    param_id: Option<i32>,
    type_id: Option<i32>,
    level: Option<i32>,
    sensor: Option<i32>,
) -> Result<Vec<MetLabel>, Error> {
    // TODO: handle Nones better in params
    Ok(conn
        .query(
            r#"
            SELECT timeseries, station_id, param_id, type_id, lvl, sensor
            FROM labels.met
            WHERE station_id = $1 AND param_id = $2
            "#,
            &[&station_id, &param_id],
        )
        .await?
        .iter()
        .map(|row| MetLabel {
            id: row.get(0),
            key: MetTimeseriesKey {
                // TODO: it's an issue that station, param, and type_id can be NULL in the schema,
                // but not in this struct
                station_id: row.get(1),
                param_id: row.get(2),
                type_id: row.get(3),
                level: row.get(4),
                sensor: row.get(5),
            },
        })
        .filter(|label| {
            type_id.is_none_or(|x| x == label.key.type_id)
                && level.is_none_or(|x| Some(x) == label.key.level)
                && sensor.is_none_or(|x| Some(x) == label.key.sensor)
        })
        .collect())
}

async fn search_handler(
    State(pools): State<DbPools>,
    Query(search_params): Query<SearchParams>,
) -> Result<Markup, (StatusCode, String)> {
    let ts_list = async {
        let mut open_conn = pools.open.get().await?;
        get_ts_list(
            &mut open_conn,
            parse_optional_field(&search_params.station_id),
            parse_optional_field(&search_params.param_id),
            parse_optional_field(&search_params.type_id),
            parse_optional_field(&search_params.level),
            parse_optional_field(&search_params.sensor),
        )
        .await
    }
    .await
    .map_err(internal)?;

    Ok(html! {
        (head("Search Results", STYLESTEET_COMMON))
        body {
            div #admin-panel {
                (search_form(Some(&search_params)))
            }
            div #search-results {
                @for ts in ts_list {
                    div #{ "timeseries-" (ts.id) }.timeseries {
                        div .keys {
                            (render_ts_field("Timeseries ID:", ts.id, "ts-id"))
                            (render_ts_field("Station ID:", ts.key.station_id, "station-id"))
                            (render_ts_field("Param ID:", ts.key.param_id, "param-id"))
                            (render_ts_field("Type ID:", ts.key.type_id, "type-id"))
                            (render_ts_field("Sensor:", render_option(ts.key.sensor), "sensor"))
                            (render_ts_field("Level:", render_option(ts.key.level), "level"))
                        }
                        input .deactivate-ts type="button" value="Deactivate" onclick={ "deactivate_ts(" (ts.id) ");" };
                    }
                }
            }
        }
    })
}

#[derive(Deserialize)]
struct DeactivateTsParams {
    id: i64,
}

//#[axum::debug_handler]
async fn deactivate_ts_handler(
    State(pools): State<DbPools>,
    Query(DeactivateTsParams { id }): Query<DeactivateTsParams>,
) -> (StatusCode, String) {
    let result: Result<u64, Error> = async {
        let open_conn = pools.open.get().await?;
        let rows_affected = open_conn
            .execute(
                r#"
            UPDATE public.timeseries
            SET deactivated = true
            WHERE id = $1
            "#,
                &[&id],
            )
            .await?;
        Ok(rows_affected)
    }
    .await;

    match result {
        Ok(1) => (StatusCode::OK, "".to_string()),
        Ok(rows_affected) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Wrong number of rows affected: {rows_affected}"),
        ),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn home() -> Markup {
    html! {
        (head("Home", STYLESTEET_COMMON))
        body {
            div #admin-panel {
                h1 { "Lard content management" }
                (search_form(None))
            }
        }
    }
}

pub fn router(assets_path: &str) -> Router<IngestorState> {
    Router::new()
        .route("/", get(home))
        .route("/search_ts", get(search_handler))
        .route("/deactivate_ts", post(deactivate_ts_handler))
        .nest_service("/assets", ServeDir::new(assets_path))
}
