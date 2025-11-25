//use crate::IngestorState;
use axum::{routing::get, Router};
use maud::{html, Markup};
use tower_http::services::ServeDir;
use util::{MetLabel, MetTimeseriesKey};

// NOTE: if you make changes to the stylesheet, you need to update the version
// number here, otherwise clients will continue using their cached sheet
const STYLESTEET_COMMON: &str = "common.css?v=1.0";

fn head(title: &str, stylesheet: &str) -> Markup {
    html! {
        head {
            meta charset="utf-8";
            meta name="viewport" content="width=device-width, initial-scale=1.0";
            title { (title) " | Lard CMS" }
            // TODO: description?
            link rel="stylesheet" href={ "/cms/assets/css/" (stylesheet) };
        }
    }
}

fn text_field(name: &str, label: &str, value: Option<&str>, required: bool) -> Markup {
    html! {
        div.form-field {
            label for=(name) { (label) }
            input
                type="text"
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

fn search_form() -> Markup {
    html! {
        form action="cms/search_ts" method="get" .search-ts {
            (text_field("station_id", "Station ID:", None, false))
            (text_field("param_id", "Param ID:", None, false))
            (text_field("type_id", "Type ID:", None, false))
            (text_field("level", "Level:", None, false))
            (text_field("sensor", "Sensor:", None, false))
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

async fn search_handler() -> Markup {
    // TODO use query params to fetch from DB
    let ts_list = std::iter::repeat_n(
        MetLabel {
            id: 12313,
            key: MetTimeseriesKey {
                station_id: 15700,
                param_id: 514,
                type_id: 511,
                level: None,
                sensor: None,
            },
        },
        7,
    );

    html! {
        (head("Search Results", STYLESTEET_COMMON))
        body {
            div #admin-panel {
                (search_form())
            }
            div #search-results {
                @for ts in ts_list {
                    div .timeseries {
                        div .keys {
                            (render_ts_field("Timeseries ID:", ts.id, "ts-id"))
                            (render_ts_field("Station ID:", ts.key.station_id, "station-id"))
                            (render_ts_field("Param ID:", ts.key.param_id, "param-id"))
                            (render_ts_field("Type ID:", ts.key.type_id, "type-id"))
                            (render_ts_field("Sensor:", render_option(ts.key.sensor), "sensor"))
                            (render_ts_field("Level:", render_option(ts.key.level), "level"))
                        }
                    }
                }
            }
        }
    }
}

async fn home() -> Markup {
    html! {
        (head("Home", STYLESTEET_COMMON))
        body {
            div #admin-panel {
                h1 { "Lard content management" }
                (search_form())
            }
        }
    }
}

pub fn router<S>() -> Router<S> {
    Router::new()
        .route("/", get(home))
        .route("/search_ts", get(search_handler))
        .nest_service("/assets", ServeDir::new("assets"))
        .with_state(())
}
