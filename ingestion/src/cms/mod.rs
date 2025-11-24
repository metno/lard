//use crate::IngestorState;
use axum::{routing::get, Router};
use maud::{html, Markup};
use util::{MetLabel, MetTimeseriesKey};

fn head(title: &str, stylesheet: &str) -> Markup {
    html! {
        head {
            meta charset="utf-8";
            meta name="viewport" content="width=device-width, initial-scale=1.0";
            title { (title) " | Lard CMS" }
            // TODO: description?
            link rel="stylesheet" href={ "/cms/assets/" (stylesheet) };
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
        (head("Search Results", "common"))
        body {
            div #admin-panel {
                (search_form())
            }
            div #search-results {
                @for ts in ts_list {
                    div .timeseries {
                        div .key.tsid {
                            (ts.id)
                        }
                        div .key.station-id {
                            (ts.key.station_id)
                        }
                        div .key.param-id {
                            (ts.key.param_id)
                        }
                        div .key.type-id {
                            (ts.key.type_id)
                        }
                        div .key.level {
                            (render_option(ts.key.level))
                        }
                        div .key.sensor {
                            (render_option(ts.key.sensor))
                        }
                    }
                }
            }
        }
    }
}

async fn home() -> Markup {
    html! {
        (head("Home", "common"))
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
        .with_state(())
}
