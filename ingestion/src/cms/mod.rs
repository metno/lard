use crate::IngestorState;
use axum::{routing::get, Router};
use maud::{html, Markup, Render};

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

async fn search_handler() -> Markup {
    html! { "TODO" }
}

async fn home() -> Markup {
    html! {
        (head("Home", "home"))
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
