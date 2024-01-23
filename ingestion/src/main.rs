use axum::{response::Json, routing::post, Router};
use serde::Serialize;

pub mod kldata;
use kldata::parse_kldata;

#[derive(Debug, Serialize)]
struct KldataResp {
    message: String,
    message_id: usize,
    res: u8, // TODO: Should be an enum?
    retry: bool,
}

async fn handle_kldata(body: String) -> Json<KldataResp> {
    let parsed = parse_kldata(&body);

    // TODO: Find or generate obsinn labels
    // TODO: Find or generate filter labels

    // TODO: Insert into data table

    Json(KldataResp {
        // TODO: fill in meaningful values here
        message: "".into(),
        message_id: 0,
        res: 0,
        retry: false,
    })
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // build our application with a single route
    let app = Router::new().route("/kldata", post(handle_kldata));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await?;

    Ok(())
}
