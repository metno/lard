use axum::Router;
use lard_ingestion::{cms, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let app = Router::new().nest("/cms", cms::router());
    //.with_state(IngestorState {
    //    db_pools,
    //    param_conversions,
    //    permit_tables,
    //    level_table,
    //    rove_connector,
    //    qc_pipelines,
    //});

    // run our app with hyper, listening globally on port 3001
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    //info!("Ingestion server started!");
    axum::serve(listener, app).await?;

    Ok(())
}
