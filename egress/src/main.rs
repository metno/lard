use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use util::getenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // let args: Vec<String> = std::env::args().collect();

    // set up postgres connection pool
    let manager =
        PostgresConnectionManager::new_from_stringlike(getenv("LARD_CONN_STRING")?, NoTls)?;
    let pool = bb8::Pool::builder().build(manager).await?;

    // set up cancellation token and signal catcher for graceful shutdown
    let cancel_token = CancellationToken::new();
    tokio::spawn(util::signal_catcher(cancel_token.clone()));

    tokio::spawn(lard_egress::run(pool, cancel_token.clone()));

    Ok(())
}
