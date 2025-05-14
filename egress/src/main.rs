use std::sync::Arc;

use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use lard_egress::errors::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // set up postgres connection pool
    let connect_string = std::env::var("LARD_CONN_STRING")?;
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls)?;
    let pool = bb8::Pool::builder().build(manager).await?;

    // Set up S3 bucket for IDF
    let bucket = Arc::from(
        s3::Bucket::new(
            &std::env::var("S3_BUCKET_NAME")?,
            s3::Region::Custom {
                region: std::env::var("AWS_REGION")?,
                endpoint: std::env::var("S3_ENDPOINT_URL")?,
            },
            // Requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to be set
            s3::creds::Credentials::default().unwrap(),
        )?
        .with_path_style(),
    );

    // set up cancellation token and signal catcher for graceful shutdown
    let cancel_token = CancellationToken::new();
    tokio::spawn(util::signal_catcher(cancel_token.clone()));

    tokio::spawn(lard_egress::run(pool, bucket, cancel_token.clone()));

    Ok(())
}
