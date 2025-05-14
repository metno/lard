use std::sync::Arc;

use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 {
        panic!("not enough args passed in, at least host, user, dbname needed, optionally password")
    }

    let mut connect_string = format!("host={} user={} dbname={}", &args[1], &args[2], &args[3]);
    if args.len() > 4 {
        connect_string.push_str(" password=");
        connect_string.push_str(&args[4])
    }

    // set up postgres connection pool
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    // Set up S3 bucket for IDF
    // TODO: fill out with correct params
    let bucket = Arc::from(
        s3::Bucket::new(
            "bucket_name",
            s3::Region::Custom {
                region: "where?".to_string(),
                endpoint: "url".to_string(),
            },
            s3::creds::Credentials::default().unwrap(),
        )
        .unwrap()
        .with_path_style(),
    );

    // Set up S3 client for IDF
    // let s3_config = aws_config::load_from_env().await;
    // let s3_client = Arc::new(ws_sdk_s3::Client::new(&s3_config));

    // set up cancellation token and signal catcher for graceful shutdown
    let cancel_token = CancellationToken::new();
    tokio::spawn(util::signal_catcher(cancel_token.clone()));

    tokio::spawn(lard_egress::run(pool, bucket, cancel_token.clone()));
}
