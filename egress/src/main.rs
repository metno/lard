use bb8_postgres::PostgresConnectionManager;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 {
        panic!("not enough args passed in, at least host, user, dbname needed, optionally password")
    }

    // set up postgres connection pool
    let mut connect_string = format!("host={} user={} dbname={}", &args[1], &args[2], &args[3]);
    if args.len() > 4 {
        connect_string.push_str(" password=");
        connect_string.push_str(&args[4])
    }
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    let db_pool = bb8::Pool::builder().build(manager).await.unwrap();

    // initialize the product operator registry
    let pop_reg = Arc::new(drops::operator::init_reg(db_pool.clone()));
    // NOTE: the LARD Ingestor also needs to call init_reg(), and the two product operator
    // registries should have the same contents (i.e. a product type is supported in the Ingestor
    // iff it is supported in the API)

    // set up cancellation token and signal catcher for graceful shutdown
    let cancel_token = CancellationToken::new();
    tokio::spawn(util::signal_catcher(cancel_token.clone()));

    // run server
    tokio::spawn(lard_egress::run(db_pool, pop_reg, cancel_token.clone()));
}
