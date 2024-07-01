use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
use tokio_postgres::{types::FromSql, NoTls};

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

#[derive(Debug, Serialize, Deserialize, FromSql)]
#[postgres(name = "location")]
pub struct Location {
    lat: Option<f32>,
    lon: Option<f32>,
    hamsl: Option<f32>,
    hag: Option<f32>,
}
