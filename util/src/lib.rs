use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::signal;
use tokio::signal::unix::{SignalKind, signal};
use tokio_postgres::{NoTls, types::FromSql};
use tokio_util::sync::CancellationToken;

pub mod auth;
pub mod deserialize;
pub mod dut_parse;
pub mod http_error;
pub mod idf_parse;
pub mod interval;
pub mod mock;
pub mod normals_parse;
pub mod patchwork;
pub mod stinfofacade;
pub mod timerange;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;
pub type PgPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

pub type StationId = i32;
pub type TypeId = i32;
pub type ParamId = i32;
pub type PermitId = i32;
pub type TsId = i64;

pub const FROM_TO_FUTURES_FAILURES: &str = "from_to_futures_failures";
pub const REFRESH_FROM_TO_DURATION_SECONDS: &str = "refresh_from_to_duration_seconds";

#[derive(Debug, Clone)]
pub struct DbPools {
    pub open: PgPool,
    pub restricted: PgPool,
}

#[derive(Debug, Serialize, Deserialize, FromSql)]
#[postgres(name = "location")]
pub struct Location {
    lat: Option<f64>,
    lon: Option<f64>,
    hamsl: Option<f64>,
    hag: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub struct MetTimeseriesKey {
    pub station_id: i32,
    pub param_id: i32,
    pub type_id: i32,
    pub level: Option<i32>,
    pub sensor: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetLabel {
    pub id: i64,
    pub key: MetTimeseriesKey,
}

impl MetLabel {
    pub fn new(
        id: i64,
        station_id: i32,
        param_id: i32,
        type_id: i32,
        level: Option<i32>,
        sensor: Option<i32>,
    ) -> MetLabel {
        MetLabel {
            id,
            key: MetTimeseriesKey {
                station_id,
                param_id,
                type_id,
                level,
                sensor,
            },
        }
    }
}

#[derive(Error, Debug, PartialEq)]
#[error("Could not read environment variable: {0}")]
pub struct EnvError(String);

/// Gets an environment variable, providing more details than calling std::env::var() directly.
pub fn getenv(key: &str) -> Result<String, EnvError> {
    std::env::var(key).map_err(|e| EnvError(format!("{e}: {key}")))
}

/// Returns a Future that triggers cancel_token and completes once a relevant signal to shutdown
/// the service is caught.
pub async fn signal_catcher(cancel_token: CancellationToken) {
    // SIGTERM is the most important signal to handle since it is the one that is sent by
    // systemd (as a result of commands like 'systemctl stop' or 'systemctl restart').
    let sigterm = async {
        signal(SignalKind::terminate())
            .expect("failed to install signal handler for SIGTERM")
            .recv()
            .await;
    };

    // SIGINT could also be relevant in some cases
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install signal handler for SIGINT (Ctrl-C)");
    };

    tokio::select! {
        _ = sigterm => {},
        _ = ctrl_c => {},
    }

    cancel_token.cancel()
}
