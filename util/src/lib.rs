use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::signal;
use tokio::signal::unix::{signal, SignalKind};
use tokio_postgres::{types::FromSql, NoTls};
use tokio_util::sync::CancellationToken;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

#[derive(Debug, Serialize, Deserialize, FromSql)]
#[postgres(name = "location")]
pub struct Location {
    lat: Option<f64>,
    lon: Option<f64>,
    hamsl: Option<f64>,
    hag: Option<f64>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Could not read environment variable: {0}")]
    Env(String),
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        use Error::*;

        match (self, other) {
            (Env(a), Env(b)) => a == b,
        }
    }
}

/// Gets an environment variable, providing more details than calling std::env::var() directly.
pub fn getenv(key: &str) -> Result<String, Error> {
    std::env::var(key).map_err(|e| Error::Env(format!("{e}: {key}")))
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
