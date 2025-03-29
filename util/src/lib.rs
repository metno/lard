use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use chronoutil::RelativeDuration;
use serde::{Deserialize, Serialize};
use tokio::signal;
use tokio::signal::unix::{signal, SignalKind};
use tokio_postgres::{types::FromSql, NoTls};
use tokio_util::sync::CancellationToken;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

#[derive(Clone, Debug, Serialize, Deserialize, FromSql)]
#[postgres(name = "location")]
pub struct Location {
    lat: Option<f64>,
    lon: Option<f64>,
    hamsl: Option<f64>,
    hag: Option<f64>,
}

// TODO: this is a messy hack, but it's the only way people at met currently have to determine
// time_resolution. Ultimately we intend to store time_resolution info in the database under
// public.timeseries or labels.met. This will be populated by a combination of a script that looks
// at a timeseries's history, and manual editing by content managers.
pub fn type_id_to_time_resolution(type_id: i32) -> Option<RelativeDuration> {
    // Source for these matches: PDF presented by PiM
    match type_id {
        514 => Some(RelativeDuration::minutes(1)),
        506 | 509 | 510 => Some(RelativeDuration::minutes(10)),
        7 | 311 | 330 | 342 | 501 | 502 | 503 | 505 | 507 | 511 => Some(RelativeDuration::hours(1)),
        522 => Some(RelativeDuration::days(1)),
        399 => Some(RelativeDuration::years(1)),
        _ => None,
    }
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
