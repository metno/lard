use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use tokio::signal;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::Interval;
use tokio_postgres::{types::FromSql, NoTls};
use tokio_util::sync::CancellationToken;

pub mod deserialize;
pub mod dut_parse;
pub mod idf_parse;
pub mod stinfofacade;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;
pub type PgPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

pub const FROM_TO_FUTURES_FAILURES: &str = "from_to_futures_failures";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClosedTimerange {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl ClosedTimerange {
    pub fn new(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        ClosedTimerange { from, to }
    }

    pub fn overlap(&self, other: Self) -> Option<Self> {
        let from = self.from.max(other.from);
        let to = self.to.min(other.to);

        // If they overlap return the new timerange
        (from < to).then_some(ClosedTimerange { from, to })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpenTimerange {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

impl OpenTimerange {
    pub fn new(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> Self {
        OpenTimerange { from, to }
    }

    /// Used to cut the priorities to cover ranges that actually matter to a particular timeseries
    /// Takes the from and to times of the timeseries as well as the from and to of the priority range
    /// Returns an option, since it could be they do not overlapp at all (and thus it returns empty)
    pub fn overlap(&self, other: Self) -> Option<Self> {
        let fromtime = match (self.from, other.from) {
            (Some(lhs), Some(rhs)) => Some(lhs.max(rhs)), // return the later one
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        };
        let totime = match (self.to, other.to) {
            (Some(lhs), Some(rhs)) => Some(lhs.min(rhs)), // return the earlier one
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        };

        match (fromtime, totime) {
            // If both ends are closed and the ranges overlap return the new timerange
            (Some(from), Some(to)) => {
                if from >= to {
                    None
                } else {
                    Some(OpenTimerange {
                        from: Some(from),
                        to: Some(to),
                    })
                }
            }
            (from, to) => Some(OpenTimerange { from, to }),
        }
    }
}

/// Type for refreshing caches
pub struct Cron<State, F: AsyncFn(&State)> {
    pub state: State,
    pub action: F,
    pub interval: Interval,
}

impl<State, F: AsyncFn(&State)> Cron<State, F> {
    /// Consumes itself to run the given action in a loop
    pub async fn run_forever(mut self) {
        loop {
            self.interval.tick().await;
            (self.action)(&self.state).await;
        }
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
