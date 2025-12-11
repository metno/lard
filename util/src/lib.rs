use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::signal;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::Interval;
use tokio_postgres::{types::FromSql, NoTls};
use tokio_util::sync::CancellationToken;

pub mod deserialize;
pub mod dut_parse;
pub mod idf_parse;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;
pub type PgPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

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

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Timeresolution {
    // minute resolutions
    PT1M,
    PT10M,
    PT20M,
    PT30M,
    // hourly resolutions
    PT1H,
    PT3H,
    PT6H,
    PT12H,
    // daily resolutions
    P1D,
    // monthly resolutions
    P1M,
    P3M,
    P6M,
    P1Y,
    VARIABLE, // list of timeresolutions
    UNKNOWN,  // for types we don't have in the mapping
}

pub type TimeresolutionMap = Arc<HashMap<i32, Timeresolution>>;

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

pub fn get_typeid_to_timeresolution(filename: &str) -> Result<TimeresolutionMap, csv::Error> {
    Ok(Arc::new(
        csv::Reader::from_path(filename)
            .unwrap()
            .into_records()
            .map(|record_result| {
                record_result.map(|record| {
                    (
                        record.get(0).unwrap().to_owned().parse().unwrap(), // typeid
                        match record.get(2).unwrap() {
                            "PT1M" => Timeresolution::PT1M,
                            "PT10M" => Timeresolution::PT10M,
                            "PT20M" => Timeresolution::PT20M,
                            "PT30M" => Timeresolution::PT30M,
                            "PT1H" => Timeresolution::PT1H,
                            "PT3H" => Timeresolution::PT3H,
                            "PT6H" => Timeresolution::PT6H,
                            "PT12H" => Timeresolution::PT12H,
                            "P1D" => Timeresolution::P1D,
                            "P1M" => Timeresolution::P1M,
                            "P3M" => Timeresolution::P3M,
                            "P6M" => Timeresolution::P6M,
                            "P1Y" => Timeresolution::P1Y,
                            _ => Timeresolution::VARIABLE,
                        }, // timeresolutions
                    )
                })
            })
            .collect::<Result<HashMap<i32, Timeresolution>, csv::Error>>()?,
    ))
}
