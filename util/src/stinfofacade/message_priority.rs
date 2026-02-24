// TODO: doc comment

use std::collections::HashMap;

use chrono::NaiveDateTime;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};

use crate::{
    stinfofacade::{
        persistence::message_priority::{load_persisted, persist},
        Error,
    },
    OpenTimerange, ParamId, PatchworkLabel, TypeId,
};

#[derive(Debug, Clone, PartialEq)]
pub struct MessagePriority {
    pub priority: i32,
    pub timerange: OpenTimerange,
}

impl MessagePriority {
    pub fn new(priority: i32, timerange: OpenTimerange) -> MessagePriority {
        MessagePriority {
            priority,
            timerange,
        }
    }
}

/// This table is where to look for the timeseries priority
/// for a given typeid and paramid
pub type DefaultTable = HashMap<(TypeId, ParamId), MessagePriority>;
/// This table contains more specific exceptions to the default table
/// for a patchwork label and typeid
pub type ExceptionTable = HashMap<(PatchworkLabel, TypeId), MessagePriority>;

/// Get a fresh cache of message priority from stinfosys
/// this is the defaults for a typeid and paramid
async fn fetch_message_priority_default(client: &Client) -> Result<DefaultTable, Error> {
    let rows = client
        .query(
            "SELECT \
                mpd.message_formatid, \
                mpd.paramid, \
                mpd.priority, \
                mpd.fromtime, \
                mpd.totime \
            FROM message_priority_default mpd \
            ORDER BY message_formatid, paramid",
            &[],
        )
        .await
        .inspect_err(|e| warn!("failed to query message_priority defaults: {e}"))?;

    // build hashmap
    let mut message_priority = HashMap::new();

    for row in rows {
        let f: Option<NaiveDateTime> = row.get(3);
        let t: Option<NaiveDateTime> = row.get(4);
        message_priority.insert(
            (row.get(0), row.get(1)),
            MessagePriority {
                priority: row.get(2),
                timerange: OpenTimerange {
                    from: f.map(|x| x.and_utc()),
                    to: t.map(|x| x.and_utc()),
                },
            },
        );
    }
    Ok(message_priority)
}

/// Get a fresh cache of message priority from stinfosys
/// this is the exceptions, so more specific and includes the station number as well as type id
async fn fetch_message_priority_exception(client: &Client) -> Result<ExceptionTable, Error> {
    let rows = client
        .query(
            "SELECT \
                mpe.stationid, \
                mpe.message_formatid, \
                mpe.paramid, \
                mpe.hlevel, \
                mpe.sensor, \
                mpe.priority, \
                mpe.fromtime, \
                mpe.totime \
            FROM message_priority_exception mpe \
            ORDER BY stationid, message_formatid, paramid",
            &[],
        )
        .await
        .inspect_err(|e| warn!("failed to query message_priority exceptions: {e}"))?;

    // build hashmap
    let mut message_priority: HashMap<(PatchworkLabel, i32), MessagePriority> = HashMap::new();

    for row in rows {
        let f: Option<NaiveDateTime> = row.get(6);
        let t: Option<NaiveDateTime> = row.get(7);
        message_priority.insert(
            (
                PatchworkLabel {
                    station_id: row.get(0),
                    param_id: row.get(2),
                    level: row.get(3),
                    sensor: row.get(4),
                },
                row.get(1),
            ),
            MessagePriority {
                priority: row.get(5),
                timerange: OpenTimerange {
                    from: f.map(|x| x.and_utc()),
                    to: t.map(|x| x.and_utc()),
                },
            },
        );
    }
    Ok(message_priority)
}

pub async fn fetch_message_priority_stinfosys(
    stinfo_conn_string: Option<&str>,
) -> Result<(DefaultTable, ExceptionTable), Error> {
    let stinfo_conn_string = match stinfo_conn_string {
        Some(s) => s,
        None => return Err(Error::NoConnString),
    };

    let (client, conn) = tokio_postgres::connect(stinfo_conn_string, NoTls).await?;
    // conn object independently performs communication with database, so needs it's own task.
    // it will return when the client is dropped
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {}", e);
        }
    });

    let default = fetch_message_priority_default(&client).await?;
    let exception = fetch_message_priority_exception(&client).await?;

    Ok((default, exception))
}

pub async fn fetch_message_priority(
    stinfo_conn_string: Option<&str>,
) -> Result<(DefaultTable, ExceptionTable), Error> {
    match fetch_message_priority_stinfosys(stinfo_conn_string).await {
        Ok(t) => {
            persist(&t.0, &t.1).await?;
            Ok(t)
        }
        Err(_) => load_persisted().await,
    }
}

pub async fn setup_refresh_message_priority(
    stinfo_conn_string: Option<&'static str>,
    mut refresh_interval: tokio::time::Interval,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<JoinHandle<()>, Error> {
    let handle = tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                _ = refresh_interval.tick() => {
                    info!("Refreshing message_priority");

                    // TODO: make a metric to track these failures?
                    let _ = fetch_message_priority(stinfo_conn_string).await.inspect_err(|err| {
                        warn!("failed to refresh message_priority from stinfosys: {err}")
                    });
                }
            }
        }
    });

    Ok(handle)
}
