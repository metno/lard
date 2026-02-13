// TODO: doc comment

use std::collections::HashMap;

use chrono::NaiveDateTime;
use tokio_postgres::Client;
use tracing::warn;

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

pub async fn fetch_message_priority(
    client: &Client,
) -> Result<(DefaultTable, ExceptionTable), Error> {
    let default = fetch_message_priority_default(client).await;
    let exception = fetch_message_priority_exception(client).await;

    if default.is_ok() && exception.is_ok() {
        let (default, exception) = (default.unwrap(), exception.unwrap());
        persist(&default, &exception).await?;
        Ok((default, exception))
    } else {
        load_persisted().await
    }
}
