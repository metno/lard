//! Collapses multiple timeseries representing the same data that differ only
//! in typeid/timeresolution into one, selecting whichever is highest priority
//! (as defined in metadata) at a given time
//!
//! Adapted from [ODAs concept of "filter"]
//! (https://gitlab.met.no/oda/oda/-/blob/main/internal/cron/filtergen/filtergen.go?ref_type=heads)
//! We renamed to patchwork since we think it is a name that better describes
//! what this does: aka create a patchwork of timeseries to give one overall
//! timeseries. This is for reference since parts of it are reused in some way
//! here. Most specifically the calls for metadata from stinfosys. The
//! algorithm itself for creating a "filter" timeseries was redone in rust, but
//! the idea remains the same - give the most recomended timeseries at any
//! given time (and thus avoid giving multiple overlaping timeseries).

use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use pg_interval::Interval;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::Error;
use ::util::{
    PooledPgConn, TsId,
    interval::{de_optional_interval_iso8601, ser_optional_interval_iso8601},
    timerange::{ClosedTimerange, OpenTimerange},
};

pub use ::util::patchwork::Label;

pub mod table;

pub use table::{Table, Tables};

pub const PATCHWORK_FUTURES_FAILURES: &str = "patchwork_futures_failures";

#[derive(Debug, Clone)]
pub struct Patch {
    pub tsid: TsId,
    pub timeresolution: Option<Interval>,
    pub typeid: i32,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl Patch {
    pub fn overlap(&self, other: &Self) -> Option<ClosedTimerange> {
        let this_timerange = ClosedTimerange::new(self.from, self.to);
        let other_timerange = ClosedTimerange::new(other.from, other.to);

        this_timerange.overlap(other_timerange)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Datum {
    // can assume have original and timestamp? (the field for original value
    // can technically be null (database schema wise)...)
    // definitely do not always have corrected and quality code (eg. before qc'ing)
    pub original: Option<f64>,
    pub timestamp: DateTime<Utc>,
    pub corrected: Option<f64>,
    pub quality_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceTimeseries {
    #[serde(
        deserialize_with = "de_optional_interval_iso8601",
        serialize_with = "ser_optional_interval_iso8601"
    )]
    pub timeresolution: Option<Interval>,
    pub typeid: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub timerange: ClosedTimerange,
    // keep this part as an option so that we could use this struct to explicitly list holes if desired
    pub source_timeseries: Option<SourceTimeseries>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub sources: Vec<Source>,
}

/// Checks if the given `label` exists in the patchwork `table` and extracts a vector of time
/// sorted timeseries patches given the bounds of the input `from` and `to` times
pub fn get_applicable_timeseries(
    timerange: ClosedTimerange,
    label: Label,
    roles_permit: &[i32],
    roles_station: &[i32],
    table: Arc<RwLock<Table>>,
) -> Result<Vec<Patch>, Error> {
    // the table we are currenntly looking at (either open or closed)
    let t = table.read()?;
    let Some(timeseries) = t.get(&label) else {
        // Label not found, therefore no timeseries are applicable
        return Ok(vec![]);
    };

    let request_fromto = OpenTimerange {
        from: Some(timerange.from),
        to: Some(timerange.to),
    };

    // TODO: if the label has none for sensor / level should it match on all???
    // create a structure to keep what is applicable
    let applicable_ts: Vec<_> = timeseries
        .iter()
        .filter(|ts| {
            ts.permit == 1
                || roles_permit.contains(&ts.permit)
                || roles_station.contains(&label.station_id)
        })
        .filter_map(|ts| {
            let overlap = request_fromto.overlap(OpenTimerange {
                from: Some(ts.from),
                to: ts.to,
            })?;

            Some(Patch {
                tsid: ts.tsid,
                timeresolution: ts.timeresolution,
                typeid: ts.typeid,
                from: overlap.from.unwrap(),
                to: overlap.to.unwrap_or(timerange.to),
            })
        })
        .collect();

    // TODO: should this return an error if empty?
    Ok(applicable_ts)
}

pub async fn get_patchwork(
    conn: &PooledPgConn<'_>,
    timerange: ClosedTimerange,
    label: Label,
    table: Arc<RwLock<Table>>,
    permit_roles: &[i32],
    station_roles: &[i32],
) -> Result<(Vec<Datum>, Vec<Source>), Error> {
    // get ts that are applicable for this lable from the background patchwork table
    let applicable_ts =
        get_applicable_timeseries(timerange, label, permit_roles, station_roles, table)?;

    if applicable_ts.is_empty() {
        return Ok((vec![], vec![]));
    }

    let query = conn
        .prepare(
            "SELECT timeseries, obstime, original, corrected, quality_code \
            FROM legacy.data \
            WHERE timeseries = $1 \
                AND obstime >= $2 \
                AND obstime < $3 \
            ORDER BY obstime",
        )
        .await?;

    let mut futures = applicable_ts
        .iter()
        .map(async |patch| {
            conn.query(&query, &[&patch.tsid, &patch.from, &patch.to])
                .await
        })
        .collect::<FuturesOrdered<_>>()
        .enumerate();

    let mut data: Vec<Datum> = Vec::new();

    while let Some((i, res)) = futures.next().await {
        let rows = match res {
            Ok(val) => val,
            Err(err) => {
                // log these fails
                metrics::counter!(PATCHWORK_FUTURES_FAILURES).increment(1);
                error!("patchwork future failed: {}, {}", i, err);
                continue;
            }
        };
        for row in rows {
            data.push(Datum {
                original: row.get(2),
                timestamp: row.get(1),
                corrected: row.get(3),
                quality_code: row.get(4),
            });
        }
    }
    let mut sources: Vec<Source> = Vec::new();
    for patch in &applicable_ts {
        sources.push(Source {
            timerange: ClosedTimerange {
                from: patch.from,
                to: patch.to,
            },
            source_timeseries: Some(SourceTimeseries {
                timeresolution: patch.timeresolution,
                typeid: patch.typeid,
            }),
        })
    }

    Ok((data, sources))
}
