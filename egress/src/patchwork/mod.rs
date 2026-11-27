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

use std::{
    ops::Deref,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use pg_interval::Interval;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::Error;
use ::util::{
    PermitId, PooledPgConn, TsId,
    interval::{de_optional_interval_iso8601, ser_optional_interval_iso8601},
    timerange::{Closed, half_open},
};

pub use ::util::patchwork::Label;

pub mod table;

pub use table::{Table, Tables};

pub const PATCHWORK_FUTURES_FAILURES: &str = "patchwork_futures_failures";

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SourceTs {
    // internal implementation detail that should not be exposed to API users
    #[serde(skip)]
    pub tsid: TsId,
    // TODO: we should maybe offer this one though?
    // FIXME: available assumes these are all the same. That seems a reasonable
    // assumption, but we should probably enforce it by moving this higher in
    // the type hierarchy
    #[serde(skip)]
    pub permit: PermitId,
    #[serde(
        deserialize_with = "de_optional_interval_iso8601",
        serialize_with = "ser_optional_interval_iso8601"
    )]
    pub timeresolution: Option<Interval>,
    // TODO: this spelling conflicts with elsewhere in codebase (missing underscore)
    // is this intentional? for the API?
    pub typeid: i32,
}

impl SourceTs {
    pub fn new(
        tsid: TsId,
        permit: PermitId,
        timeresolution: Option<Interval>,
        typeid: i32,
    ) -> Self {
        Self {
            tsid,
            permit,
            timeresolution,
            typeid,
        }
    }
}

/// Tells you which source timeseries to use data from, in the timerange from
/// this patch's `from` until the next patch's `from`.
///
/// Should be understood in the context of [`Patchwork`], one patch has no
/// meaning alone since it has no `to` time. We considered adding explicit
/// to times, but the problem is they would have to be optional, since the
/// last patch (or hole) is open ended, but all the patches before must never
/// have `to == None`, so this creates the possibility of invalid states.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Patch {
    pub from: DateTime<Utc>,
    /// None means there is a hole, i.e. no timeseries could be found to patch
    /// this timerange
    pub source: Option<SourceTs>,
}

impl Patch {
    pub fn new(from: DateTime<Utc>, source: Option<SourceTs>) -> Self {
        Self { from, source }
    }
}

/// Instructions for how to make a patchworked timeseries, sequentially listing
/// which source timeseries to select for each timerange.
pub type Patchwork = Vec<Patch>;

/// A [`Patchwork`] that has been truncated to a [`Closed`] timerange.
///
/// The first [`Patch`]'s `from` should be the same as the timerange's from,
/// and `to` should be the timerange's `to`.
///
/// Returned by [`get_patchwork`] to make it more ergonomic to fetch data with,
/// as we (always?) fetch data within specified bounds.
#[derive(Debug, Clone)]
pub struct Truncated {
    pub patchwork: Patchwork,
    pub to: DateTime<Utc>, // would there be any reason to make this optional?
}

// FIXME: I think this is actually not patchwork specific, and instead shared
// by all irregular data
/// Data point in a patchwork timeseries
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Datum {
    // can assume have original and timestamp? (the field for original value
    // can technically be null (database schema wise)...)
    // definitely do not always have corrected and quality code (eg. before qc'ing)
    // TODO: ordering wise does it really make sense to have original and
    // corrected split up?? It would technically be a breaking change, but
    // frost is our only user for now and could probably handle it fine?
    pub original: Option<f64>,
    pub timestamp: DateTime<Utc>,
    pub corrected: Option<f64>,
    pub quality_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub sources: Patchwork,
}

// TODO: TEST ME
/// Fetches the [`Patchwork`] for a given [`Label`] from the given [`Table`]
/// (which is only one out of restricted or open, so two calls are required to
/// read both), restricted to `timerange` and filtered according to auth roles
pub fn get_patchwork(
    timerange: Closed,
    label: Label,
    roles_permit: &[i32],
    roles_station: &[i32],
    table: &Table,
) -> Option<Truncated> {
    // TODO: if the label has none for sensor / level should it match on all???
    let patchwork = table.get(&label)?;

    if !roles_station.contains(&label.station_id)
        // more strict than it needs to be, assumes all permits are the same.
        // should reduce strictness or encode assumption in types
        && !patchwork.iter().all(|patch| {
            patch
                .source
                .is_none_or(|patch| patch.permit == 1 || roles_permit.contains(&patch.permit))
        })
    {
        return None;
    }

    // could be more efficient, but probably not worth it since
    // `patchwork.len()` is expected to be small
    let mut constrained = Vec::new();
    if patchwork
        .first()
        .is_none_or(|first| timerange.from < first.from)
    {
        constrained.push(Patch::new(timerange.from, None))
    }
    for (i, patch) in patchwork.iter().enumerate() {
        let patch_range = half_open(patch.from, patchwork.get(i + 1).map(|next| next.from));
        if let Some(overlap) = timerange.overlap_half_open(patch_range) {
            constrained.push(Patch::new(overlap.from, patch.source));
        }
    }

    Some(Truncated {
        patchwork: constrained,
        to: timerange.to,
    })
}

pub async fn get_patchwork_data(
    conn: &PooledPgConn<'_>,
    timerange: Closed,
    label: Label,
    table: Arc<RwLock<Table>>,
    permit_roles: &[i32],
    station_roles: &[i32],
) -> Result<(Patchwork, Vec<Datum>), Error> {
    let Some(truncated) = get_patchwork(
        timerange,
        label,
        permit_roles,
        station_roles,
        table.read().unwrap().deref(),
    ) else {
        return Ok((vec![], vec![]));
    };
    let patchwork_ret = truncated.patchwork.clone();

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

    let mut futures = truncated
        .patchwork
        .iter()
        .zip(
            truncated
                .patchwork
                .iter()
                .map(|patch| patch.from)
                .chain(std::iter::once(truncated.to))
                .skip(1),
        )
        // ignore holes
        .filter(|(patch, _)| patch.source.is_some())
        .map(|(patch, to)| {
            let query = &query;
            let tsid = patch.source.as_ref().unwrap().tsid;
            async move { conn.query(query, &[&tsid, &patch.from, &to]).await }
        })
        .collect::<FuturesOrdered<_>>();

    let mut data: Vec<Datum> = Vec::new();

    while let Some(res) = futures.next().await {
        let rows = match res {
            Ok(val) => val,
            Err(err) => {
                // log these fails
                // TODO: is this really the correct behaviour? I think we
                // should be returning an error
                metrics::counter!(PATCHWORK_FUTURES_FAILURES).increment(1);
                error!("patchwork future failed: {}", err);
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

    Ok((patchwork_ret, data))
}
