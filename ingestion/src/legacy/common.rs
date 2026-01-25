use chrono::{DateTime, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use serde::Deserialize;
use thiserror::Error;

use crate::util::kafka::Offset;
use ::util::stinfofacade::{
    self,
    level::{param_get_level, LevelTable},
    permissions::{timeseries_get_permit, PermitId, PermitTables},
};
use util::PooledPgConn;

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("metadata cache error: {0}")]
    Stinfo(#[from] stinfofacade::Error),
}

#[derive(Debug, Clone, Deserialize)]
pub enum Param {
    Id(i32),
    Code(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct KvalobsId {
    pub station: i32,
    pub param: Param,
    pub typeid: i32,
    pub sensor: i32,
    pub level: i32,
}

#[derive(Debug, Clone)]
pub struct UnlabelledDatum<T: Clone> {
    pub kvid: KvalobsId,
    pub obstime: DateTime<Utc>,
    pub value: T,
}

#[derive(Debug)]
pub struct Datum<T> {
    pub tsid: i64,
    pub obstime: DateTime<Utc>,
    pub value: T,
}

// Query to get a tsid from the relevant source-specific label
pub const QUERY_GET_KVALOBS_STR: &str = r#"
    SELECT timeseries FROM labels.kvalobs
        WHERE station_id = $1
        AND (($2::int IS NULL AND param_id IS NULL) OR (param_id = $2))
        AND (($3::text IS NULL AND param_code IS NULL) OR (param_code = $3))
        AND type_id = $4
        AND (($5::int IS NULL AND lvl IS NULL) OR (lvl = $5))
        AND (($6::int IS NULL AND sensor IS NULL) OR (sensor = $6))
    "#;

async fn create_timeseries<T: Clone>(
    conn: &mut PooledPgConn<'_>,
    raw_datum: &UnlabelledDatum<T>,
    permit: Option<PermitId>,
    level_table: LevelTable,
) -> Result<i64, Error> {
    let (paramid, paramcode) = match &raw_datum.kvid.param {
        Param::Id(id) => (Some(id), None),
        Param::Code(code) => (None, Some(code)),
    };

    let transaction = conn.transaction().await?;

    // lock timseries table so we don't risk duplicate timeseries creation
    //
    // SHARE ROW EXCLUSIVE is chosen because:
    // - it conflicts with itself, so only one of these transactions can run at a time
    // - it does not conflict with ROW SHARE, so SELECTs outside transactions (the happy path of
    //   ingestion, plus egress) can still run.
    //
    // INSERT already acquires SHARE ROW EXCLUSIVE, but the explicit lock here is to make sure it
    // covers the SELECT that checks for an existing label too.
    //
    // We only need to lock public.timeseries and not the labels because the labels exist to
    // describe a timeseries. They should always be there if the timeseries exists, and if it
    // doesn't (i.e the public.timeseries INSERT fails), the transaction will be rolled back.
    //
    // The lock does not need to be explicitly released (in fact there is no way to do that), in
    // postgres locks are tied to transactions and are released when the transaction is committed
    // or rolled back.
    transaction
        .execute(
            "LOCK TABLE public.timeseries IN SHARE ROW EXCLUSIVE MODE",
            &[],
        )
        .await?;

    // re-check for an existing label since the first check was outside the transaction
    let rows = transaction
        .query(
            QUERY_GET_KVALOBS_STR,
            &[
                &raw_datum.kvid.station,
                &paramid,
                &paramcode,
                &raw_datum.kvid.typeid,
                &raw_datum.kvid.level,
                &raw_datum.kvid.sensor,
            ],
        )
        .await?;
    if let Some(row) = rows.first() {
        return Ok(row.get(0));
    }

    // TODO: currently we create a timeseries with null location
    // In the future the location column should be moved to the timeseries metadata table
    let timeseries_id = transaction
        .query_one(
            "INSERT INTO public.timeseries (fromtime, permit) VALUES ($1, $2) RETURNING id",
            &[&raw_datum.obstime, &permit],
        )
        .await?
        .get(0);

    // create source-specific label
    transaction
        .execute(
            "INSERT INTO labels.kvalobs \
        (timeseries, station_id, param_id, param_code, type_id, lvl, sensor) \
    VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &timeseries_id,
                &raw_datum.kvid.station,
                &paramid,
                &paramcode,
                &raw_datum.kvid.typeid,
                &raw_datum.kvid.level,
                &raw_datum.kvid.sensor,
            ],
        )
        .await?;

    // if level does not exist then we can also assume default?
    // but see in stinfosys there isn't always a default...
    let level = match raw_datum.kvid.param {
        Param::Id(id) => param_get_level(level_table.clone(), id, raw_datum.kvid.level)?,
        // No lookup available for paramcodes
        // TODO: is it safe to assume these have no relevant concept of level,
        // or should we try to keep what's reported?
        Param::Code(_) => None,
    };

    // create met label, but only if the paramid is not None
    if paramid.is_some() {
        transaction
            .execute(
                "INSERT INTO labels.met \
        (timeseries, station_id, param_id, type_id, lvl, sensor) \
    VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &timeseries_id,
                    &raw_datum.kvid.station,
                    &paramid,
                    &raw_datum.kvid.typeid,
                    &level, // currently just overrriding the level in the met label
                    &raw_datum.kvid.sensor,
                ],
            )
            .await?;
    }

    transaction.commit().await?;

    Ok(timeseries_id)
}

async fn label<T: Clone>(
    conn: &mut PooledPgConn<'_>,
    raw_data: Vec<(UnlabelledDatum<T>, Option<PermitId>)>,
    query_met: tokio_postgres::Statement,
    level_table: LevelTable,
) -> Result<Vec<Datum<T>>, Error> {
    let mut fails: Vec<usize> = Vec::new();
    let mut data: Vec<Datum<T>> = Vec::new();

    let mut futures = raw_data
        .iter()
        .map(|(raw_datum, _)| async {
            let (paramid, paramcode) = match &raw_datum.kvid.param {
                Param::Id(id) => (Some(id), None),
                Param::Code(code) => (None, Some(code)),
            };

            conn.query(
                &query_met,
                &[
                    &raw_datum.kvid.station,
                    &paramid,
                    &paramcode,
                    &raw_datum.kvid.typeid,
                    &raw_datum.kvid.level,
                    &raw_datum.kvid.sensor,
                ],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>()
        .enumerate();

    while let Some((i, res)) = futures.next().await {
        if let Some(row) = res?.first() {
            let tsid = row.get(0);
            data.push(Datum {
                tsid,
                obstime: raw_data[i].0.obstime,
                //this clone (╥﹏╥)
                value: raw_data[i].0.value.clone(),
            });
        } else {
            fails.push(i);
        }
    }
    // explicit drop is needed to free the borrow of the conn object, so we can use it mutably to
    // create missing timeseries
    drop(futures);

    for i in fails {
        let tsid =
            create_timeseries(conn, &raw_data[i].0, raw_data[i].1, level_table.clone()).await?;
        data.push(Datum {
            tsid,
            obstime: raw_data[i].0.obstime,
            value: raw_data[i].0.value.clone(),
        });
    }

    Ok(data)
}

pub async fn filter_and_label<T: Clone>(
    open_conn: &mut PooledPgConn<'_>,
    restricted_conn: &mut PooledPgConn<'_>,
    raw_buffer: &[(Vec<UnlabelledDatum<T>>, Offset)],
    permit_table: PermitTables,
    level_table: LevelTable,
) -> Result<(Vec<Datum<T>>, Vec<Datum<T>>), Error> {
    let query_met_open = open_conn.prepare(QUERY_GET_KVALOBS_STR).await?;
    let query_met_restricted = restricted_conn.prepare(QUERY_GET_KVALOBS_STR).await?;

    let mut open_raw: Vec<(UnlabelledDatum<T>, Option<PermitId>)> = Vec::new();
    let mut restricted_raw: Vec<(UnlabelledDatum<T>, Option<PermitId>)> = Vec::new();

    for (raw_data_vec, _) in raw_buffer {
        for raw_datum in raw_data_vec {
            let paramid = match raw_datum.kvid.param {
                Param::Id(id) => Some(id),
                Param::Code(_) => None,
            };

            let permit = timeseries_get_permit(
                permit_table.clone(),
                raw_datum.kvid.station,
                raw_datum.kvid.typeid,
                paramid,
            )?;

            let dest = match permit {
                Some(1) => &mut open_raw,
                _ => &mut restricted_raw,
            };
            dest.push((raw_datum.clone(), permit));
        }
    }

    let (open_data, restricted_data) = tokio::join!(
        label(open_conn, open_raw, query_met_open, level_table.clone()),
        label(
            restricted_conn,
            restricted_raw,
            query_met_restricted,
            level_table.clone()
        )
    );

    Ok((open_data?, restricted_data?))
}
