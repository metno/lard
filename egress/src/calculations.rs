use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use crate::{
    Error,
    patchwork::{self, Truncated, get_patchwork},
    util::{CalculationPatch, CalculationTruncated, merge_patches},
};
use ::util::{PooledPgConn, timerange::Closed};

pub mod humidity;

pub const CALCULATIONS_REQUESTS_RECEIVED: &str = "calculations_requests_received";
pub const CALCULATIONS_AVAILABLE_REQUESTS_RECEIVED: &str =
    "calculations_available_requests_received";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreationParams {
    pub station_id: i32,
    pub param_ids: Vec<i32>, // for calculations, we need to specify multiple paramids
    pub level: Option<i32>,
    pub sensor: Option<i32>,
}

pub async fn get_calculation_data_pair(
    truncated: CalculationTruncated,
    accepted_qc: &[i32],
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, [f64; 2])>, Error> {
    let query = conn
        .prepare(
            r#"SELECT
                obstime,
                param1.original, param2.original,
                param1.corrected, param2.corrected
            FROM (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $1
                AND obstime >= $3 AND obstime < $4
                AND COALESCE(quality_code, -1) = ANY($5::int[])
            ) param1
            INNER JOIN ( 
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $2 
                AND obstime >= $3 AND obstime < $4
                AND COALESCE(quality_code, -1) = ANY($5::int[])
            ) param2
            USING (obstime)"#,
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
        .filter(|(patch, _)| patch.tsids.is_some())
        .map(|(patch, to)| {
            let query = &query;
            let tsid1 = patch.tsids.as_ref().unwrap()[0];
            let tsid2 = patch.tsids.as_ref().unwrap()[1];
            async move {
                conn.query(query, &[&tsid1, &tsid2, &patch.from, &to, &accepted_qc])
                    .await
            }
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            let value1 = row
                .get::<usize, Option<f64>>(3)
                .or(row.get::<usize, Option<f64>>(1));
            let value2 = row
                .get::<usize, Option<f64>>(4)
                .or(row.get::<usize, Option<f64>>(2));
            if value1.is_none() || value2.is_none() {
                continue; // if don't have a value for one of the params, skip this row
            }
            data.push((row.get(0), [value1.unwrap(), value2.unwrap()]));
        }
    }

    Ok(data)
}

pub async fn get_calculation_data_triple(
    truncated: CalculationTruncated,
    accepted_qc: &[i32],
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, [f64; 3])>, Error> {
    let query = conn
        .prepare(
            r#"SELECT
                obstime,
                param1.original, param2.original, param3.original,
                param1.corrected, param2.corrected, param3.corrected
            FROM (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $1
                AND obstime >= $4 AND obstime < $5
                AND COALESCE(quality_code, -1) = ANY($6::int[])
            ) param1
            INNER JOIN (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $2
                AND obstime >= $4 AND obstime < $5
                AND COALESCE(quality_code, -1) = ANY($6::int[])
            ) param2
            USING (obstime)
            INNER JOIN (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $3
                AND obstime >= $4 AND obstime < $5
                AND COALESCE(quality_code, -1) = ANY($6::int[])
            ) param3
            USING (obstime)"#,
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
        .filter(|(patch, _)| patch.tsids.is_some())
        .map(|(patch, to)| {
            let query = &query;
            let tsid1 = patch.tsids.as_ref().unwrap()[0];
            let tsid2 = patch.tsids.as_ref().unwrap()[1];
            let tsid3 = patch.tsids.as_ref().unwrap()[2];
            async move {
                conn.query(
                    query,
                    &[&tsid1, &tsid2, &tsid3, &patch.from, &to, &accepted_qc],
                )
                .await
            }
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            let value1 = row
                .get::<usize, Option<f64>>(4)
                .or(row.get::<usize, Option<f64>>(1));
            let value2 = row
                .get::<usize, Option<f64>>(5)
                .or(row.get::<usize, Option<f64>>(2));
            let value3 = row
                .get::<usize, Option<f64>>(6)
                .or(row.get::<usize, Option<f64>>(3));
            if value1.is_none() || value2.is_none() || value3.is_none() {
                continue; // if don't have a value for one of the params, skip this row
            }
            data.push((
                row.get(0),
                [value1.unwrap(), value2.unwrap(), value3.unwrap()],
            ));
        }
    }

    Ok(data)
}

/// Get patches for multiple parameters and merge them.
pub fn get_merged_patchset(
    parameters: CreationParams,
    timerange: Closed,
    permit_roles: &[i32],
    station_roles: &[i32],
    patchwork_table: Arc<RwLock<patchwork::Table>>,
) -> Result<CalculationTruncated, Error> {
    let patchworks = parameters
        .param_ids
        .iter()
        .map(|param_id| {
            let patchwork_label = patchwork::Label {
                station_id: parameters.station_id,
                param_id: *param_id,
                level: parameters.level,
                sensor: parameters.sensor,
            };
            get_patchwork(
                timerange,
                patchwork_label,
                permit_roles,
                station_roles,
                patchwork_table.clone(),
            )
        })
        .collect::<Result<Vec<Option<Truncated>>, Error>>()?;

    if patchworks.iter().any(|truncated| {
        truncated
            .as_ref()
            // TODO: I think we assure that if there is some truncated, the
            // patchwork is not empty because it will at least contain a hole
            .is_none_or(|truncated| truncated.patchwork.is_empty())
    }) {
        // TODO: should we return an error here? or maybe `Ok(None)`?
        return Ok(CalculationTruncated {
            patchwork: vec![CalculationPatch::new(timerange.from, None)],
            to: timerange.to,
        });
    }

    Ok(merge_patches(
        patchworks
            .into_iter()
            .map(|patchwork| patchwork.unwrap())
            .collect(),
    ))
}
