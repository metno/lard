use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use crate::{
    Error,
    patchwork::{self, Patch, get_applicable_timeseries},
    util::{CalculationPatch, merge_patches},
};
use ::util::PooledPgConn;

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
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    accepted_qc: &[i32],
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, [(f64, Option<i32>); 2])>, Error> {
    let query = conn
        .prepare(
            r#"SELECT
                obstime,
                param1.original, param2.original,
                param1.corrected, param2.corrected,
                param1.quality_code, param2.quality_code
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

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(
                &query,
                &[&patch.tsids[0], &patch.tsids[1], &from, &to, &accepted_qc],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            if let Some((val1, val2)) = row
                .get::<usize, Option<f64>>(1)
                .or(row.get::<usize, Option<f64>>(3))
                .zip(
                    row.get::<usize, Option<f64>>(2)
                        .or(row.get::<usize, Option<f64>>(4)),
                )
            {
                data.push((
                    row.get(0),
                    [
                        (val1, row.get::<usize, Option<i32>>(5)),
                        (val2, row.get::<usize, Option<i32>>(6)),
                    ],
                ));
            }
        }
    }

    Ok(data)
}

pub async fn get_calculation_data_triple(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    accepted_qc: &[i32],
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, [(f64, Option<i32>); 3])>, Error> {
    let query = conn
        .prepare(
            r#"SELECT
                obstime,
                param1.original, param2.original, param3.original,
                param1.corrected, param2.corrected, param3.corrected,
                param1.quality_code, param2.quality_code, param3.quality_code
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

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(
                &query,
                &[
                    &patch.tsids[0],
                    &patch.tsids[1],
                    &patch.tsids[2],
                    &from,
                    &to,
                    &accepted_qc,
                ],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            let value1 = row
                .get::<usize, Option<f64>>(1)
                .or(row.get::<usize, Option<f64>>(4));
            let value2 = row
                .get::<usize, Option<f64>>(2)
                .or(row.get::<usize, Option<f64>>(5));
            let value3 = row
                .get::<usize, Option<f64>>(3)
                .or(row.get::<usize, Option<f64>>(6));
            if value1.is_none() || value2.is_none() || value3.is_none() {
                continue; // if don't have a value for one of the params, skip this row
            }
            data.push((
                row.get(0),
                [
                    (value1.unwrap(), row.get::<usize, Option<i32>>(7)),
                    (value2.unwrap(), row.get::<usize, Option<i32>>(8)),
                    (value3.unwrap(), row.get::<usize, Option<i32>>(9)),
                ],
            ));
        }
    }

    Ok(data)
}

/// Get patches for multiple parameters and merge them.
pub fn get_merged_patchset(
    parameters: CreationParams,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    permit_roles: &[i32],
    station_roles: &[i32],
    patchwork_table: Arc<RwLock<patchwork::Table>>,
) -> Result<Vec<CalculationPatch>, Error> {
    let patches = parameters
        .param_ids
        .iter()
        .map(|param_id| {
            let patchwork_label = patchwork::Label {
                station_id: parameters.station_id,
                param_id: *param_id,
                level: parameters.level,
                sensor: parameters.sensor,
            };
            get_applicable_timeseries(
                from,
                to,
                patchwork_label,
                permit_roles,
                station_roles,
                patchwork_table.clone(),
            )
        })
        .collect::<Result<Vec<Vec<Patch>>, Error>>()?;

    Ok(merge_patches(patches))
}

/// wrapper to apply a calculation to some data, while coalescing the QC of the data together
pub fn apply_calc2(
    timestamp: DateTime<Utc>,
    data: [(f64, Option<i32>); 2],
    f: impl Fn(f64, f64) -> f64,
) -> (DateTime<Utc>, f64) {
    // apply the calculation function to the values of the two params
    // choosing the corrected value if it exists, otherwise the original value
    let value = match (data[0].1.is_some(), data[1].1.is_some()) {
        (true, true) => f(data[0].1.unwrap() as f64, data[1].1.unwrap() as f64),
        (true, false) => f(data[0].1.unwrap() as f64, data[1].0),
        (false, true) => f(data[0].0, data[1].1.unwrap() as f64),
        (false, false) => f(data[0].0, data[1].0),
    };
    (timestamp, value)
}

/// see `[apply_calc2]` but for calculations that take 3 inputs
pub fn apply_calc3(
    timestamp: DateTime<Utc>,
    data: [(f64, Option<i32>); 3],
    f: impl Fn(f64, f64, f64) -> f64,
) -> (DateTime<Utc>, f64) {
    // apply the calculation function to the values of the three params
    // choosing the corrected value if it exists, otherwise the original value
    let value = match (
        data[0].1.is_some(),
        data[1].1.is_some(),
        data[2].1.is_some(),
    ) {
        (true, true, true) => f(
            data[0].1.unwrap() as f64,
            data[1].1.unwrap() as f64,
            data[2].1.unwrap() as f64,
        ),
        (true, true, false) => f(
            data[0].1.unwrap() as f64,
            data[1].1.unwrap() as f64,
            data[2].0,
        ),
        (true, false, true) => f(
            data[0].1.unwrap() as f64,
            data[1].0,
            data[2].1.unwrap() as f64,
        ),
        (true, false, false) => f(data[0].1.unwrap() as f64, data[1].0, data[2].0),
        (false, true, true) => f(
            data[0].0,
            data[1].1.unwrap() as f64,
            data[2].1.unwrap() as f64,
        ),
        (false, true, false) => f(data[0].0, data[1].1.unwrap() as f64, data[2].0),
        (false, false, true) => f(data[0].0, data[1].0, data[2].1.unwrap() as f64),
        (false, false, false) => f(data[0].0, data[1].0, data[2].0),
    };
    (timestamp, value)
}
