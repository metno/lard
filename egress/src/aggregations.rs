/*!
This file currently attempts to replace aggregations as they were in KDVH.
Aggregations in kvalobs serve different purposes that might need to be covered by
something else in the future, i.e. sending aggregated data to GTS.

According to Ketil, to meet the needs of climate users, KDVH imposed
some restrictions on how aggregations are calculated:
    - source data for an aggregation should have no gaps in that aggregation's time bucket
    - the data must be not be early or late
    - monthly and yearly aggregations are based on daily aggregations
This is to avoid sampling bias where some parts of the day are more heavily sampled in the dataset.
We have replicated this approach here, but Ketil was open to removing these constraints if we could
find another way to enforce/quantify sampling bias.
*/
use std::{sync::Arc, sync::RwLock};

use chrono::{DateTime, Duration, NaiveTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use pg_interval::Interval;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    Error,
    patchwork::{self, Patchwork, SourceTs, Truncated, get_patchwork},
};
use ::util::{
    PooledPgConn,
    timerange::{Closed, closed},
};

/// Other aggregations ------------------------
/// Integral_of_excess and integral_of_deficit:
/// These aggregations should fit neatly in with the others:
/// Count values matching logical operation: < num | <= num | > num | >= num | IN(num1,num2,...)
/// Sum values exceeding threshold (integral_of_excess, integral_of_deficit)
/// That would complete the normal aggregators Ketil had reproduced in ODA.
/// It includes GD17 which Ketil believes we are required to deliver for use in the building industry.
///
/// Over_time elements:
/// Ketil has noted that he is unsure how important these are, and that we might survive without them for a bit?
/// SLAG: 10051, over_time(precipitation_type P1D)
/// SLAGV: 10052, over_time(precipitation_class1 P1D)
/// SLAGW: 10053, over_time(precipitation_type_visual P1D)
/// SLAGWA: 10054, over_time(precipitation_type_automatic P1D)
/// The SLAGs are all diurnal summaries of the most important precipitation code value. For example, SLAGWA will check all
/// automatic observations of coded weather type over the day, and return the code for hail if hail was seen during the day.
/// If not, it will check whether both rain and snow were seen during the day and return the code for mixed precipitation.
/// And so on. The logical structure can get verbose:
/// https://gitlab.met.no/oda/oda/-/blob/main/internal/pkg/abakus/scanners.go?ref_type=heads#L219
///
/// Importantly, the SLAGs do not usually care if some values are missing during the day. They will check whatever is available
/// (which often varies from station to station). The trigger SQLs may be more useful than the ODA code in determining how to
/// do this in SQL.
#[derive(Debug, Serialize, Deserialize)]
pub struct Aggregation {
    pub value: Option<f64>,
    pub bin_end: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub sources: Patchwork,
    pub aggregation_period: String,
}

#[derive(Debug, Clone)]
pub struct AggregationSpec {
    pub label: patchwork::Label,
    pub period: AggregationPeriod,
    pub agg_type: AggregationType,
    pub offset_hours: i64,
    pub accepted_qc: Vec<i32>,
    pub count_cutoff: bool,
}

// TODO: Will we actually stop doing the aggregations in the sql, and rather pull these
// out into rust so as to keep the SQL part more generic / simpler?
#[derive(Debug, Deserialize, Clone, Copy)]
pub enum AggregationType {
    Max,
    Min,
    Avg,
    Sum,
    // TODO: deal with over_time (but these will be exceptions since they are coded values)
    // In addition need to support: integral_of_excess, integral_of_deficit?
    // When using triggers these were calculated together with the max/min/avg,
    // and appear to rely on avg for the calculation, for example I believe GD17 would be something like:
    // GREATEST(0, 17 - AVG(COALESCE(data.corrected, data.original)))
}

impl AggregationType {
    pub fn as_str(&self) -> &str {
        match self {
            AggregationType::Max => "max",
            AggregationType::Min => "min",
            AggregationType::Avg => "avg",
            AggregationType::Sum => "sum",
        }
    }
}

/// Monthly - P1M and Yearly - P1Y ----------------------
/// Based on daily aggregations, be able to calculate monthly and yearly aggregations. At least for avg, max, min, but the
/// full list would possibly require checking all the triggers from KDVH or looking at what Ketil did here:
/// https://gitlab.met.no/oda/oda/-/blob/main/tools/metproducts/productsfull_add.txt?ref_type=heads
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationPeriod {
    Hourly,
    TwiceDaily,
    Daily,
    //Monthly,
    //Yearly,
}
use AggregationPeriod::*;

pub fn get_min_count(period: AggregationPeriod, interval: Interval) -> Option<usize> {
    let interval = interval.to_iso_8601();
    match (period, interval.as_str()) {
        (Hourly, "PT1M") => Some(60),
        (Hourly, "PT10M") => Some(6),
        // this is already of the timeresolution that is being requested - so we should not make an aggregation!
        // it could be returned if requesting avg or sum perhaps (depending on the element), but not other equations?
        // TODO: but this means another element might exist that already contains the max/min?
        //(Hourly, "PT1H") => Some(1),
        (TwiceDaily, "PT1M") => Some(720),
        (TwiceDaily, "PT10M") => Some(72),
        (TwiceDaily, "PT1H") => Some(12),
        (TwiceDaily, "PT3H") => Some(4),
        (TwiceDaily, "PT6H") => Some(2),
        //(TwiceDaily, "PT12H") => Some(1),
        (Daily, "PT1M") => Some(1440),
        (Daily, "PT10M") => Some(144),
        (Daily, "PT1H") => Some(24),
        (Daily, "PT3H") => Some(8),
        (Daily, "PT6H") => Some(4),
        (Daily, "PT12H") => Some(2),
        //(Daily, "P1D") => Some(1),
        _ => None,
    }
}

impl AggregationPeriod {
    pub fn as_str(&self) -> &str {
        match self {
            AggregationPeriod::Hourly => "PT1H",
            AggregationPeriod::TwiceDaily => "PT12H",
            AggregationPeriod::Daily => "P1D",
            // AggregationPeriod::Monthly => "P1M",
            // AggregationPeriod::Yearly => "P1Y",
        }
    }
}

impl AggregationPeriod {
    pub fn hours(&self) -> i64 {
        match self {
            AggregationPeriod::Hourly => 1,
            AggregationPeriod::TwiceDaily => 12,
            AggregationPeriod::Daily => 24,
        }
    }
}

fn get_binning(period: AggregationPeriod) -> String {
    let time_binning = match period {
        AggregationPeriod::Hourly => "date_trunc('hour', data.obstime - $6::interval, 'UTC') + $6",

        AggregationPeriod::TwiceDaily => {
            "date_bin('12 hours', data.obstime - $6::interval, TIMESTAMP '2000-01-01 00:00:00') + $6"
        }

        AggregationPeriod::Daily => "date_trunc('day', data.obstime - $6::interval, 'UTC') + $6",
        // TODO: add other aggregation periods once they are supported
    };
    time_binning.to_string()
}

fn get_patchwork_offset(
    timerange: Closed,
    aggregation_spec: AggregationSpec,
    patchwork_table: Arc<RwLock<patchwork::Table>>,
    roles_permit: &[i32],
    roles_station: &[i32],
) -> Result<Option<Truncated>, Error> {
    let Some(mut truncated) = get_patchwork(
        timerange,
        aggregation_spec.label,
        roles_permit,
        roles_station,
        patchwork_table,
    )?
    else {
        return Ok(None);
    };

    // post process the patch boundaries if aggregating above hourly
    if matches!(
        aggregation_spec.period,
        AggregationPeriod::Daily | AggregationPeriod::TwiceDaily
    ) {
        let offset_time: NaiveTime =
            NaiveTime::from_hms_opt(aggregation_spec.offset_hours as u32, 0, 0).unwrap();
        // NOTE: move the time forward since assume more likely to have continuation of older timeseries
        for p in truncated.patchwork.iter_mut() {
            p.from = p.from.date_naive().and_time(offset_time).and_utc();
        }
        truncated.to = truncated.to.date_naive().and_time(offset_time).and_utc();
    }
    Ok(Some(truncated))
}

pub async fn get_aggregation(
    conn: &PooledPgConn<'_>,
    timerange: Closed,
    aggregation_spec: AggregationSpec,
    patchwork_table: Arc<RwLock<patchwork::Table>>,
    roles_permit: &[i32],
    roles_station: &[i32],
) -> Result<(Vec<Aggregation>, Patchwork), Error> {
    // get patchwork and process the edges between patches
    let Some(truncated) = get_patchwork_offset(
        timerange,
        aggregation_spec.clone(),
        patchwork_table,
        roles_permit,
        roles_station,
    )?
    else {
        return Ok((vec![], vec![]));
    };

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
        .map(|(patch, to)| {
            get_aggregation_data(
                closed(patch.from, to),
                patch.source,
                aggregation_spec.clone(),
                conn,
            )
        })
        .collect::<FuturesOrdered<_>>();

    let mut aggregations = Vec::new();

    while let Some(result) = futures.next().await {
        let mut agg = result?;
        aggregations.append(&mut agg);
    }

    // this will be empty if nothing could be calculated (404 happens at the handler level)
    Ok((aggregations, truncated.patchwork))
}

async fn get_aggregation_data(
    timerange: Closed,
    source: Option<SourceTs>,
    aggregation_spec: AggregationSpec,
    conn: &tokio_postgres::Client,
) -> Result<Vec<Aggregation>, Error> {
    // calculate how many aggregations we should be able to create and preallocate the array
    let period_loop = Duration::hours(aggregation_spec.period.hours());
    let num_aggregations =
        (timerange.to - timerange.from).num_seconds() / (aggregation_spec.period.hours() * 3600);
    eprintln!("num_aggregations: {}", num_aggregations);

    // We populate with Nones since we cannot be sure that when we get the data we will have an aggregation
    // for each time step, so this way we return a regularlized timeseries with empty values where we are missing
    // enough data for an aggregation
    let mut data: Vec<Aggregation> = (0..num_aggregations)
        .map(|i| {
            Aggregation {
                value: None,
                // Bin end is used in the output since that is the way it has been done in the climate division
                // that a time interval is represented by the end of the interval. The +1 is needed to start the count
                // at 1 instead of 0 (as is done in the code).
                bin_end: timerange.from + period_loop * (i as i32 + 1),
            }
        })
        .collect();

    let Some(source) = source else {
        // in case of a hole, we still need the `None`s to have regular output
        return Ok(data);
    };

    let time_binning = get_binning(aggregation_spec.period);
    let offset_interval =
        pg_interval::Interval::new(0, 0, aggregation_spec.offset_hours * 3_600_000_000);

    // Figure out how to not do the caculation if missing too much data (highly dependent on timeresolution)
    // See Ketil's comment about how it was done in kdvh triggers: https://codeberg.org/metno/lard/pulls/83
    // Have a map of aggregation periods to minimums for certain timeresolutions,
    // construct the query string
    let query_string = {
        let min_count = if aggregation_spec.count_cutoff {
            if let Some(timeresolution) = source.timeresolution
                && let Some(min_count) = get_min_count(aggregation_spec.period, timeresolution)
            {
                min_count
            } else {
                return Ok(data);
            }
        } else {
            0
        };

        format!(
            r#"
                    SELECT
                        {}(COALESCE(data.corrected, data.original)) AS agg_value,
                        {} AS time_bin
                    FROM legacy.data AS data
                    WHERE
                        data.timeseries = $1
                        AND data.obstime >= $2::timestamptz
                        AND data.obstime < $3::timestamptz
                        AND COALESCE(data.quality_code, -1) = ANY($4::int[])
                        AND MOD(
                            EXTRACT(EPOCH FROM (data.obstime - $2::timestamptz)),
                            EXTRACT(EPOCH FROM $5::interval)
                        ) = 0
                    GROUP BY time_bin
                    HAVING COUNT(*) >= {};
                    "#,
            aggregation_spec.agg_type.as_str(),
            time_binning,
            min_count,
        )
    };
    eprintln!(
        "tsid: {}, timerange: {:?}, source.timeres: {:?}, offset: {:?}",
        source.tsid, timerange, source.timeresolution, offset_interval
    );

    let agg_results = conn
        .query(
            query_string.as_str(),
            &[
                &source.tsid,
                &timerange.from,
                &timerange.to,
                &aggregation_spec.accepted_qc,
                &source.timeresolution,
                &offset_interval,
            ],
        )
        .await;

    let rows = match agg_results {
        Ok(rows) => rows,
        Err(e) => {
            warn!("Error executing aggregation query: {:?}", e);
            return Err(Error::Database(e));
        }
    };

    for row in rows {
        let value = row.get("agg_value");
        let bin_start: DateTime<Utc> = row.get("time_bin");

        let index =
            ((bin_start - timerange.from).num_seconds() / period_loop.num_seconds()) as usize;

        eprintln!("index: {}", index);

        data[index].value = value;
    }

    Ok(data)
}
