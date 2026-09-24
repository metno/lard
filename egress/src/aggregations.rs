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
use crate::Error;
use crate::patchwork::{self, Patch, get_applicable_timeseries};
use chrono::{DateTime, Duration, NaiveTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use pg_interval::Interval;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, sync::RwLock};
use tracing::warn;
use util::{PooledPgConn, TsId, timerange::ClosedTimerange};

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
    pub sources: Vec<patchwork::Source>,
    pub aggregation_period: String,
}

#[derive(Debug, Clone)]
pub struct AggregationSpec {
    pub period: AggregationPeriod,
    pub agg_type: AggregationType,
    pub offset_hours: i64,
    pub accepted_qc: Vec<i32>,
    pub count_cutoff: bool,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
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

// need to have time resolution to be able to check the number of expected data points
// create a sort of map of aggregation periods to minimums for certain timeresolutions?
// currently have these marked in the database: P1D, PT12H, PT6H, PT3H, PT1H, PT10M, PT1M
pub type MinCount = Arc<HashMap<AggregationPeriod, Vec<(Interval, i64)>>>;
pub fn minimum_count_timeresolution() -> MinCount {
    let min_count = HashMap::from([
        (
            AggregationPeriod::Hourly,
            vec![
                (Interval::from_duration(Duration::minutes(1)).unwrap(), 60),
                (Interval::from_duration(Duration::minutes(10)).unwrap(), 6),
                (Interval::from_duration(Duration::minutes(60)).unwrap(), 1),
            ],
        ),
        (
            AggregationPeriod::TwiceDaily,
            vec![
                (Interval::from_duration(Duration::minutes(1)).unwrap(), 720),
                (Interval::from_duration(Duration::minutes(10)).unwrap(), 72),
                (Interval::from_duration(Duration::minutes(60)).unwrap(), 12),
                (Interval::from_duration(Duration::hours(3)).unwrap(), 4),
                (Interval::from_duration(Duration::hours(6)).unwrap(), 2),
                (Interval::from_duration(Duration::hours(12)).unwrap(), 1),
            ],
        ),
        (
            AggregationPeriod::Daily,
            vec![
                (Interval::from_duration(Duration::minutes(1)).unwrap(), 1440),
                (Interval::from_duration(Duration::minutes(10)).unwrap(), 144),
                (Interval::from_duration(Duration::minutes(60)).unwrap(), 24),
                (Interval::from_duration(Duration::hours(3)).unwrap(), 8),
                (Interval::from_duration(Duration::hours(6)).unwrap(), 4),
                (Interval::from_duration(Duration::hours(12)).unwrap(), 2),
                (Interval::from_duration(Duration::days(1)).unwrap(), 1),
            ],
        ),
        // TODO: figure out what the minimum counts should be for monthly and yearly aggregations
        // with montly will vary based on number of days in the month, yearly needs to account for leap years
        // need to be calculated from underlying aggregations (as in daily...) - check kdvh triggers
    ]);
    Arc::new(min_count)
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
        AggregationPeriod::Hourly => "date_trunc('hour', data.obstime - $6::interval) + $6",

        AggregationPeriod::TwiceDaily => {
            "date_bin('12 hours', data.obstime - $6::interval, TIMESTAMP '2000-01-01 00:00:00') + $6"
        }

        AggregationPeriod::Daily => "date_trunc('day', data.obstime - $6::interval) + $6",
        // TODO: add other aggregation periods once they are supported
    };
    time_binning.to_string()
}

fn adjust_patch_boundaries(
    mut patches: Vec<Patch>,
    period: AggregationPeriod,
    offset_hours: i64,
) -> Vec<Patch> {
    // post process the patch boundaries if aggregating above hourly
    if matches!(
        period,
        AggregationPeriod::Daily | AggregationPeriod::TwiceDaily
    ) {
        let offset_time: NaiveTime = NaiveTime::from_hms_opt(offset_hours as u32, 0, 0).unwrap();
        // NOTE: move the time forward since assume more likely to have continuation of older timeseries
        for p in patches.iter_mut() {
            p.from = p.from.date_naive().and_time(offset_time).and_utc();
            p.to = p.to.date_naive().and_time(offset_time).and_utc();
        }
    }
    patches
}

pub async fn get_aggregation(
    conn: &PooledPgConn<'_>,
    aggregations_spec: AggregationSpec,
    patchwork_label: patchwork::Label,
    patchwork_table: Arc<RwLock<patchwork::Table>>,
    roles_permit: &[i32],
    roles_station: &[i32],
) -> Result<(Vec<Aggregation>, Vec<patchwork::Source>), Error> {
    // get applicable ts and process the edges between patches
    let patches = get_applicable_timeseries(
        aggregations_spec.from,
        aggregations_spec.to,
        patchwork_label,
        roles_permit,
        roles_station,
        patchwork_table,
    )?;
    let applicable_ts = adjust_patch_boundaries(
        patches,
        aggregations_spec.period,
        aggregations_spec.offset_hours,
    );

    // loop over all the tsid and get the aggregation for each, then combine into a single response
    let mut futures = applicable_ts
        .iter()
        .map(async |ts| {
            get_aggregation_data(
                ts.tsid,
                ts.from,
                ts.to,
                ts.timeresolution,
                aggregations_spec.clone(),
                conn,
            )
            .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut aggregations = Vec::new();

    while let Some(result) = futures.next().await {
        let mut agg = result?;
        aggregations.append(&mut agg);
    }

    let mut sources: Vec<patchwork::Source> = Vec::new();
    for patch in &applicable_ts {
        sources.push(patchwork::Source {
            timerange: ClosedTimerange {
                from: patch.from,
                to: patch.to,
            },
            source_timeseries: Some(patchwork::SourceTimeseries {
                timeresolution: patch.timeresolution,
                typeid: patch.typeid,
            }),
        })
    }
    // this will be empty if nothing could be calculated (404 happens at the handler level)
    Ok((aggregations, sources))
}

async fn get_aggregation_data(
    tsid: TsId,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    timeresolution: Option<Interval>,
    aggregations_spec: AggregationSpec,
    conn: &tokio_postgres::Client,
) -> Result<Vec<Aggregation>, Error> {
    // get the timebinning
    let time_binning = get_binning(aggregations_spec.period);
    // convert the offset to an interval
    let offset_interval =
        pg_interval::Interval::new(0, 0, aggregations_spec.offset_hours * 3_600_000_000);

    // Figure out how to not do the caculation if missing too much data (highly dependent on timeresolution)
    // See Ketil's comment about how it was done in kdvh triggers: https://codeberg.org/metno/lard/pulls/83
    // Have a map of aggregation periods to minimums for certain timeresolutions,
    // construct the query string
    let query_string = match timeresolution {
        Some(timeresolution) => {
            // have time resolution so can try to assess minimum counts
            let min_counts_for_aggregation_period = minimum_count_timeresolution()
                .get(&aggregations_spec.period)
                .cloned();
            let mut min_count = min_counts_for_aggregation_period
                .as_ref()
                .and_then(|counts| counts.iter().find(|(res, _)| *res == timeresolution))
                .map(|(_, count)| *count);

            match min_count {
                Some(count) => {
                    if count == 1 {
                        // this is already of the timeresolution that is being requested - so we should not make an aggregation!
                        // it could be returned if requesting avg or sum perhaps (depending on the element), but not other equations?
                        // TODO: but this means another element might exist that already contains the max/min?
                        return Ok(Vec::<Aggregation>::new());
                    }
                }
                None => return Ok(Vec::<Aggregation>::new()),
            }
            // turn off the min count?
            if !aggregations_spec.count_cutoff {
                min_count = Some(0);
            }
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
                aggregations_spec.agg_type.as_str(),
                time_binning,
                min_count.unwrap(),
            )
        }
        None => return Ok(Vec::<Aggregation>::new()), // cannot do anything with this data since we do not have timeresolution
    };

    let agg_results = conn
        .query(
            query_string.as_str(),
            &[
                &tsid,
                &start_time,
                &end_time,
                &aggregations_spec.accepted_qc,
                &timeresolution.unwrap(),
                &offset_interval,
            ],
        )
        .await;

    match agg_results {
        Ok(rows) => {
            let data = if let Some(_timeresolution) = timeresolution {
                // TODO: do we need timresolution here, or is it more that it necessary to know it exists...?
                // fell back on hours_agg (which comes from it)

                // calculate how many aggregations we should be able to create and preallocate the array
                let period_loop = Duration::hours(aggregations_spec.period.hours());
                let num_aggregations = (end_time - start_time).num_seconds()
                    / (aggregations_spec.period.hours() * 3600);

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
                            bin_end: start_time + period_loop * (i as i32 + 1),
                        }
                    })
                    .collect();

                for row in rows {
                    let value = row.get("agg_value");
                    let bin_start: DateTime<Utc> = row.get("time_bin");

                    let index = ((bin_start - start_time).num_seconds() / period_loop.num_seconds())
                        as usize;

                    data[index].value = value;
                }
                data
            } else {
                let mut data: Vec<Aggregation> = Vec::with_capacity(rows.len());
                for row in rows {
                    let value = row.get("agg_value");
                    let bin_start: DateTime<Utc> = row.get("time_bin");
                    data.push(Aggregation {
                        value,
                        bin_end: bin_start + Duration::hours(aggregations_spec.period.hours()),
                    });
                }
                data
            };
            Ok(data)
        }
        Err(e) => {
            warn!("Error executing aggregation query: {:?}", e);
            Err(Error::Database(e))
        }
    }
}
