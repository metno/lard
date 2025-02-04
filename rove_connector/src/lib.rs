use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, TimeZone, Utc};
use chronoutil::RelativeDuration;
use rove::data_switch::{self, DataCache, DataConnector, SpaceSpec, TimeSpec, Timeseries};
use thiserror::Error;
use tokio_postgres::{types::FromSql, NoTls};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("the connector does not know how to handle this time resolution: {0:?}")]
    UnhandledTimeResolution(RelativeDuration),
    #[error("could not parse param_id as i32")]
    InvalidParamId,
}

type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

#[derive(Debug)]
pub struct Connector {
    pub pool: PgConnectionPool,
}

#[derive(Debug, FromSql)]
#[postgres(name = "obs")]
struct Obs {
    obstime: DateTime<Utc>,
    obsvalue: f64,
}

// TODO: this should probably live somewhere else
#[derive(Debug, FromSql)]
#[postgres(name = "location")]
pub struct Location {
    lat: Option<f64>,
    lon: Option<f64>,
    hamsl: Option<f64>,
    #[postgres(name = "hag")]
    _hag: Option<f64>,
}

fn extract_time_spec(
    time_spec: &TimeSpec,
    num_leading_points: u8,
    num_trailing_points: u8,
) -> Result<(DateTime<Utc>, DateTime<Utc>), data_switch::Error> {
    // TODO: should time_spec just use chrono timestamps instead of unix?
    // IIRC the reason for unix timestamps was easy compatibility with protobuf, but that's
    // less of a priority now
    let start_time = Utc.timestamp_opt(time_spec.timerange.start.0, 0).unwrap()
        - (time_spec.time_resolution * num_leading_points.into());
    let end_time = Utc.timestamp_opt(time_spec.timerange.end.0, 0).unwrap()
        + (time_spec.time_resolution * num_trailing_points.into());

    Ok((start_time, end_time))
}

// TODO: does the input type match postgres-types?
fn regularize(
    obses: Vec<Obs>,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    time_resolution: RelativeDuration,
    expected_len: usize,
) -> Vec<Option<f64>> {
    let mut out = Vec::with_capacity(expected_len);
    let mut curr_obs_time = start_time;

    for obs in obses {
        while curr_obs_time < obs.obstime {
            out.push(None);
            curr_obs_time = curr_obs_time + time_resolution;
        }
        if curr_obs_time == obs.obstime {
            out.push(Some(obs.obsvalue));
            curr_obs_time = curr_obs_time + time_resolution;
        } else {
            // In this case the observation is misaligned, so we should skip it. There's a case
            // to be made for returning an error, but I think we ought to be more robust.
            continue;
        }
    }

    while curr_obs_time <= end_time {
        out.push(None);
        curr_obs_time = curr_obs_time + time_resolution;
    }

    out
}

impl Connector {
    // Needed for the trait, but not currently used in practice. fetch_context is used instead
    pub async fn fetch_one(
        &self,
        ts_id: i64,
        time_spec: &TimeSpec,
        num_leading_points: u8,
        num_trailing_points: u8,
    ) -> Result<DataCache, data_switch::Error> {
        // TODO: matching intervals like this is a hack, but currently necessary to avoid
        // SQL injection. Ideally we could pass an interval type as a query param, which would
        // also save us the query_string allocation, but no ToSql implementations for intervals
        // currently exist in tokio_postgres, so we need to implement it ourselves.
        let interval = match time_spec.time_resolution {
            x if x == RelativeDuration::minutes(1) => "1 minute",
            x if x == RelativeDuration::hours(1) => "1 hour",
            x if x == RelativeDuration::days(1) => "1 day",
            _ => {
                return Err(data_switch::Error::Other(Box::new(
                    Error::UnhandledTimeResolution(time_spec.time_resolution),
                )))
            }
        };

        let (start_time, end_time) =
            extract_time_spec(time_spec, num_leading_points, num_trailing_points)?;

        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| data_switch::Error::Other(Box::new(e)))?;

        // TODO: should this contain an ORDER BY? Actually I think it's not necessary since the
        // order is dictated by the generated sequence
        // TODO: should we drop ts_rule.timestamp from the SELECT? we don't seem to use it
        // TODO: should we make this like the fetch_all query and regularize outside the query?
        // I think this query might perform badly because the join against the generated series
        // doesn't use the index optimally. Doing this would also save us the "interval" mess
        let data_results = conn
            .query(
                "
                SELECT data.obsvalue, ts_rule.timestamp \
                FROM ( \
                    SELECT data.obsvalue, data.obstime \
                    FROM data \
                    WHERE data.timeseries = $1 \
                ) as data \
                RIGHT JOIN generate_series($2::timestamptz, $3::timestamptz, ($4::text)::interval) AS ts_rule(timestamp) \
                    ON data.obstime = ts_rule.timestamp
                ", &[&ts_id, &start_time, &end_time, &interval])
            .await
            .map_err(|e| data_switch::Error::Other(Box::new(e)))?;

        let cache = {
            let mut values = Vec::with_capacity(data_results.len());

            for row in data_results {
                values.push(row.get(0));
            }

            DataCache::new(
                vec![Timeseries {
                    tag: ts_id.to_string(),
                    values,
                }],
                // TODO: we need to either query to get the lat, lon, elev, or change olympian to
                // accept not having them
                vec![],
                vec![],
                vec![],
                time_spec.timerange.start,
                time_spec.time_resolution,
                num_leading_points,
                num_trailing_points,
            )
        };

        Ok(cache)
    }

    // Like fetch one, but for when the current point isn't in the DB yet, so you pass it as an arg
    // Not required (currently) for the trait, but helpful locally
    // num_trailing points is disabled, as this is for fresh data, where trailing points shouldn't
    // be available
    // TODO: largely redundant with fetch_one. We should consolidate at some point, peraps using
    // this function to evolve the trait?
    pub async fn fetch_context(
        &self,
        ts_id: i64,
        timestamp: DateTime<Utc>,
        time_resolution: RelativeDuration,
        num_leading_points: u8,
        datum: Option<f64>,
    ) -> Result<DataCache, data_switch::Error> {
        if num_leading_points == 0 {
            return Ok(DataCache::new(
                vec![Timeseries {
                    tag: ts_id.to_string(),
                    values: vec![datum],
                }],
                // TODO: we need to either query to get the lat, lon, elev, or change olympian to
                // accept not having them
                vec![],
                vec![],
                vec![],
                rove::data_switch::Timestamp(timestamp.timestamp()),
                time_resolution,
                num_leading_points,
                0,
            ));
        }

        // TODO: matching intervals like this is a hack, but currently necessary to avoid
        // SQL injection. Ideally we could pass an interval type as a query param, which would
        // also save us the query_string allocation, but no ToSql implementations for intervals
        // currently exist in tokio_postgres, so we need to implement it ourselves.
        let interval = match time_resolution {
            x if x == RelativeDuration::minutes(1) => "1 minute",
            x if x == RelativeDuration::hours(1) => "1 hour",
            x if x == RelativeDuration::days(1) => "1 day",
            _ => {
                return Err(data_switch::Error::Other(Box::new(
                    Error::UnhandledTimeResolution(time_resolution),
                )))
            }
        };

        let start_time = timestamp - (time_resolution * num_leading_points.into());
        // exclude the current value
        let end_time = timestamp - time_resolution;

        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| data_switch::Error::Other(Box::new(e)))?;

        // TODO: should this contain an ORDER BY? Actually I think it's not necessary since the
        // order is dictated by the generated sequence
        // TODO: should we drop ts_rule.timestamp from the SELECT? we don't seem to use it
        // TODO: should we make this like the fetch_all query and regularize outside the query?
        // I think this query might perform badly because the join against the generated series
        // doesn't use the index optimally. Doing this would also save us the "interval" mess
        let data_results = conn
            .query(
                "
                SELECT data.obsvalue, ts_rule.timestamp \
                FROM ( \
                    SELECT data.obsvalue, data.obstime \
                    FROM data \
                    WHERE data.timeseries = $1 \
                ) as data \
                RIGHT JOIN generate_series($2::timestamptz, $3::timestamptz, ($4::text)::interval) AS ts_rule(timestamp) \
                    ON data.obstime = ts_rule.timestamp
                ", &[&ts_id, &start_time, &end_time, &interval])
            .await
            .map_err(|e| data_switch::Error::Other(Box::new(e)))?;

        let cache = {
            let mut values = Vec::with_capacity(data_results.len() + 1);

            for row in data_results {
                values.push(row.get(0));
            }
            values.push(datum);

            DataCache::new(
                vec![Timeseries {
                    tag: ts_id.to_string(),
                    values,
                }],
                // TODO: we need to either query to get the lat, lon, elev, or change olympian to
                // accept not having them
                vec![],
                vec![],
                vec![],
                rove::data_switch::Timestamp(timestamp.timestamp()),
                time_resolution,
                num_leading_points,
                0,
            )
        };

        Ok(cache)
    }

    async fn fetch_all(
        &self,
        param_id: i32,
        time_spec: &TimeSpec,
        num_leading_points: u8,
        num_trailing_points: u8,
    ) -> Result<DataCache, data_switch::Error> {
        let (start_time, end_time) =
            extract_time_spec(time_spec, num_leading_points, num_trailing_points)?;

        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| data_switch::Error::Other(Box::new(e)))?;

        let data_results = conn
            .query(
                "
                SELECT timeseries.id, data.values, timeseries.loc \
                FROM ( \
                    SELECT timeseries, ARRAY_AGG ((obstime, obsvalue)::obs ORDER BY obstime ASC) as values \
                    FROM data \
                    WHERE obstime BETWEEN $1 AND $2 \
                    GROUP BY timeseries \
                ) as data \
                JOIN timeseries \
                    ON data.timeseries = timeseries.id \
                JOIN labels.met \
                    ON met.timeseries = timeseries.id \
                WHERE met.param_id = $3
                ",
                &[&start_time, &end_time, &param_id],
            )
            .await
            .map_err(|e| data_switch::Error::Other(Box::new(e)))?;

        let cache = {
            let mut data = Vec::with_capacity(data_results.len());
            let mut lats = Vec::with_capacity(data_results.len());
            let mut lons = Vec::with_capacity(data_results.len());
            let mut elevs = Vec::with_capacity(data_results.len());

            let ts_length = {
                let mut ts_length = 0;
                let mut curr_time = start_time;
                while curr_time <= end_time {
                    ts_length += 1;
                    curr_time = curr_time + time_spec.time_resolution;
                }
                ts_length
            };

            for row in data_results {
                let ts_id: i64 = row.get(0);
                let raw_values: Vec<Obs> = row.get(1);
                let loc: Option<Location> = row.get(2);

                // TODO: is there a better way to handle this? If we insert with default latlon we
                // risk corrupting spatial checks, if not we miss QCing data we probably should be
                // QCing... Perhaps we can change the definition of DataCache to accommodate this
                // better?
                if loc.is_none() {
                    continue;
                }
                let loc = loc.unwrap();
                if loc.lat.is_none() || loc.lon.is_none() || loc.hamsl.is_none() {
                    continue;
                }

                data.push(Timeseries {
                    tag: ts_id.to_string(),
                    values: regularize(
                        raw_values,
                        start_time,
                        end_time,
                        time_spec.time_resolution,
                        ts_length,
                    ),
                });
                lats.push(loc.lat.unwrap());
                lons.push(loc.lon.unwrap());
                elevs.push(loc.hamsl.unwrap());
                // to get a sane result out of the integration test (for now) comment the loc
                // Option checks, and uncomment these:
                // lats.push(0.);
                // lons.push(0.);
                // elevs.push(0.);
            }

            DataCache::new(
                data,
                lats,
                lons,
                elevs,
                time_spec.timerange.start,
                time_spec.time_resolution,
                num_leading_points,
                num_trailing_points,
            )
        };

        Ok(cache)
    }
}

#[async_trait]
impl DataConnector for Connector {
    async fn fetch_data(
        &self,
        space_spec: &SpaceSpec,
        time_spec: &TimeSpec,
        num_leading_points: u8,
        num_trailing_points: u8,
        extra_spec: Option<&str>,
    ) -> Result<DataCache, data_switch::Error> {
        match space_spec {
            SpaceSpec::One(ts_id) => {
                self.fetch_one(
                    ts_id
                        .parse()
                        .map_err(|_| data_switch::Error::InvalidSeriesId(ts_id.to_string()))?,
                    time_spec,
                    num_leading_points,
                    num_trailing_points,
                )
                .await
            }
            SpaceSpec::Polygon(_) => unimplemented!(),
            SpaceSpec::All => {
                // TODO: this should probably be in SpaceSpec not ExtraSpec
                let param_id = match extra_spec {
                    Some(param_id_string) => param_id_string.parse().map_err(|_| {
                        data_switch::Error::InvalidExtraSpec {
                            data_source: "lard",
                            extra_spec: extra_spec.map(String::from),
                            source: Box::new(Error::InvalidParamId),
                        }
                    })?,
                    None => {
                        return Err(data_switch::Error::InvalidExtraSpec {
                            data_source: "lard",
                            extra_spec: extra_spec.map(String::from),
                            source: Box::new(Error::InvalidParamId),
                        })
                    }
                };
                self.fetch_all(param_id, time_spec, num_leading_points, num_trailing_points)
                    .await
            }
        }
    }
}
