use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::future::Future;

use util::{MetLabel, MetTimeseriesKey};

use crate::{util::tsupdate::DeactivatedTimeseries, Error};

// TODO: should this trait abstract away permits and levels? And/or abstract away periodic updates
// of metadata?
#[allow(clippy::type_complexity)]
pub trait MetadataFetch {
    fn cache_deactivated_stinfosys(
        &self,
    ) -> impl Future<
        Output = Result<
            (
                HashMap<i32, DateTime<Utc>>,
                HashMap<MetTimeseriesKey, DateTime<Utc>>,
            ),
            Error,
        >,
    > + Send;
    fn fetch_deactivated(
        &self,
        obs_pgm_totime: &HashMap<MetTimeseriesKey, DateTime<Utc>>,
        station_totime: &HashMap<i32, DateTime<Utc>>,
        labels: Vec<MetLabel>,
    ) -> impl Future<Output = Result<Vec<DeactivatedTimeseries>, Error>> + Send;
}
