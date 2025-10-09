use std::future::Future;

use util::MetLabel;

use crate::{util::tsupdate::DeactivatedTimeseries, Error};

// TODO: should this trait abstract away permits and levels? And/or abstract away periodic updates
// of metadata?
pub trait MetadataFetch {
    fn fetch_deactivated(
        &self,
        labels: Vec<MetLabel>,
    ) -> impl Future<Output = Result<Vec<DeactivatedTimeseries>, Error>> + Send;
}
