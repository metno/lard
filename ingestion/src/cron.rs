use tracing::{error, info};

use crate::util::{
    stinfosys::Stinfosys,
    tsupdate::{self},
};
use util::{stinfofacade::param::ParamTables, DbPools};

pub async fn refresh_from_to((stinfosys, pools, params): &(Stinfosys, DbPools, ParamTables)) {
    info!("Updating timeseries fromtime & totime");

    // TODO: add retries instead of panicking?
    let mut open_conn = pools.open.get().await.unwrap();
    let mut restricted_conn = pools.restricted.get().await.unwrap();

    info!("Caching closed stations and observation programs from StInfoSys");
    let (obs_pgm_times_map, station_times_map) = stinfosys.cache_closed_stinfosys().await.unwrap();

    info!("Updating open and restricted database timeseries");
    let (open_res, restricted_res) = tokio::join!(
        tsupdate::update_from_to(
            &mut open_conn,
            &obs_pgm_times_map,
            &station_times_map,
            params.clone()
        ),
        tsupdate::update_from_to(
            &mut restricted_conn,
            &obs_pgm_times_map,
            &station_times_map,
            params.clone()
        ),
    );

    if let Err(err) = open_res {
        error!("Error while updating open db timeseries: {err}");
    }

    if let Err(err) = restricted_res {
        error!("Error while updating restricted db timeseries: {err}");
    }
}
