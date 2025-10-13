use tokio::time::Interval;
use tracing::{error, info};
use util::DbPools;

use crate::util::{
    levels::{self, LevelTable},
    permissions::{self, PermitTables},
    stinfosys::Stinfosys,
    tsupdate::{self},
};

// TODO: refactor how these two tables are refreshed, since could be more elegantly combined
// (especially if have more tables in the future)
pub async fn refresh_permits(
    (stinfo_conn_string, permit_tables): (String, PermitTables),
    mut interval: Interval,
) {
    loop {
        interval.tick().await;
        info!("Refreshing permit tables");

        // TODO: is the async block needed to drop the mutex? Isn't it dropped after each
        // iteration?
        async {
            // TODO: better error handling here? Nothing is listening to what returns on this task
            // but we could surface failures in metrics. Also we maybe don't want to bork the task
            // forever if these functions fail
            let new_permit_tables = permissions::fetch_permits(&stinfo_conn_string)
                .await
                .unwrap();

            let mut tables = permit_tables.write().unwrap();
            *tables = new_permit_tables;
        }
        .await;
    }
}

pub async fn refresh_levels(
    (stinfo_conn_string, level_table): (String, LevelTable),
    mut interval: Interval,
) {
    loop {
        interval.tick().await;
        info!("Refreshing level tables");

        // TODO: is the async block needed to drop the mutex? Isn't it dropped after each
        // iteration?
        async {
            let new_level_table = levels::fetch_levels(&stinfo_conn_string).await.unwrap();
            let mut tables = level_table.write().unwrap();
            *tables = new_level_table;
        }
        .await;
    }
}

pub async fn refresh_deactivated(
    (stinfo_conn_string, levels, pools): (String, LevelTable, DbPools),
    mut interval: Interval,
) {
    let stinfosys = Stinfosys::new(stinfo_conn_string, levels);

    loop {
        interval.tick().await;
        info!("Updating timeseries totime");

        // TODO: add retries instead of panicking?
        let mut open_conn = pools.open.get().await.unwrap();
        let mut restricted_conn = pools.restricted.get().await.unwrap();

        let (open_res, restricted_res) = tokio::join!(
            tsupdate::set_deactivated(&stinfosys, &mut open_conn),
            tsupdate::set_deactivated(&stinfosys, &mut restricted_conn),
        );

        if let Err(err) = open_res {
            error!("Error while updating open db timeseries: {err}");
        }

        if let Err(err) = restricted_res {
            error!("Error while updating restricted db timeseries: {err}");
        }
    }
}
