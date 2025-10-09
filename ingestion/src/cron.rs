use tracing::{error, info};
use util::DbPools;

use crate::util::{
    levels::{self, LevelTable},
    permissions::{self, PermitTables},
    stinfosys::Stinfosys,
    tsupdate::{self},
};

const HOUR: u64 = 3600;

// TODO: refactor how these two tables are refreshed, since could be more elegantly combined
// (especially if have more tables in the future)
pub async fn refresh_permits(stinfo_conn_string: String, background_permit_tables: PermitTables) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

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
            let mut tables = background_permit_tables.write().unwrap();
            *tables = new_permit_tables;
        }
        .await;
    }
}

pub async fn refresh_levels(stinfo_conn_string: String, background_level_table: LevelTable) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

    loop {
        interval.tick().await;
        info!("Refreshing level tables");

        // TODO: is the async block needed to drop the mutex? Isn't it dropped after each
        // iteration?
        async {
            let new_level_table = levels::fetch_levels(&stinfo_conn_string).await.unwrap();
            let mut tables = background_level_table.write().unwrap();
            *tables = new_level_table;
        }
        .await;
    }
}

pub async fn refresh_deactivated(stinfo_conn_string: String, levels: LevelTable, pools: DbPools) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(12 * HOUR));
    let stinfosys = Stinfosys::new(stinfo_conn_string, levels);

    loop {
        interval.tick().await;
        info!("Updating timeseries totime");

        // TODO: add retries instead of panicking?
        let open_conn = pools.open.get().await.unwrap();
        let restricted_conn = pools.restricted.get().await.unwrap();

        let (open_res, restricted_res) = tokio::join!(
            tsupdate::set_deactivated(&stinfosys, &open_conn),
            tsupdate::set_deactivated(&stinfosys, &restricted_conn),
        );

        if let Err(err) = open_res {
            error!("Error while updating open db timeseries: {err}");
        }

        if let Err(err) = restricted_res {
            error!("Error while updating open db timeseries: {err}");
        }
    }
}
