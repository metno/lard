use tracing::{error, info};
use crate::util::{
    levels::{self, LevelTable},
    permissions::{self, PermitTables},
};
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
