use tracing::info;
use util::DbPools;

use crate::patchwork::{fetch_patchwork_table, PatchworkTables};

pub async fn refresh_patchwork(
    (stinfo_conn_string, pools, patchwork_table): &(String, DbPools, PatchworkTables),
) {
    info!("Refreshing patchwork table");

    let open_conn = &pools.open.get().await.unwrap();
    let restricted_conn = &pools.restricted.get().await.unwrap();

    // TODO: is the async block needed to drop the mutex? Isn't it dropped after each
    // iteration?
    let new_open_table = fetch_patchwork_table(open_conn, stinfo_conn_string)
        .await
        .unwrap();

    let new_restricted_table = fetch_patchwork_table(restricted_conn, stinfo_conn_string)
        .await
        .unwrap();

    let mut open_table = patchwork_table.open.write().unwrap();
    *open_table = new_open_table;

    let mut restricted_table = patchwork_table.restricted.write().unwrap();
    *restricted_table = new_restricted_table;
}
