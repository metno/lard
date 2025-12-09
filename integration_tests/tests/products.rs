use chrono::{Duration, DurationRound, SecondsFormat, TimeDelta, Utc};
use rdkafka::producer::FutureProducer;

use lard_egress::patchwork::PatchworkTables;
use lard_egress::products::ProductTables;

use util::DbPools;

pub mod common;
use common::{
    legacy::{e2e_test_wrapper_legacy, ingest_raw, IngestData},
    Param, TestData,
};

#[tokio::test]
async fn test_products_dew_point() {
    e2e_test_wrapper_legacy(
        async |producer: FutureProducer,
               db_pools: DbPools,
               patchwork_tables: PatchworkTables,
               product_tables: ProductTables| {
            let now = Utc::now().duration_round(TimeDelta::hours(1)).unwrap();
            let eleven_hours_ago =
                Utc::now().duration_round(TimeDelta::hours(1)).unwrap() - Duration::hours(11);

            let data = IngestData::new(vec![TestData {
                station_id: 20001,
                params: vec![Param::new("TA"), Param::new("UU")],
                start_time: eleven_hours_ago,
                period: Duration::hours(1),
                type_id: 501,
                len: 12,
            }]);

            ingest_raw(&data, producer, db_pools, patchwork_tables, product_tables).await;

            // check available
            let url_available =
                "http://localhost:3000/products/available/dew_point_temperature".to_string();

            let resp_available = reqwest::get(url_available).await.unwrap();
            println!("resp_available: {:?}", resp_available);

            let params = format!(
                "?stationids=20001\
                &levels=200\
                &sensors=0\
                &from={}&to={}",
                eleven_hours_ago.to_rfc3339_opts(SecondsFormat::Secs, true),
                now.to_rfc3339_opts(SecondsFormat::Secs, true)
            );

            // get the dew_point of station 20001
            let url = format!("http://localhost:3000/products/dew_point_temperature{params}",);

            let resp = reqwest::get(url).await.unwrap();
            println!("resp: {:?}", resp);
            assert!(resp.status().is_success());
        },
    )
    .await
}
