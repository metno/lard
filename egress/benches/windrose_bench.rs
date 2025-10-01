use bb8_postgres::PostgresConnectionManager;
use chrono::{TimeZone, Utc};
use criterion::{criterion_group, criterion_main, Criterion};
use lard_egress::{get_wind_days, WindPatch, Windrose, DIRECTION_AXIS, SPEED_AXIS};
use tokio_postgres::NoTls;
use util::PooledPgConn;

const SPEED_TSID: i64 = 1649;
const DIRECTION_TSID: i64 = 1645;

pub async fn rust_bench<'a>(patches: &[WindPatch], conn: &PooledPgConn<'_>) -> Windrose<'a> {
    let days = get_wind_days(patches, None, conn).await.unwrap();
    Windrose::new_from_days(SPEED_AXIS, DIRECTION_AXIS, days)
}

pub async fn sql_bench<'a>(patches: &[WindPatch], conn: &PooledPgConn<'_>) -> Windrose<'a> {
    Windrose::new_from_sql(SPEED_AXIS, DIRECTION_AXIS, patches, None, conn)
        .await
        .unwrap()
}

pub fn histogram_benchmark(c: &mut Criterion) {
    let patches = vec![WindPatch {
        speed_tsid: SPEED_TSID,
        direction_tsid: DIRECTION_TSID,
        from: Utc.with_ymd_and_hms(1990, 1, 1, 0, 0, 0).unwrap(),
        to: Utc.with_ymd_and_hms(2006, 1, 1, 0, 0, 0).unwrap(),
    }];

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let open_manager = PostgresConnectionManager::new_from_stringlike(
        std::env::var("LARD_CONN_STRING").unwrap(),
        NoTls,
    )
    .unwrap();

    let pool = runtime.block_on(async { bb8::Pool::builder().build(open_manager).await.unwrap() });

    let mut group = c.benchmark_group("Windrose");

    group.bench_function("rust", |b| {
        b.to_async(&runtime).iter(|| async {
            let conn = pool.get().await.unwrap();
            rust_bench(&patches, &conn).await;
        })
    });

    group.bench_function("sql", |b| {
        b.to_async(&runtime).iter(|| async {
            let conn = pool.get().await.unwrap();
            sql_bench(&patches, &conn).await;
        })
    });
}

criterion_group!(benches, histogram_benchmark);
criterion_main!(benches);
