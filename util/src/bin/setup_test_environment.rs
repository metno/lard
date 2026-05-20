use std::{
    fs,
    path::Path,
    sync::{Arc, RwLock},
};

use tokio_postgres::{Error, NoTls};

use util::{mock::data::load_mock_data, stinfofacade::persistence};

async fn db_connect(var_name: &str) -> tokio_postgres::Client {
    let (postgres_client, connection) =
        tokio_postgres::connect(&std::env::var(var_name).unwrap(), NoTls)
            .await
            .expect("Should be able to connect to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    postgres_client
}

async fn insert_schema(client: &tokio_postgres::Client, filename: &str) -> Result<(), Error> {
    let schema = fs::read_to_string(filename).expect("Should be able to read SQL file");
    client.batch_execute(schema.as_str()).await
}

fn parse_database_directory() -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = fs::read_dir("db")
        .unwrap()
        .map(|res| res.unwrap())
        // Only applying files whose name starts with 3 digits,
        // so that they can be properly sorted
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .unwrap()
                .bytes()
                .take(3)
                .all(|b| b.is_ascii_digit())
        })
        .map(|entry| entry.path())
        .collect();

    files.sort();

    files
}

async fn find_and_load_mock_data(
    mock_content_name: &str,
    open_client: &tokio_postgres::Client,
    restricted_client: &tokio_postgres::Client,
) {
    let mock_content_base = Path::new("resources/mock_content").join(mock_content_name);

    let mock_content_data_path = mock_content_base.join("data.toml");

    let mock_param_permissions_path = mock_content_base.join("persistence/permissions/param.csv");
    let mock_station_permissions_path =
        mock_content_base.join("persistence/permissions/station.csv");
    let permit_tables = Arc::new(RwLock::new(
        persistence::permissions::load_persisted_from_path(
            mock_param_permissions_path,
            mock_station_permissions_path,
        )
        .await
        .unwrap(),
    ));

    load_mock_data(
        mock_content_data_path,
        open_client,
        restricted_client,
        permit_tables,
    )
    .await
}

#[tokio::main]
async fn main() {
    let mock_content_name = std::env::args().nth(1).unwrap();

    let postgres_client = db_connect("PG_CONN_STRING").await;

    postgres_client
        .execute("CREATE DATABASE lard", &[])
        .await
        .expect("Failed to create lard db");
    postgres_client
        .execute("CREATE DATABASE lard_restricted", &[])
        .await
        .expect("Failed to create lard_restricted db");
    postgres_client
        .execute("CREATE USER lard_readonly WITH PASSWORD 'postgres'", &[])
        .await
        .expect("Failed to create readonly user");
    postgres_client
        .execute("GRANT CONNECT ON DATABASE lard TO lard_readonly", &[])
        .await
        .expect("Failed to grant connect to readonly user");
    postgres_client
        .execute(
            "GRANT CONNECT ON DATABASE lard_restricted TO lard_readonly",
            &[],
        )
        .await
        .expect("Failed to grant connect to readonly user");

    let files = parse_database_directory();

    let open_client = db_connect("LARD_CONN_STRING").await;
    let restricted_client = db_connect("LARD_RESTRICTED_CONN_STRING").await;

    for client in [&open_client, &restricted_client] {
        for file in files.iter() {
            let statements = file.to_str().unwrap();
            insert_schema(client, statements).await.expect(statements);
        }

        for schema in ["public", "legacy", "labels"] {
            client
                .execute(
                    &format!("GRANT USAGE ON SCHEMA {schema} TO lard_readonly"),
                    &[],
                )
                .await
                .expect("Failed to grant schema usage");
            client
                .execute(
                    &format!("GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO lard_readonly"),
                    &[],
                )
                .await
                .expect("Failed to create grant select");
        }
    }

    find_and_load_mock_data(&mock_content_name, &open_client, &restricted_client).await;
}
