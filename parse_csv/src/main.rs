use parse_csv::parse_csv_file;
use std::error::Error;
use std::fs;
use tokio::runtime::Runtime;

async fn push_to_s3(list_of_files: Vec<String>, path: String) -> Result<(), Box<dyn Error>> {
    // Set up S3 bucket for IDF
    let bucket = s3::Bucket::new(
        &std::env::var("S3_BUCKET_NAME")?,
        s3::Region::from_env("AWS_REGION", Some("S3_ENDPOINT_URL")).unwrap(),
        // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
        s3::creds::Credentials::from_env().unwrap(),
    )?
    .with_path_style();

    // loop over the files and push them to the s3
    for file in list_of_files {
        // get the file contents
        let filepath = format!("{path}{file}");
        let contents = fs::read_to_string(filepath)?;
        // actually push it to the s3 (async)
        let s3path = format!("/lard_reports/idf/{file}");
        bucket.put_object(s3path, contents.as_bytes()).await?;
    }
    // also push the metadata file
    let filepath = format!("{path}metadata.csv");
    let metadata_contents = fs::read_to_string(filepath)?;
    let s3metadatapath = "/lard_reports/idf/metadata.csv".to_string();
    bucket
        .put_object(s3metadatapath, metadata_contents.as_bytes())
        .await?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let filename = "parse_csv/files/FINAL_IVF_2025_w_cls_tdato_v01.csv";
    let path = "parse_csv/files/output/".to_string();
    let list_of_files = parse_csv_file(filename, &path)?;
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        push_to_s3(list_of_files, path).await.unwrap();
    });

    Ok(())
}
