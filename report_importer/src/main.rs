use report_importer::parse_csv_file;
use std::error::Error;
use std::fs;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CSVError {
    #[error("CLI error: {0}")]
    CLIerror(String),
    #[error("CSV parsing error: {0}")]
    CSVerror(#[from] Box<dyn std::error::Error>),
}

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

#[tokio::main]
async fn main() -> Result<(), CSVError> {
    let args: Vec<String> = std::env::args().collect();
    let filename = if args.len() > 1 {
        println!("Using the filepath: {}", &args[1]);
        &args[1]
    } else {
        return Err(CSVError::CLIerror(
            "Issue getting filepath on CLI".to_string(),
        ));
    };
    let output_path = "parse_csv/files/output/".to_string();
    let list_of_files = parse_csv_file(filename, &output_path)?;

    push_to_s3(list_of_files, output_path).await?;

    Ok(())
}
