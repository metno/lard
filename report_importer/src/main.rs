//! This provides a CLI to be used for parsing IVF (and potentially other report csv files)
//! in order to seperate them and format them as desired for the report endoint. It also
//! pushes them to ths s3 bucket used by Lard, so that they are found there by the endpoint
//! handler(s).
//! Example:
//! cargo run --bin report_importer "report_importer/files/FINAL_IVF_2025_w_cls_tdato_v01.csv"
use report_importer::{create_csv_content, parse_csv_file, Error};
use std::env;

async fn push_to_s3(path: &str, content: &str) -> Result<(), Error> {
    // Set up S3 bucket for IDF
    // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
    // when running locally you need to export / set these vars
    // the variables can be found in the vault encrypted file in ansible/roles/deploy/files/var_file
    let bucket = s3::Bucket::new(
        &std::env::var("S3_BUCKET_NAME")?,
        s3::Region::from_env("AWS_REGION", Some("S3_ENDPOINT_URL")).unwrap(),
        s3::creds::Credentials::from_env().unwrap(),
    )?
    .with_path_style();

    // actually push it to the s3 (async)
    let s3path = format!("/lard_reports/idf/{path}");
    bucket.put_object(s3path, content.as_bytes()).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let filename = if args.len() > 1 {
        println!("Using the filepath: {}", &args[1]);
        &args[1]
    } else {
        return Err(Error::CliError("Issue getting filepath on CLI".to_string()));
    };
    let current_dir = env::current_dir()?;
    println!("Current working directory: {}", current_dir.display());

    let hashmap_data = parse_csv_file(filename)?;
    let list_of_content = create_csv_content(hashmap_data)?;
    println!("Pushing files to s3...");
    for content in list_of_content {
        // the name and the content
        push_to_s3(&content.0, &content.1).await?;
        // TODO: split to push to the base path (latest) as well as a folder with todays date?
    }
    println!("Done");
    Ok(())
}
