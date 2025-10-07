//! This provides a CLI to be used for parsing IVF (and potentially other report csv files)
//! in order to seperate them and format them as desired for the report endoint. It also
//! pushes them to ths s3 bucket used by Lard, so that they are found there by the endpoint
//! handler(s).
//! Example:
//! cargo run --bin parse_report_csv "report_files/FINAL_IVF_2025_w_cls_tdato_v01.csv" "true"
use chrono::prelude::*;
use std::env;
use util::{create_csv_content, parse_csv_file, Error, IDF_S3_PATH};

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
    let s3path = format!("{IDF_S3_PATH}{path}");
    bucket.put_object(s3path, content.as_bytes()).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let filename = if args.len() > 1 {
        println!("Using the filepath, for parsing the file: {}", &args[1]);
        &args[1]
    } else {
        return Err(Error::CliError("Issue getting filepath on CLI".to_string()));
    };
    let latest: Option<&String> = if args.len() > 2 {
        println!("Should push to latest? {}", &args[2]);
        Some(&args[2])
    } else {
        None
    };

    let current_dir = env::current_dir()?;
    println!("Current working directory: {}", current_dir.display());

    let hashmap_data = parse_csv_file(filename)?;
    let list_of_content = create_csv_content(hashmap_data)?;
    println!("Pushing files to s3...");
    for content in list_of_content {
        // add todays date to the name for the path
        let now: DateTime<Local> = Local::now();
        let today_date_string = now.format("%Y-%m-%d").to_string();
        let name = content.0;
        let path = format!("{today_date_string}/{name}");
        // push the path and the content
        push_to_s3(&path, &content.1).await?;
        // also push to /latest if desired
        if latest.is_some() && latest.unwrap() == "true" {
            let latest_path = format!("latest/{name}");
            // push the path and the content
            push_to_s3(&latest_path, &content.1).await?;
        }
    }
    println!("Done");
    Ok(())
}
