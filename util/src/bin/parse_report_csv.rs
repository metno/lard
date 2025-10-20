//! This provides a CLI to be used for parsing IVF (and potentially other report csv files)
//! in order to seperate them and format them as desired for the report endoint. It also
//! pushes them to ths s3 bucket used by Lard, so that they are found there by the endpoint
//! handler(s).
//! Example:
//! cargo run --bin parse_report_csv "report_files/FINAL_IVF_2025_w_cls_tdato_v01.csv" idf true
//! cargo run --bin parse_report_csv "report_files/DUT_alle_kommuner_SOMMER_og_VINTER_v02_23032023.csv" dut false
use chrono::prelude::*;
use std::env;
use util::dut_parse::{create_dut_csv_content, parse_dut_csv_file, DUT_S3_BASEPATH, DUT_S3_PATH};
use util::idf_parse::{
    create_idf_csv_content, parse_idf_csv_file, Error, IDF_S3_BASEPATH, IDF_S3_PATH,
};

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
    bucket.put_object(path, content.as_bytes()).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let filename = if args.len() > 1 {
        println!("Using the filepath, for parsing the file: {}", &args[1]);
        &args[1]
    } else {
        return Err(Error::CliError(
            "Issue getting filepath and type on CLI".to_string(),
        ));
    };
    let filetype = if args.len() > 2 {
        println!("Using the file type: {}", &args[2]);
        &args[2]
    } else {
        return Err(Error::CliError(
            "Issue getting filepath and type on CLI".to_string(),
        ));
    };
    let latest: Option<&String> = if args.len() > 3 {
        println!("Should push to latest? {}", &args[3]);
        Some(&args[3])
    } else {
        None
    };

    let current_dir = env::current_dir()?;
    println!("Current working directory: {}", current_dir.display());

    if filetype == "IDF" || filetype == "idf" {
        println!("Processing IDF...");
        let hashmap_data = parse_idf_csv_file(filename)?;
        let list_of_content = create_idf_csv_content(hashmap_data)?;
        println!("Pushing files to s3...");
        for content in list_of_content {
            // add todays date to the name for the path
            let now: DateTime<Local> = Local::now();
            let today_date_string = now.format("%Y-%m-%d").to_string();
            let name = content.0;
            let date_path = format!("{today_date_string}/{name}");
            // push the path and the content
            let path = format!("{IDF_S3_BASEPATH}{date_path}");
            push_to_s3(&path, &content.1).await?;
            // also push to /latest if desired
            if latest.is_some() && latest.unwrap() == "true" {
                let latest_path = format!("{IDF_S3_PATH}{name}");
                // push the path and the content
                push_to_s3(&latest_path, &content.1).await?;
            }
        }
    } else if filetype == "DUT" || filetype == "dut" {
        println!("Processing DUT...");
        let hashmap_data = parse_dut_csv_file(filename)?;
        let list_of_content = create_dut_csv_content(hashmap_data)?;
        println!("Pushing files to s3...");
        for content in list_of_content {
            // add todays date to the name for the path
            let now: DateTime<Local> = Local::now();
            let today_date_string = now.format("%Y-%m-%d").to_string();
            let name = content.0;
            let date_path = format!("{today_date_string}/{name}");
            // push the path and the content
            let path = format!("{DUT_S3_BASEPATH}{date_path}");
            push_to_s3(&path, &content.1).await?;
            // also push to /latest if desired
            if latest.is_some() && latest.unwrap() == "true" {
                let latest_path = format!("{DUT_S3_PATH}{name}");
                // push the path and the content
                push_to_s3(&latest_path, &content.1).await?;
            }
        }
    }

    println!("Done");
    Ok(())
}
