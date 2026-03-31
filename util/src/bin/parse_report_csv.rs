//! We are given one large csv report file for each of the different types of reports,
//! however we cannot rely on their structure or content being consistent, since there is a human involved.
//! When the we get a new csv report file we may discover that the structure has changed.
//! We may even (in the future) need to adapt the initial parsing code because of this.
//! So until we can integrate the code that generates these report files into our pipeline, we need
//! an initial parsing step to "wash" the files.
//! This initial step of parsing the report csv files is therefore separated from the report endpoint.
//! It separates the large csv file into smaller files per station, and formats them as desired.
//! We then push them to s3, this ensures that in the end we have a standard structure that the
//! report endpoint can rely on.
//! CLI:
//! This provides a CLI to be used for parsing IVF (and potentially other report csv files)
//! in order to separate them and format them as desired for the report endoint. It also
//! pushes them to the s3 bucket used by Lard, so that they are found there by the endpoint
//! handler(s).
//! NOTE: you need to set env variables for S3 access when running this locally
//! (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT_URL, S3_BUCKET_NAME, AWS_REGION).
//! Example:
//! cargo run --bin parse_report_csv "report_files/FINAL_IVF_2025_w_cls_tdato_v01.csv" idf --latest
//! cargo run --bin parse_report_csv "report_files/DUT_alle_kommuner_SOMMER_og_VINTER_v02_23032023_processed.csv" dut
use chrono::prelude::*;
use clap::{Parser, ValueEnum};
use std::env;
use util::dut_parse::{DUT_S3_BASEPATH, DUT_S3_PATH, create_dut_csv_content, parse_dut_csv_file};
use util::idf_parse::{
    Error, IDF_S3_BASEPATH, IDF_S3_PATH, create_idf_csv_content, parse_idf_csv_file,
};

#[derive(Parser)]
struct Cli {
    /// The path to the CSV file to parse
    file_path: String,
    /// The type of the report (IDF or DUT)
    report_type: ReportType,
    /// Whether to push to the latest path in S3
    #[arg(short, long)]
    latest: bool,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ReportType {
    Idf,
    Dut,
}

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

async fn process_content(
    list_of_content: Vec<(String, String)>,
    s3_base_path: &str,
    s3_path: &str,
    latest: bool,
) -> Result<(), Error> {
    for content in list_of_content {
        // add todays date to the name for the path
        let now: DateTime<Local> = Local::now();
        let today_date_string = now.format("%Y-%m-%d").to_string();
        let name = content.0;
        let date_path = format!("{today_date_string}/{name}");
        // push the path and the content
        let path = format!("{s3_base_path}{date_path}");
        push_to_s3(&path, &content.1).await?;
        // also push to /latest if desired
        if latest {
            let latest_path = format!("{s3_path}{name}");
            // push the path and the content
            push_to_s3(&latest_path, &content.1).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    let current_dir = env::current_dir()?;
    println!("Current working directory: {}", current_dir.display());

    let (list_of_content, base_path, path) = match cli.report_type {
        ReportType::Idf => {
            println!("Processing IDF...");
            let hashmap_data = parse_idf_csv_file(&cli.file_path)?;
            (
                create_idf_csv_content(hashmap_data)?,
                IDF_S3_BASEPATH,
                IDF_S3_PATH,
            )
        }
        ReportType::Dut => {
            println!("Processing DUT...");
            let hashmap_data = parse_dut_csv_file(&cli.file_path)?;
            (
                create_dut_csv_content(hashmap_data)?,
                DUT_S3_BASEPATH,
                DUT_S3_PATH,
            )
        }
    };
    println!("Pushing files to s3...");
    process_content(list_of_content, base_path, path, cli.latest).await?;
    println!("Done");
    Ok(())
}
