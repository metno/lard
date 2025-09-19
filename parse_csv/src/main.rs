use parse_csv::parse_csv_file;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let filename = "parse_csv/files/FINAL_IVF_2025_w_cls_tdato_v01.csv";
    let path = "parse_csv/files/output/".to_string();
    parse_csv_file(filename, &path)
}
