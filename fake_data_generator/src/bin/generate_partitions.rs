use std::{fs::File, io::Write};

use chrono::{DateTime, TimeZone, Utc};
use std::io::BufWriter;

fn create_table_partitions(
    table: &str,
    boundaries: &[DateTime<Utc>],
    writer: &mut BufWriter<File>,
) -> Result<(), std::io::Error> {
    // .windows(2) gives a 2-wide sliding view of the vector, so we can see
    // both bounds relevant to a partition
    for window in boundaries.windows(2) {
        let start_time = window[0];
        let end_time = window[1];

        let line = format!(
            "CREATE TABLE IF NOT EXISTS {}_y{}_to_y{} PARTITION OF public.{}\nFOR VALUES FROM ('{}') TO ('{}');\n",
            table,
            start_time.format("%Y"),
            end_time.format("%Y"),
            table,
            start_time.format("%Y-%m-%d %H:%M:%S+00"),
            end_time.format("%Y-%m-%d %H:%M:%S+00")
        );
        writer.write_all(line.as_bytes())?;
    }

    Ok(())
}

fn main() -> Result<(), std::io::Error> {
    let outfile = File::create("../db/partitions_generated.sql")?;
    let mut writer = BufWriter::new(outfile);

    // create a vector of the boundaries between partitions
    let paritition_boundary_years: Vec<DateTime<Utc>> = [1950, 2000, 2010]
        .into_iter()
        .chain(2015..=2030)
        .map(|y| Utc.with_ymd_and_hms(y, 1, 1, 0, 0, 0).unwrap())
        .collect();

    create_table_partitions("data", &paritition_boundary_years, &mut writer)?;
    create_table_partitions("nonscalar_data", &paritition_boundary_years, &mut writer)?;

    Ok(())
}
