use regex::Regex;
use std::str::Lines;

// TODO: this should go somewhere more shared
// TODO: verify integer types
pub struct Obs {
    obstime: i32,
    id: i32,
}

struct KlMeta {
    station_id: i32,
    type_id: i32,
}

struct Column {
    param_code: String,
    sensor_and_level: Option<(i32, i32)>,
}

fn parse_meta_field(
    field: &str,
    expected_key: &'static str,
) -> Result<i32, Box<dyn std::error::Error>> {
    let (key, value) = field.split_once('=').unwrap();

    assert_eq!(key, expected_key);

    Ok(value.parse().unwrap())
}

fn parse_meta(meta: &str) -> Result<KlMeta, &dyn std::error::Error> {
    let mut parts = meta.splitn(4, '/');

    assert_eq!(parts.next().unwrap(), "kldata");

    let station_id = parse_meta_field(parts.next().unwrap(), "station_id").unwrap();
    let type_id = parse_meta_field(parts.next().unwrap(), "type_id").unwrap();

    Ok(KlMeta {
        station_id,
        type_id,
    })
}

fn parse_columns(cols_raw: &str) -> Result<Vec<Column>, &dyn std::error::Error> {
    // this regex is taken from kvkafka's kldata parser
    // let col_regex = Regex::new(r"([^(),]+)(\([0-9]+,[0-9]+\))?").unwrap();
    // it is modified below to capture sensor and level separately, while keeping
    // the block collectively optional
    let col_regex = Regex::new(r"([^(),]+)(?\(([0-9]+),([0-9]+)\))?").unwrap();

    Ok(col_regex
        .captures_iter(cols_raw)
        .map(|caps| match caps.len() {
            2 => Column {
                param_code: caps.get(1).unwrap().as_str().to_owned(),
                sensor_and_level: None,
            },
            4 => Column {
                param_code: caps.get(1).unwrap().as_str().to_owned(),
                sensor_and_level: Some((
                    caps.get(2).unwrap().as_str().parse().unwrap(),
                    caps.get(3).unwrap().as_str().parse().unwrap(),
                )),
            },
            _ => panic!(),
        })
        .collect())
}

fn parse_obs(
    csv_body: Lines<'_>,
    columns: &[Column],
) -> Result<Vec<Obs>, Box<dyn std::error::Error>> {
    todo!()
}

pub fn parse_kldata(msg: &str) -> Result<Vec<Obs>, &dyn std::error::Error> {
    // parse the first two lines of the message as meta, and csv column names,
    // leave the rest as an iter over the lines of csv body
    let (meta, columns, csv_body) = {
        let mut lines = msg.lines();

        let meta = parse_meta(lines.next().unwrap()).unwrap();
        let columns = parse_columns(lines.next().unwrap()).unwrap();

        (meta, columns, lines)
    };

    Ok(parse_obs(csv_body, &columns).unwrap())
}
