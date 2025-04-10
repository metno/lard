use serde::{de, Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
/// Represents <KvalobsData>...</KvalobsData>
pub struct KvalobsData {
    #[serde(rename = "station")]
    pub stations: Vec<Station>,
}
#[derive(Debug, Deserialize)]
/// Represents <station>...</station>
pub struct Station {
    #[serde(rename = "@val")]
    pub val: i32,
    #[serde(rename = "typeid")]
    pub typeids: Vec<Typeid>,
}
#[derive(Debug, Deserialize)]
/// Represents <typeid>...</typeid>
pub struct Typeid {
    #[serde(rename = "@val")]
    pub val: i32,
    #[serde(rename = "obstime")]
    pub obstimes: Vec<Obstime>,
}
#[derive(Debug, Deserialize)]
/// Represents <obstime>...</obstime>
pub struct Obstime {
    #[serde(rename = "@val")]
    pub val: String, // avoiding parsing time at this point...
    #[serde(rename = "tbtime")]
    pub tbtimes: Vec<Tbtime>,
}
#[derive(Debug, Deserialize)]
/// Represents <tbtime>...</tbtime>
pub struct Tbtime {
    #[serde(rename = "@val")]
    _val: String, // avoiding parsing time at this point...
    _kvtextdata: Option<Vec<Kvtextdata>>,
    #[serde(rename = "sensor")]
    pub sensors: Vec<Sensor>,
}
/// Represents <kvtextdata>...</kvtextdata>
#[derive(Debug, Deserialize)]
struct Kvtextdata {
    _paramid: Option<i32>,
    _original: Option<String>,
}
#[derive(Debug, Deserialize)]
/// Represents <sensor>...</sensor>
pub struct Sensor {
    #[serde(rename = "@val", deserialize_with = "optional")]
    pub val: Option<i32>,
    #[serde(rename = "level")]
    pub levels: Vec<Level>,
}
/// Represents <level>...</level>
#[derive(Debug, Deserialize)]
pub struct Level {
    #[serde(rename = "@val", deserialize_with = "optional")]
    pub val: Option<i32>,
    pub kvdata: Option<Vec<Kvdata>>,
}

/// Represents <kvdata>...</kvdata>
#[derive(Debug, Clone, Deserialize)]
pub struct Kvdata {
    #[serde(rename = "@paramid")]
    pub paramid: i32,
    #[serde(default, deserialize_with = "optional")]
    pub corrected: Option<f64>,
    #[serde(default, deserialize_with = "optional")]
    pub controlinfo: Option<String>,
    #[serde(default, deserialize_with = "optional")]
    pub useinfo: Option<String>,
    #[serde(default, deserialize_with = "optional")]
    pub cfailed: Option<String>,
}

// The #[serde(default)] macro deserializes an Option field to None if it's missing.
// This function deserializes an empty field (empty string "") to None.
fn optional<'de, D, T>(des: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    let parsed = match Option::deserialize(des)? {
        Some("") | None => None,
        Some(val) => Some(val.parse().map_err(de::Error::custom)?),
    };

    Ok(parsed)
}
