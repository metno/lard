use pg_interval::Interval;
use serde::de;

pub fn de_interval_iso8601<'de, D>(deserializer: D) -> Result<Interval, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    Interval::from_iso(&s).map_err(de::Error::custom)
}
