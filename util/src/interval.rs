use pg_interval::Interval;
use serde::de;

pub fn de_interval_iso8601<'de, D>(deserializer: D) -> Result<Interval, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    Interval::from_iso(&s).map_err(de::Error::custom)
}

pub fn de_optional_interval_iso8601<'de, D>(deserializer: D) -> Result<Option<Interval>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: &str = de::Deserialize::deserialize(deserializer)?;
    match s {
        "" => Ok(None),
        _ => Ok(Some(Interval::from_iso(s).map_err(de::Error::custom)?)),
    }
}
