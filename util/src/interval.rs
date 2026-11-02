use pg_interval::Interval;
use serde::{de, ser};

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
    let opt_s: Option<String> = de::Deserialize::deserialize(deserializer)?;
    match opt_s.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => Ok(Some(Interval::from_iso(s).map_err(de::Error::custom)?)),
    }
}

pub fn ser_optional_interval_iso8601<S>(
    opt_interval: &Option<Interval>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: ser::Serializer,
{
    match opt_interval {
        Some(interval) => serializer.serialize_some(&interval.to_iso_8601()),
        None => serializer.serialize_none(),
    }
}
