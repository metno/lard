//! Here we define various functions that can be used with `#[serde(deserialize_with = "")]`
use chrono::NaiveDate;
use std::{fmt, marker::PhantomData, str::FromStr};

use crate::dut_parse::Season;
use serde::{
    Deserialize, Deserializer,
    de::{self, Visitor},
};

// Deserialize a comma separated list of strings to a collection of the requested type
// Adapted from https://github.com/serde-rs/serde/issues/581#issuecomment-253626616
pub fn comma_separated<'de, D, V, T>(des: D) -> Result<V, D::Error>
where
    V: FromIterator<T>,
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    struct CommaSeparated<V, T>(PhantomData<V>, PhantomData<T>);

    impl<V, T> Visitor<'_> for CommaSeparated<V, T>
    where
        V: FromIterator<T>,
        T: FromStr,
        <T as FromStr>::Err: fmt::Display,
    {
        type Value = V;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string containing comma-separated elements")
        }

        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let iter = s.split(",").map(|val| val.parse::<T>());
            Result::from_iter(iter).map_err(de::Error::custom)
        }
    }

    let visitor = CommaSeparated(PhantomData, PhantomData);
    des.deserialize_str(visitor)
}

pub fn optional_comma_separated<'de, D, T>(des: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    let parsed = match Option::<&str>::deserialize(des)? {
        Some(s) => {
            let iter = s.split(",").map(|val| val.parse::<T>());
            let res = Result::from_iter(iter).map_err(de::Error::custom)?;
            Some(res)
        }
        None => None,
    };

    Ok(parsed)
}

pub fn idf_date<'de, D>(des: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    // We need to check for both the format we get and the format we generate
    const ORIGINAL_FORMAT: &str = "%d.%m.%Y"; // DD.MM.YYYY
    const SANE_FORMAT: &str = "%Y-%m-%d";

    let s = String::deserialize(des)?;
    NaiveDate::parse_from_str(&s, ORIGINAL_FORMAT)
        .or_else(|_| NaiveDate::parse_from_str(&s, SANE_FORMAT))
        .map_err(de::Error::custom)
}

pub fn record_date<'de, D>(des: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    // We need to check for both the format we get and the format we generate
    const ORIGINAL_FORMAT: &str = "%d/%m/%Y"; // DD/MM/YYYY
    const SANE_FORMAT: &str = "%Y-%m-%d";

    let s = String::deserialize(des)?;
    NaiveDate::parse_from_str(&s, ORIGINAL_FORMAT)
        .or_else(|_| NaiveDate::parse_from_str(&s, SANE_FORMAT))
        .map_err(de::Error::custom)
}

pub fn dut_season<'de, D>(des: D) -> Result<Season, D::Error>
where
    D: Deserializer<'de>,
{
    let s = i32::deserialize(des)?;
    Ok(match s {
        21 => Season::Spring,
        22 => Season::Summer,
        23 => Season::Autumn,
        24 => Season::Winter,
        _ => Season::Unknown,
    })
}
