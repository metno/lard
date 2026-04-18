use chrono::{DateTime, Utc};
use jsonwebtoken::DecodingKey;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use chrono::{Duration, TimeZone};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

use util::{
    MetTimeseriesKey, OpenTimerange,
    auth::{Access, Claims, Resource, Roles},
    stinfofacade::{
        level::{self, Level, LevelTable},
        message_priority::{DefaultTable, MessagePriority},
        permissions::{ParamPermit, ParamPermitTable, StationPermitTable},
    },
};

pub struct MetadataMock {
    pub station: i32,
    pub fromtime: DateTime<Utc>,
    pub totime: DateTime<Utc>,
}

impl MetadataMock {
    pub async fn cache_closed_stinfosys(
        &self,
    ) -> Result<
        (
            HashMap<MetTimeseriesKey, OpenTimerange>,
            HashMap<i32, OpenTimerange>,
        ),
        lard_ingestion::Error,
    > {
        let mut station_times = HashMap::new();
        station_times.insert(
            self.station,
            OpenTimerange {
                from: Some(self.fromtime),
                to: Some(self.totime),
            },
        );

        let obs_pgm_times: HashMap<MetTimeseriesKey, OpenTimerange> = HashMap::new();

        Ok((obs_pgm_times, station_times))
    }
}

pub fn mock_permit_tables() -> Arc<RwLock<(ParamPermitTable, StationPermitTable)>> {
    let param_permit = HashMap::from([
        // station_id -> (type_id, param_id, permit_id)
        (10000, vec![ParamPermit::new(0, 0, 0)]),
        (10001, vec![ParamPermit::new(0, 0, 1)]), // open
    ]);

    let station_permit = HashMap::from([
        // station_id -> permit_id
        (10000, 1), // overridden by param_permit
        (10001, 0), // overridden by param_permit
        (20000, 0),
        (20001, 1), // open
        (20002, 1), // open
        (99995, 5), // restricted
        (1234, 2),  // restricted
    ]);

    Arc::new(RwLock::new((param_permit, station_permit)))
}

pub fn mock_level_table() -> LevelTable {
    let param_level = HashMap::from([
        (211, Level::new(2, level::Unit::M, level::Direction::Up)),
        (81, Level::new(10, level::Unit::M, level::Direction::Up)),
        (3, Level::new(20, level::Unit::Cm, level::Direction::Down)),
        // Needed for IDF event
        (105, Level::new(2, level::Unit::M, level::Direction::Up)),
        // Needed for windrose
        (61, Level::new(10, level::Unit::M, level::Direction::Up)),
        (81, Level::new(10, level::Unit::M, level::Direction::Up)),
    ]);

    Arc::new(RwLock::new(param_level))
}

pub fn mock_auth_certs() -> DecodingKey {
    jsonwebtoken::DecodingKey::from_ec_pem(
        b"-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAETz7rFlJZ8IM7r53QKr7hF6GitWKpY3FN
tqdj2gL4EFqYX459/hpSh7w5hIW8k8mmftDz0Pm12CmV9MyvD1Lv1pucYyoJLobR
wARDennWSrMRamnmbyLO6jno3N9mNFtq
-----END PUBLIC KEY-----",
    )
    .unwrap()
}

pub fn create_mock_jwt(roles: Roles) -> Option<String> {
    let now = Utc::now();
    let expiration_time = now + Duration::weeks(520); // Token valid for 10 years

    let claims = Claims {
        resource_access: Access {
            resource: Resource { roles },
        },
        exp: expiration_time.timestamp() as usize,
    };

    // Create header
    let header = Header::new(Algorithm::ES384);

    // Create encoding key from test private key (this should corresponds to the public key in mock_auth_certs())
    // NOTE: this is just used for testing
    let encoding_key = EncodingKey::from_ec_pem(
        b"-----BEGIN PRIVATE KEY-----
MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDDhihKsqOZ3ph6JqXnA
qDsU368kko3rmLDerN8zn3HkERY4cSETRYqXnCSrSEVVwpehZANiAARPPusWUlnw
gzuvndAqvuEXoaK1YqljcU22p2PaAvgQWphfjn3+GlKHvDmEhbyTyaZ+0PPQ+bXY
KZX0zK8PUu/Wm5xjKgkuhtHABEN6edZKsxFqaeZvIs7qOejc32Y0W2o=
-----END PRIVATE KEY-----",
    );
    match encoding_key {
        Ok(key) => encode(&header, &claims, &key).ok(),
        // This is just for testing so we return errors as None
        Err(_) => None,
    }
}

pub fn mock_message_priority() -> DefaultTable {
    let from: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 12, 31, 23, 0, 0).unwrap();
    let to: DateTime<Utc> = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();

    DefaultTable::from([
        (
            (508, 211),
            MessagePriority::new(9000, OpenTimerange::new(Some(from), Some(to))),
        ),
        (
            (501, 211),
            MessagePriority::new(9000, OpenTimerange::new(Some(to), None)),
        ),
        (
            (501, 225),
            MessagePriority::new(9000, OpenTimerange::new(Some(from), None)),
        ),
        // The next ones are needed for IDF event
        (
            (514, 105),
            MessagePriority::new(100, OpenTimerange::new(Some(from), Some(to))),
        ),
        (
            // This is needed to check that our patches are sorted
            (501, 105),
            MessagePriority::new(
                200,
                OpenTimerange::new(Some(from - Duration::hours(1)), Some(from)),
            ),
        ),
        (
            (508, 105),
            MessagePriority::new(300, OpenTimerange::new(Some(to), None)),
        ),
        // Needed for windrose
        (
            (501, 61),
            MessagePriority::new(200, OpenTimerange::new(Some(to), None)),
        ),
        (
            (501, 81),
            MessagePriority::new(200, OpenTimerange::new(Some(to), None)),
        ),
    ])
}

#[cfg(test)]
mod test {
    use util::stinfofacade::{level::param_get_level, permissions::timeseries_get_permit};

    use super::*;

    #[test]
    fn test_param_get_level() {
        let cases = vec![
            (211, 0, 200, "air_temperature default is 2m"),
            (211, 10, 1000, "air_temperature at 10m converted to cm"),
            (81, 0, 1000, "wind_speed default is 10m"),
            (3, 0, -20, "3 default is 20cm"),
        ];

        let level_table = mock_level_table();
        for case in cases {
            let param_id = case.0;
            let level = case.1;
            let expected = case.2;
            let test_case = case.3;

            let output = param_get_level(level_table.clone(), param_id, level).unwrap();
            assert_eq!(output, Some(expected), "{test_case}");
        }
    }

    #[test]
    fn test_timeseries_get_permit() {
        let cases = vec![
            (0, 0, 0, None, "stationid not in permit_tables"),
            (
                10000,
                0,
                0,
                // FIXME: Is permit 0 really what we want?
                Some(0),
                "stationid in ParamPermitTable, timeseries closed",
            ),
            (
                10001,
                0,
                0,
                Some(1),
                "stationid in ParamPermitTable, timeseries open",
            ),
            (
                20000,
                0,
                0,
                Some(0),
                "stationid in StationPermitTable, timeseries closed",
            ),
            (
                20001,
                0,
                1,
                Some(1),
                "stationid in StationPermitTable, timeseries open",
            ),
        ];

        let permit_tables = mock_permit_tables();
        for case in cases {
            let station_id = case.0;
            let type_id = case.1;
            // FIXME: shouldn't this be param_id?
            let permit_id = case.2;
            let expected = case.3;
            let test_case = case.4;

            let output =
                timeseries_get_permit(permit_tables.clone(), station_id, type_id, Some(permit_id))
                    .unwrap();
            assert_eq!(output, expected, "{test_case}");
        }
    }
}
