use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// End-exclusive timerange, i.e. from `from`, up to but not including `to`
#[derive(Debug, Default, Clone, Deserialize, Serialize, Copy, PartialEq, Eq, Hash)]
pub struct ClosedTimerange {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl ClosedTimerange {
    pub fn new(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        ClosedTimerange { from, to }
    }

    pub fn overlap(&self, other: Self) -> Option<Self> {
        let from = self.from.max(other.from);
        let to = self.to.min(other.to);

        // If they overlap return the new timerange
        (from < to).then_some(ClosedTimerange { from, to })
    }
}

/// Like `[ClosedTimerange]` except the ends can be None. In the case of `from`,
/// this represents the infinite past, and for `to` the infinite future
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpenTimerange {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

impl OpenTimerange {
    pub fn new(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> Self {
        OpenTimerange { from, to }
    }

    /// Used to cut the priorities to cover ranges that actually matter to a particular timeseries
    /// Takes the from and to times of the timeseries as well as the from and to of the priority range
    /// Returns an option, since it could be they do not overlapp at all (and thus it returns empty)
    pub fn overlap(&self, other: Self) -> Option<Self> {
        let fromtime = match (self.from, other.from) {
            (Some(lhs), Some(rhs)) => Some(lhs.max(rhs)), // return the later one
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        };
        let totime = match (self.to, other.to) {
            (Some(lhs), Some(rhs)) => Some(lhs.min(rhs)), // return the earlier one
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        };

        match (fromtime, totime) {
            // If both ends are closed and the ranges overlap return the new timerange
            (Some(from), Some(to)) => {
                if from >= to {
                    None
                } else {
                    Some(OpenTimerange {
                        from: Some(from),
                        to: Some(to),
                    })
                }
            }
            (from, to) => Some(OpenTimerange { from, to }),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_opentimerange_overlap() {
        let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let t2: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let t3: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let t4: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();

        let cases = vec![
            (
                "ts times inside the priority range",
                // ts:             |-----|
                // priority: |-----------------|
                // output:         |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                },
                OpenTimerange {
                    from: Some(t1),
                    to: Some(t4),
                },
                Some(OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times outside the priority range",
                // ts:       |-----------------|
                // priority:       |-----|
                // output:         |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t1),
                    to: Some(t4),
                },
                OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                },
                Some(OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times overlapp to time of priority range",
                // ts:             |-----------|
                // priority: |-----------|
                // output:         |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t2),
                    to: Some(t4),
                },
                OpenTimerange {
                    from: Some(t1),
                    to: Some(t3),
                },
                Some(OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times overlapp from time of priority range",
                // ts:       |-----------|
                // priority:       |-----------|
                // output:         |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t1),
                    to: Some(t3),
                },
                OpenTimerange {
                    from: Some(t2),
                    to: Some(t4),
                },
                Some(OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "no overlapp ts first",
                // ts:       |-----|
                // priority:             |-----|
                // output:
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t1),
                    to: Some(t2),
                },
                OpenTimerange {
                    from: Some(t3),
                    to: Some(t4),
                },
                None,
            ),
            (
                "no overlapp priority first",
                // ts:                   |-----|
                // priority: |-----|
                // output:
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t3),
                    to: Some(t4),
                },
                OpenTimerange {
                    from: Some(t1),
                    to: Some(t2),
                },
                None,
            ),
            // handle open endedness!!!
            (
                "ts times inside the priority range, open ended priority",
                // ts:             |-----|
                // priority: <----------------->
                // output:         |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                },
                OpenTimerange {
                    from: None,
                    to: None,
                },
                Some(OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times outside the priority range, open ended ts",
                // ts:       <----------------->
                // priority:       |-----|
                // output:         |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: None,
                    to: None,
                },
                OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                },
                Some(OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times overlapp one end of priority range, open ended priority first",
                // ts:             |----------->
                // priority: <-----------|
                // output:         |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t2),
                    to: None,
                },
                OpenTimerange {
                    from: None,
                    to: Some(t3),
                },
                Some(OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "ts times overlapp one end of priority range, open ended ts first",
                // ts:       <-----------|
                // priority:       |----------->
                // output:         |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: None,
                    to: Some(t3),
                },
                OpenTimerange {
                    from: Some(t2),
                    to: None,
                },
                Some(OpenTimerange {
                    from: Some(t2),
                    to: Some(t3),
                }),
            ),
            (
                "no overlapp, open ended ts first",
                // ts:       <-----|
                // priority:             |----->
                // output:
                //           1     2     3     4
                OpenTimerange {
                    from: None,
                    to: Some(t2),
                },
                OpenTimerange {
                    from: Some(t3),
                    to: None,
                },
                None,
            ),
            (
                "no overlapp, open ended priority",
                // ts:                   |----->
                // priority: <-----|
                // output:
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t3),
                    to: None,
                },
                OpenTimerange {
                    from: None,
                    to: Some(t2),
                },
                None,
            ),
            (
                "opposite ends touching, open ended",
                // ts:       <-----|
                // priority:       |----->
                // output:
                //           1     2     3     4
                OpenTimerange {
                    from: None,
                    to: Some(t2),
                },
                OpenTimerange {
                    from: Some(t2),
                    to: None,
                },
                None,
            ),
            (
                "same end touching",
                // ts:       |-----|
                // priority: |-----------|
                // output:   |-----|
                //           1     2     3     4
                OpenTimerange {
                    from: Some(t1),
                    to: Some(t2),
                },
                OpenTimerange {
                    from: Some(t1),
                    to: Some(t3),
                },
                Some(OpenTimerange {
                    from: Some(t1),
                    to: Some(t2),
                }),
            ),
        ];

        for (description, ts_times, priority_times, expected_output) in cases {
            let output = ts_times.overlap(priority_times);
            assert_eq!(output, expected_output, "{description}");
        }
    }
}
