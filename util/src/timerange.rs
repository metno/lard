use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// End-exclusive timerange, i.e. from `from`, up to but not including `to`
#[derive(Debug, Default, Clone, Deserialize, Serialize, Copy, PartialEq, Eq, Hash)]
pub struct Closed {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl Closed {
    pub fn overlap(&self, other: Self) -> Option<Self> {
        let from = self.from.max(other.from);
        let to = self.to.min(other.to);

        // If they overlap return the new timerange
        (from < to).then_some(Closed { from, to })
    }

    pub fn overlap_half_open(&self, other: HalfOpen) -> Option<Self> {
        let from = self.from.max(other.from);
        let to = if let Some(otherto) = other.to {
            self.to.min(otherto)
        } else {
            self.to
        };

        // If they overlap return the new timerange
        (from < to).then_some(Closed { from, to })
    }

    pub fn to_half_open(self) -> HalfOpen {
        HalfOpen {
            from: self.from,
            to: Some(self.to),
        }
    }
}

pub fn closed(from: DateTime<Utc>, to: DateTime<Utc>) -> Closed {
    Closed { from, to }
}

/// Like `[Closed]` except the ends can be None. In the case of `from`,
/// this represents the infinite past, and for `to` the infinite future
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Open {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

impl Open {
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
                    Some(Self {
                        from: Some(from),
                        to: Some(to),
                    })
                }
            }
            (from, to) => Some(Open { from, to }),
        }
    }
}

pub fn open(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> Open {
    Open { from, to }
}

#[derive(Debug, Clone, Copy)]
pub struct HalfOpen {
    pub from: DateTime<Utc>,
    pub to: Option<DateTime<Utc>>,
}

pub fn half_open(from: DateTime<Utc>, to: Option<DateTime<Utc>>) -> HalfOpen {
    HalfOpen { from, to }
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
                Open {
                    from: Some(t2),
                    to: Some(t3),
                },
                Open {
                    from: Some(t1),
                    to: Some(t4),
                },
                Some(Open {
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
                Open {
                    from: Some(t1),
                    to: Some(t4),
                },
                Open {
                    from: Some(t2),
                    to: Some(t3),
                },
                Some(Open {
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
                Open {
                    from: Some(t2),
                    to: Some(t4),
                },
                Open {
                    from: Some(t1),
                    to: Some(t3),
                },
                Some(Open {
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
                Open {
                    from: Some(t1),
                    to: Some(t3),
                },
                Open {
                    from: Some(t2),
                    to: Some(t4),
                },
                Some(Open {
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
                Open {
                    from: Some(t1),
                    to: Some(t2),
                },
                Open {
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
                Open {
                    from: Some(t3),
                    to: Some(t4),
                },
                Open {
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
                Open {
                    from: Some(t2),
                    to: Some(t3),
                },
                Open {
                    from: None,
                    to: None,
                },
                Some(Open {
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
                Open {
                    from: None,
                    to: None,
                },
                Open {
                    from: Some(t2),
                    to: Some(t3),
                },
                Some(Open {
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
                Open {
                    from: Some(t2),
                    to: None,
                },
                Open {
                    from: None,
                    to: Some(t3),
                },
                Some(Open {
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
                Open {
                    from: None,
                    to: Some(t3),
                },
                Open {
                    from: Some(t2),
                    to: None,
                },
                Some(Open {
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
                Open {
                    from: None,
                    to: Some(t2),
                },
                Open {
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
                Open {
                    from: Some(t3),
                    to: None,
                },
                Open {
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
                Open {
                    from: None,
                    to: Some(t2),
                },
                Open {
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
                Open {
                    from: Some(t1),
                    to: Some(t2),
                },
                Open {
                    from: Some(t1),
                    to: Some(t3),
                },
                Some(Open {
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
