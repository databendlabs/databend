// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_io::Interval;

#[test]
fn test_interval_from_string() {
    let tests = vec![
        ("1 year 2 months 3 days", Interval {
            months: 14,
            days: 3,
            nanos: 0,
        }),
        ("-1 year -2 months -3 days", Interval {
            months: -14,
            days: -3,
            nanos: 0,
        }),
        ("1 year 2 months 3 days ago", Interval {
            months: -14,
            days: -3,
            nanos: 0,
        }),
        ("1day", Interval {
            months: 0,
            days: 1,
            nanos: 0,
        }),
        ("1 hour", Interval {
            months: 0,
            days: 0,
            nanos: 3600000000000,
        }),
        ("1 hours 1 second", Interval {
            months: 0,
            days: 0,
            nanos: 3601000000000,
        }),
        ("1 day 01:23:45", Interval {
            months: 0,
            days: 1,
            nanos: 5_025_000_000_000,
        }),
        ("2 hours 30 minutes", Interval {
            months: 0,
            days: 0,
            nanos: 9_000_000_000_000,
        }),
        ("-1 day 01:23:45", Interval {
            months: 0,
            days: -1,
            nanos: 5025000000000,
        }),
        ("-1 day -01:23:45", Interval {
            months: 0,
            days: -1,
            nanos: -5025000000000,
        }),
    ];

    for (input, expected) in tests {
        let interval = Interval::from_string(input).unwrap();
        assert_eq!(interval, expected);
    }
}

#[test]
fn test_string_to_interval() {
    let tests = vec![
        (
            Interval {
                months: 14,
                days: 3,
                nanos: 0,
            },
            "1 year 2 months 3 days",
        ),
        (
            Interval {
                months: -14,
                days: -3,
                nanos: 0,
            },
            "-1 year -2 months -3 days",
        ),
        (
            Interval {
                months: 0,
                days: 1,
                nanos: 0,
            },
            "1 day",
        ),
        (
            Interval {
                months: 0,
                days: 0,
                nanos: 3600000000000,
            },
            "1:00:00",
        ),
        (
            Interval {
                months: 0,
                days: 0,
                nanos: 3601000000000,
            },
            "1:00:01",
        ),
        (
            Interval {
                months: 0,
                days: 1,
                nanos: 5025000000000,
            },
            "1 day 1:23:45",
        ),
        (
            Interval {
                months: 0,
                days: 0,
                nanos: 9000000000000,
            },
            "2:30:00",
        ),
        (
            Interval {
                months: 0,
                days: -1,
                nanos: 5025000000000,
            },
            "-1 day 1:23:45",
        ),
        (
            Interval {
                months: 0,
                days: -1,
                nanos: -5025000000000,
            },
            "-1 day -1:23:45",
        ),
    ];

    for (interval, expected) in tests {
        assert_eq!(interval.to_string(), expected);
    }
}
