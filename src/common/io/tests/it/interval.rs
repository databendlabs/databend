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
            micros: 0,
        }),
        ("-1 year -2 months -3 days", Interval {
            months: -14,
            days: -3,
            micros: 0,
        }),
        ("1 year 2 months 3 days ago", Interval {
            months: -14,
            days: -3,
            micros: 0,
        }),
        ("1day", Interval {
            months: 0,
            days: 1,
            micros: 0,
        }),
        ("1 hour", Interval {
            months: 0,
            days: 0,
            micros: 3600000000,
        }),
        ("1 hours 1 second", Interval {
            months: 0,
            days: 0,
            micros: 3601000000,
        }),
        ("1 day 01:23:45", Interval {
            months: 0,
            days: 1,
            micros: 5_025_000_000,
        }),
        ("2 hours 30 minutes", Interval {
            months: 0,
            days: 0,
            micros: 9_000_000_000,
        }),
        ("-1 day 01:23:45", Interval {
            months: 0,
            days: -1,
            micros: 5025000000,
        }),
        ("-1 day -01:23:45", Interval {
            months: 0,
            days: -1,
            micros: -5025000000,
        }),
        ("1.0000000 hour", Interval::new(0, 0, 3_600_000_000)),
        ("00:00:01.5", Interval::new(0, 0, 1_500_000)),
        ("1 second 500 milliseconds", Interval::new(0, 0, 1_500_000)),
        // Fractional quantities round to a whole unit, halves away from zero (#20988).
        ("0.3 hour", Interval::new(0, 0, 0)),
        ("1.3 hour", Interval::new(0, 0, 3_600_000_000)),
        ("1.49 hour", Interval::new(0, 0, 3_600_000_000)),
        (
            "1.49999999999999999999 hour",
            Interval::new(0, 0, 3_600_000_000),
        ),
        ("1.5 hour", Interval::new(0, 0, 7_200_000_000)),
        ("1.7 hour", Interval::new(0, 0, 7_200_000_000)),
        ("0.5 hour", Interval::new(0, 0, 3_600_000_000)),
        ("2.5 hour", Interval::new(0, 0, 10_800_000_000)),
        ("-0.5 hour", Interval::new(0, 0, -3_600_000_000)),
        ("-1.3 hour", Interval::new(0, 0, -3_600_000_000)),
        ("-2.5 hour", Interval::new(0, 0, -10_800_000_000)),
        ("1.5 hour ago", Interval::new(0, 0, -7_200_000_000)),
        ("1.3 hour 1.3 hour", Interval::new(0, 0, 7_200_000_000)),
        ("1.5 second", Interval::new(0, 0, 2_000_000)),
        ("1.5 minute", Interval::new(0, 0, 120_000_000)),
        ("1.5 day", Interval::new(0, 2, 0)),
        ("1.5 week", Interval::new(0, 14, 0)),
        ("1.5 month", Interval::new(2, 0, 0)),
        ("0.5 quarter", Interval::new(3, 0, 0)),
        ("1.5 year", Interval::new(24, 0, 0)),
        ("1.5 ms", Interval::new(0, 0, 2_000)),
        ("1.5 us", Interval::new(0, 0, 2)),
        ("1.5", Interval::new(0, 0, 2)),
        ("1. hour", Interval::new(0, 0, 3_600_000_000)),
        ("P1Y2M3DT4H5M6S", Interval {
            months: 14,
            days: 3,
            micros: 14_706_000_000,
        }),
        ("p1y2m3dt4h5m6s", Interval {
            months: 14,
            days: 3,
            micros: 14_706_000_000,
        }),
        ("P3W", Interval {
            months: 0,
            days: 21,
            micros: 0,
        }),
        ("PT0.123456S", Interval {
            months: 0,
            days: 0,
            micros: 123_456,
        }),
        ("PT1.5M", Interval {
            months: 0,
            days: 0,
            micros: 90_000_000,
        }),
        ("+P2D", Interval {
            months: 0,
            days: 2,
            micros: 0,
        }),
        ("@ P1Y", Interval {
            months: 12,
            days: 0,
            micros: 0,
        }),
        ("PT0.123456000S", Interval {
            months: 0,
            days: 0,
            micros: 123_456,
        }),
        ("-P1DT2H", Interval {
            months: 0,
            days: -1,
            micros: -7_200_000_000,
        }),
    ];

    for (input, expected) in tests {
        let interval = Interval::from_string(input).unwrap();
        assert_eq!(interval, expected);
    }

    for input in [
        "P",
        "PT",
        "P0.5D",
        "P1.5Y",
        "P1.5M",
        "P1D2Y",
        "PT1H2Y",
        "PT0.123456789S",
        "1.5:30",
        "1.5.5 hour",
        "9223372036854775807.5",
    ] {
        assert!(
            Interval::from_string(input).is_err(),
            "{input} should be rejected"
        );
    }
}

#[test]
fn test_interval_to_string() {
    let tests = vec![
        (
            Interval {
                months: 14,
                days: 3,
                micros: 0,
            },
            "1 year 2 months 3 days",
        ),
        (
            Interval {
                months: -14,
                days: -3,
                micros: 0,
            },
            "-1 year -2 months -3 days",
        ),
        (
            Interval {
                months: 0,
                days: 1,
                micros: 0,
            },
            "1 day",
        ),
        (
            Interval {
                months: 0,
                days: 0,
                micros: 3600000000,
            },
            "1:00:00",
        ),
        (
            Interval {
                months: 0,
                days: 0,
                micros: 3601000000,
            },
            "1:00:01",
        ),
        (
            Interval {
                months: 0,
                days: 1,
                micros: 5025000000,
            },
            "1 day 1:23:45",
        ),
        (
            Interval {
                months: 0,
                days: 0,
                micros: 9000000000,
            },
            "2:30:00",
        ),
        (
            Interval {
                months: 0,
                days: -1,
                micros: 5025000000,
            },
            "-1 day 1:23:45",
        ),
        (
            Interval {
                months: 0,
                days: -225,
                micros: -52550000000,
            },
            "-225 days -14:35:50",
        ),
        (
            Interval {
                months: 0,
                days: -1,
                micros: -5025000000,
            },
            "-1 day -1:23:45",
        ),
    ];

    for (interval, expected) in tests {
        assert_eq!(interval.to_string(), expected);
    }
}
