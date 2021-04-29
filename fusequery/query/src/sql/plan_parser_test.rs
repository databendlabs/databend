// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use pretty_assertions::assert_eq;

use crate::sql::PlanParser;

#[test]
fn test_plan_parser() -> anyhow::Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        sql: &'static str,
        expect: &'static str,
        error: &'static str
    }

    let tests = vec![
        Test {
        name: "cast-passed",
        sql: "select cast('1' as int)",
        expect: "Projection: cast(1 as Int32):Int32\n  ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
        error: "",
    },
        Test {
        name: "database-passed",
        sql: "select database()",
        expect: "Projection: database([default]):Utf8\n  ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
        error: "",
        },
        Test {
            name: "unsupported-function",
            sql: "select unsupported()",
            expect: "",
            error: "Code: 8, displayText = Unsupported function: \"unsupported\".",
        },
        Test {
            name: "interval-passed",
            sql: "SELECT INTERVAL '1 year', INTERVAL '1 month', INTERVAL '1 day', INTERVAL '1 hour', INTERVAL '1 minute', INTERVAL '1 second'",
            expect: "Projection: 12:Interval(YearMonth), 1:Interval(YearMonth), 4294967296:Interval(DayTime), 3600000:Interval(DayTime), 60000:Interval(DayTime), 1000:Interval(DayTime)\n  ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
            error: ""
        },
        Test {
            name: "interval-unsupported",
            sql: "SELECT INTERVAL '1 year 1 day'",
            expect: "",
            error: "Code: 5, displayText = DF does not support intervals that have both a Year/Month part as well as Days/Hours/Mins/Seconds: \"1 year 1 day\". Hint: try breaking the interval into two parts, one with Year/Month and the other with Days/Hours/Mins/Seconds - e.g. (NOW() + INTERVAL \'1 year\') + INTERVAL \'1 day\'."
        },
        Test {
            name: "interval-out-of-range",
            sql: "SELECT INTERVAL '100000000000000000 day'",
            expect: "",
            error: "Code: 5, displayText = Interval field value out of range: \"100000000000000000 day\".",
        },
    ];

    let ctx = crate::tests::try_create_context()?;
    for t in tests {
        let plan = PlanParser::create(ctx.clone()).build_from_sql(t.sql);
        match plan {
            Ok(v) => {
                assert_eq!(t.expect, format!("{:?}", v));
            }
            Err(e) => {
                assert_eq!(t.error, e.to_string());
            }
        }
    }

    Ok(())
}
