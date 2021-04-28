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
        actual: &'static str,
        error: &'static str
    }

    let tests = vec![
        Test {
        name: "cast-passed",
        sql: "select cast('1' as int)",
        actual: "Projection: cast(1 as Int32):Int32\n  ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
        error: "",
    },
        Test {
        name: "database-passed",
        sql: "select database()",
        actual: "Projection: database([default]):Utf8\n  ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
        error: "",
        },
        Test {
            name: "unsupported-function",
            sql: "select unsupported()",
            actual: "",
            error: "Unsupported function: \"unsupported\"",
        },
    ];

    let ctx = crate::tests::try_create_context()?;
    for t in tests {
        let plan = PlanParser::create(ctx.clone()).build_from_sql(t.sql);
        match plan {
            Ok(v) => {
                assert_eq!(t.actual, format!("{:?}", v));
            }
            Err(e) => {
                assert_eq!(t.error, e.to_string());
            }
        }
    }

    Ok(())
}
