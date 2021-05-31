// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use pretty_assertions::assert_eq;

use crate::sql::PlanParser;

#[test]
fn test_plan_parser() -> anyhow::Result<()> {
    struct Test {
        name: &'static str,
        sql: &'static str,
        expect: &'static str,
        error: &'static str
    }

    let tests = vec![
        Test {
            name: "create-database-passed",
            sql: "CREATE DATABASE db1",
            expect: "Create database db1, engine: Remote, if_not_exists:false, option: {}",
            error: "",
        },
        Test {
            name: "create-database-if-not-exists-passed",
            sql: "CREATE DATABASE IF NOT EXISTS db1",
            expect: "Create database db1, engine: Remote, if_not_exists:true, option: {}",
            error: "",
        },
        Test {
            name: "drop-database-passed",
            sql: "DROP DATABASE db1",
            expect: "Drop database db1, if_exists:false",
            error: "",
        },
        Test {
            name: "drop-database-if-exists-passed",
            sql: "DROP DATABASE IF EXISTS db1",
            expect: "Drop database db1, if_exists:true",
            error: "",
        },
        Test {
            name: "create-table-passed",
            sql: "CREATE TABLE t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Parquet location = 'foo.parquet' ",
            expect: "Create table default.t Field { name: \"c1\", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, Field { name: \"c2\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, Field { name: \"c3\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, engine: Parquet, if_not_exists:false, option: {\"location\": \"foo.parquet\"}",
            error: "",
        },
        Test {
            name: "create-table-if-not-exists-passed",
            sql: "CREATE TABLE IF NOT EXISTS t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Parquet location = 'foo.parquet' ",
            expect: "Create table default.t Field { name: \"c1\", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, Field { name: \"c2\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, Field { name: \"c3\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, engine: Parquet, if_not_exists:true, option: {\"location\": \"foo.parquet\"}",
            error: "",
        },
        Test {
            name: "drop-table-passed",
            sql: "DROP TABLE t1",
            expect: "Drop table default.t1, if_exists:false",
            error: "",
        },
        Test {
            name: "drop-table-passed",
            sql: "DROP TABLE db1.t1",
            expect: "Drop table db1.t1, if_exists:false",
            error: "",
        },
        Test {
            name: "drop-table-if-exists-passed",
            sql: "DROP TABLE IF EXISTS db1.t1",
            expect: "Drop table db1.t1, if_exists:true",
            error: "",
        },
        Test {
        name: "cast-passed",
        sql: "select cast('1' as int)",
        expect: "Projection: cast(1 as Int32):Int32\n  Expression: cast(1 as Int32):Int32 (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
        error: "",
        },
        Test {
        name: "database-passed",
        sql: "select database()",
        expect: "Projection: database(default):Utf8\n  Expression: database(default):Utf8 (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
        error: "",
        },
        Test {
            name: "aggr-fail1",
            sql: "select number + 1, number + 3 from numbers(10) group by number + 2, number + 1",
            expect: "",
            error: "Code: 26, displayText = Column `number` is not under aggregate function and not in GROUP BY: While processing [(number + 1), (number + 3)].",
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
            expect: "Projection: 12:Interval(YearMonth), 1:Interval(YearMonth), 4294967296:Interval(DayTime), 3600000:Interval(DayTime), 60000:Interval(DayTime), 1000:Interval(DayTime)\n  Expression: 12:Interval(YearMonth), 1:Interval(YearMonth), 4294967296:Interval(DayTime), 3600000:Interval(DayTime), 60000:Interval(DayTime), 1000:Interval(DayTime) (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
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

        Test {
            name: "insert-simple",
            sql: "insert into t(col1, col2) values(1,2), (3,4)",
            expect: "",
            error: "",
        },

        Test {
            name: "insert-value-other-than-simple-expression",
            sql: "insert into t(col1, col2) values(1 + 0, 1 + 1), (3,4)",
            expect: "",
            error: "Code: 2, displayText = not support value expressions other than literal value yet."
        },

        Test {
            name: "insert-subquery-not-supported",
            sql: "insert into t select * from t",
            expect: "",
            error: "Code: 2, displayText = only supports simple value tuples as source of insertion."
        },

        Test {
            name: "select-full",
            sql: "select sum(number+1)+2, number%3 as id from numbers(10) where number>1 group by id having id>1 order by id desc limit 3",
            expect: "\
            Limit: 3\
            \n  Projection: (sum((number + 1)) + 2):UInt64, (number % 3) as id:UInt64\
            \n    Sort: (number % 3):UInt64\
            \n      Having: ((number % 3) > 1)\
            \n        Expression: (sum((number + 1)) + 2):UInt64, (number % 3):UInt64 (Before OrderBy)\
            \n          AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]\
            \n            AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]\
            \n              Expression: (number % 3):UInt64, (number + 1):UInt64 (Before GroupBy)\
            \n                Filter: (number > 1)\
            \n                  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80]",
            error: ""
        },
    ];

    let ctx = crate::tests::try_create_context()?;
    for t in tests {
        let plan = PlanParser::create(ctx.clone()).build_from_sql(t.sql);
        match plan {
            Ok(v) => {
                assert_eq!(t.expect, format!("{:?}", v), "{}", t.name);
            }
            Err(e) => {
                assert_eq!(t.error, format!("{}", e), "{}", t.name);
            }
        }
    }

    Ok(())
}
