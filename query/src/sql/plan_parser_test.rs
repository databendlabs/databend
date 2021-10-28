// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::sql::PlanParser;

#[test]
fn test_plan_parser() -> Result<()> {
    struct Test {
        name: &'static str,
        sql: &'static str,
        expect: &'static str,
        error: &'static str,
    }

    let tests = vec![
        Test {
            name: "create-database-passed",
            sql: "CREATE DATABASE db1",
            expect: "Create database db1, if_not_exists:false, option: {}",
            error: "",
        },
        Test {
            name: "create-database-if-not-exists-passed",
            sql: "CREATE DATABASE IF NOT EXISTS db1",
            expect: "Create database db1, if_not_exists:true, option: {}",
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
            expect: "Create table default.t DataField { name: \"c1\", data_type: Int32, nullable: false }, DataField { name: \"c2\", data_type: Int64, nullable: false }, DataField { name: \"c3\", data_type: String, nullable: false }, engine: Parquet, if_not_exists:false, option: {\"location\": \"foo.parquet\"}",
            error: "",
        },
        Test {
            name: "create-table-if-not-exists-passed",
            sql: "CREATE TABLE IF NOT EXISTS t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Parquet location = 'foo.parquet' ",
            expect: "Create table default.t DataField { name: \"c1\", data_type: Int32, nullable: false }, DataField { name: \"c2\", data_type: Int64, nullable: false }, DataField { name: \"c3\", data_type: String, nullable: false }, engine: Parquet, if_not_exists:true, option: {\"location\": \"foo.parquet\"}",
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
            name: "describe-table-passed",
            sql: "DESCRIBE t1",
            expect: "",
            error: "",
        },
        Test {
            name: "desc-table-passed",
            sql: "DESC db1.t1",
            expect: "",
            error: "",
        },
        Test {
            name: "truncate-table-passed",
            sql: "TRUNCATE TABLE db1.t1",
            expect: "",
            error: "",
        },
        Test {
            name: "cast-passed",
            sql: "select cast('1' as int)",
            expect: "Projection: cast('1' as Int32):Int32\n  Expression: cast(1 as Int32):Int32 (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            error: "",
        },
        Test {
            name: "database-passed",
            sql: "select database()",
            expect: "Projection: database():String\n  Expression: database(default):String (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
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
            sql: "SELECT INTERVAL '1' year, INTERVAL '1' month, INTERVAL '1' day, INTERVAL '1' hour, INTERVAL '1' minute, INTERVAL '1' second",
            expect: "Projection: 12:Interval(YearMonth), 1:Interval(YearMonth), 86400000:Interval(DayTime), 3600000:Interval(DayTime), 60000:Interval(DayTime), 1000:Interval(DayTime)\n  Expression: 12:Interval(YearMonth), 1:Interval(YearMonth), 86400000:Interval(DayTime), 3600000:Interval(DayTime), 60000:Interval(DayTime), 1000:Interval(DayTime) (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            error: "",
        },
        // Test {
        //     name: "interval-unsupported",
        //     sql: "SELECT INTERVAL '1 year 1 day'",
        //     expect: "",
        //     error: "Code: 5, displayText = DF does not support intervals that have both a Year/Month part as well as Days/Hours/Mins/Seconds: \"1 year 1 day\". Hint: try breaking the interval into two parts, one with Year/Month and the other with Days/Hours/Mins/Seconds - e.g. (NOW() + INTERVAL \'1 year\') + INTERVAL \'1 day\'.",
        // },
        // Test {
        //     name: "interval-out-of-range",
        //     sql: "SELECT INTERVAL '100000000000000000 day'",
        //     expect: "",
        //     error: "Code: 5, displayText = Interval field value out of range: \"100000000000000000 day\".",
        // },
        Test {
            name: "insert-simple",
            sql: "insert into t(col1, col2) values(1,2), (3,4)",
            expect: "",
            error: "Code: 25, displayText = Unknown table: 't'.",
        },
        Test {
            name: "insert-value-other-than-simple-expression",
            sql: "insert into t(col1, col2) values(1 + 0, 1 + 1), (3,4)",
            expect: "",
            error: "Code: 25, displayText = Unknown table: 't'.",
        },
        Test {
            name: "insert-subquery-not-supported",
            sql: "insert into t select * from t",
            expect: "",
            error: "Code: 25, displayText = Unknown table: 't'.",
        },
        Test {
            name: "select-full",
            sql: "select sum(number+1)+2, number%3 as id from numbers(10) where number>1 group by id having id>1 order by id desc limit 3",
            expect: "\
            Limit: 3\
            \n  Projection: (sum((number + 1)) + 2):UInt64, (number % 3) as id:UInt8\
            \n    Sort: (number % 3):UInt8\
            \n      Having: ((number % 3) > 1)\
            \n        Expression: (sum((number + 1)) + 2):UInt64, (number % 3):UInt8 (Before OrderBy)\
            \n          AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]\
            \n            AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[sum((number + 1))]]\
            \n              Expression: (number % 3):UInt8, (number + 1):UInt64 (Before GroupBy)\
            \n                Filter: (number > 1)\
            \n                  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            error: "",
        },

        Test {
            name: "unimplemented-cte",
            sql: "with t as ( select sum(number) n from numbers_mt(1000) )select * from t",
            expect: "",
            error: "Code: 2, displayText = CTE is not yet implement.",
        },
        Test {
            name: "kleene-logic-null",
            sql: "select * from numbers(10) where null",
            expect: "\
            Projection: number:UInt64\
            \n  Filter: NULL\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            error: "",
        },
        Test {
            name: "kleene-logic-null-and-true",
            sql: "select * from numbers(10) where null and true",
            expect: "\
            Projection: number:UInt64\
            \n  Filter: (NULL AND true)\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            error: "",
        },
        Test {
            name: "show-metrics",
            sql: "show metrics",
            expect: "\
            Projection: metric:String, kind:String, labels:String, value:String\
            \n  ReadDataSource: scan partitions: [1], scan schema: [metric:String, kind:String, labels:String, value:String], statistics: [read_rows: 0, read_bytes: 0], push_downs: []",
            error: "",
        }
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
