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

use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::SqlTestOutcome;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;
use crate::framework::golden::write_case_outcome;

async fn bind_case(case: &SqlTestCase) -> Result<SqlTestOutcome> {
    let ctx = setup_context(case).await?;
    let outcome = match ctx.bind_sql(case.sql).await {
        Ok(plan) => SqlTestOutcome::Plan(plan.format_indent(Default::default())?),
        Err(err) => SqlTestOutcome::Error {
            code: err.code(),
            message: err.message(),
        },
    };
    Ok(outcome)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binder_with_lite_table_context() -> Result<()> {
    let mut file = open_golden_file("semantic", "binder.txt")?;

    let cases = [
        SqlTestCase {
            name: "simple_aggregate_query_binds",
            description: "A plain aggregate query should bind successfully.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT avg(number) FROM t",
        },
        SqlTestCase {
            name: "where_rejects_aggregate_alias",
            description: "An aggregate alias must still be rejected in WHERE.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s FROM t WHERE s > 0",
        },
        SqlTestCase {
            name: "where_accepts_scalar_alias",
            description: "A scalar alias should remain usable in WHERE.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number + 1 AS s FROM t WHERE s > 1",
        },
        SqlTestCase {
            name: "qualify_rejects_aggregate_alias",
            description: "An aggregate alias must still be rejected in QUALIFY.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s FROM t QUALIFY s > 0",
        },
        SqlTestCase {
            name: "qualify_accepts_window_alias",
            description: "A window alias should remain usable in QUALIFY.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number, row_number() OVER (ORDER BY number) AS rn FROM t QUALIFY rn = 1",
        },
        SqlTestCase {
            name: "having_accepts_aggregate_alias",
            description: "An aggregate alias should remain usable in HAVING.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s FROM t HAVING s > 0",
        },
        SqlTestCase {
            name: "aggregate_argument_prefers_base_column_over_select_alias",
            description:
                "Inside an aggregate function, a same-name select alias should not shadow the base column.",
            setup_sqls: &["CREATE TABLE t(a UInt64, c2 UInt64)"],
            sql: "SELECT a AS c2, sum(c2) FROM t GROUP BY a",
        },
        SqlTestCase {
            name: "window_aggregate_does_not_become_group_aggregate",
            description:
                "An aggregate used as a window function should stay in the window phase rather than becoming a group aggregate.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) OVER () FROM t",
        },
        SqlTestCase {
            name: "window_partition_rejects_new_aggregate",
            description:
                "A window PARTITION BY clause must not introduce a new aggregate expression.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (PARTITION BY sum(number)) FROM t",
        },
        SqlTestCase {
            name: "window_order_rejects_new_aggregate",
            description:
                "A window ORDER BY clause must not introduce a new aggregate expression.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY sum(number)) FROM t",
        },
        SqlTestCase {
            name: "unnest_over_aggregate_is_planned_after_aggregate",
            description:
                "A set-returning function over an aggregate should stay above the aggregate phase instead of rewriting the aggregate away early.",
            setup_sqls: &[],
            sql: "SELECT unnest(max([11, 12]))",
        },
    ];

    for case in &cases {
        write_case_header(&mut file, case)?;
        let outcome = bind_case(case).await?;
        write_case_outcome(&mut file, &outcome)?;
    }

    Ok(())
}
