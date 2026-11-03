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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_sql::plans::Plan;

use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::setup_context;

/// Plans `case` with the binder and the production optimizer and returns whether the plan is
/// marked as holding a value that is valid only for this query. The planner cache does not
/// store such plans.
async fn holds_query_specific_value(case: &SqlTestCase) -> Result<bool> {
    let ctx = setup_context(case).await?;
    plan_holds_query_specific_value(&ctx, case).await
}

async fn plan_holds_query_specific_value(
    ctx: &Arc<LiteTableContext>,
    case: &SqlTestCase,
) -> Result<bool> {
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan).await?;
    let Plan::Query { metadata, .. } = &optimized_plan else {
        unreachable!("test query should bind to Plan::Query")
    };
    let holds_query_specific_value = metadata.read().holds_query_specific_value();
    Ok(holds_query_specific_value)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_plans_holding_query_specific_values() -> Result<()> {
    let cases = [
        (true, SqlTestCase {
            name: "now_predicate_turns_outer_join_into_inner_join",
            description: "Only the value of `r.ts > now()` for NULL input makes the LEFT JOIN an inner join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM plan_cache_left AS l
LEFT JOIN plan_cache_right AS r ON l.k = r.k
WHERE r.ts > now()",
        }),
        (false, SqlTestCase {
            name: "deterministic_predicate_also_turns_outer_join_into_inner_join",
            description: "A deterministic null-rejecting predicate gives the same join type without the now() predicate.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM plan_cache_left AS l
LEFT JOIN plan_cache_right AS r ON l.k = r.k
WHERE r.k > 1 AND r.ts > now()",
        }),
        (false, SqlTestCase {
            name: "preserved_side_now_predicate",
            description: "A now() predicate on the preserved side does not change the join type and stays symbolic.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM plan_cache_left AS l
LEFT JOIN plan_cache_right AS r ON l.k = r.k
WHERE l.ts > now()",
        }),
        (false, SqlTestCase {
            name: "symbolic_now_projection",
            description: "now() in a projection stays symbolic in the logical plan.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT l.ts > now() FROM plan_cache_left AS l",
        }),
        (true, SqlTestCase {
            name: "constant_scalar_subquery_with_now",
            description: "Decorrelation folds `(SELECT now())` into a constant.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT l.ts < (SELECT now()) FROM plan_cache_left AS l",
        }),
        (true, SqlTestCase {
            name: "values_with_now",
            description: "VALUES rows are evaluated while binding.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT v.n, l.k FROM (VALUES (now())) AS v(n), plan_cache_left AS l",
        }),
        (true, SqlTestCase {
            name: "lambda_body_with_now",
            description: "The type checker folds now() in a lambda body.",
            setup_sqls: &[LEFT_TABLE],
            sql: "SELECT array_filter([l.ts], x -> x < now()) FROM plan_cache_left AS l",
        }),
    ];

    for (expected, case) in &cases {
        assert_eq!(
            holds_query_specific_value(case).await?,
            *expected,
            "{}: {}",
            case.name,
            case.description
        );
    }

    Ok(())
}

const LEFT_TABLE: &str = "CREATE TABLE plan_cache_left
(
    k INTEGER,
    ts TIMESTAMP
)";

const RIGHT_TABLE: &str = "CREATE TABLE plan_cache_right
(
    k INTEGER,
    ts TIMESTAMP
)";

/// `RuleFoldCountAggregate` replaces `count(v)` with a constant computed from statistics. The
/// statistics are derived with the query's function context, so with `now()` in the input the
/// constant is valid only for this query. The test context fixes `now()` to the Unix epoch
/// (second 0), so the `if` below yields 1 and the window output has exactly zero NULLs.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_count_folded_from_time_dependent_statistics() -> Result<()> {
    let cases = [
        (true, SqlTestCase {
            name: "count_over_now_dependent_window",
            description: "The exact NULL count of the lag output depends on now().",
            setup_sqls: &[],
            sql: "SELECT count(v) FROM (SELECT lag(if(to_second(now()) % 2 = 0, 1, NULL), 0) OVER (ORDER BY k) AS v FROM plan_cache_counted) AS w",
        }),
        (false, SqlTestCase {
            name: "count_over_constant_window",
            description: "The same fold over a constant does not depend on the query.",
            setup_sqls: &[],
            sql: "SELECT count(v) FROM (SELECT lag(1, 0) OVER (ORDER BY k) AS v FROM plan_cache_counted) AS w",
        }),
        (false, SqlTestCase {
            name: "count_star",
            description: "count(*) is folded from the table row count alone.",
            setup_sqls: &[],
            sql: "SELECT count(*) FROM plan_cache_counted",
        }),
    ];

    for (expected, case) in &cases {
        let ctx = LiteTableContext::create().await?;
        ctx.register_table_sql_with_stats(
            COUNTED_TABLE,
            Some(table_statistics(2)),
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
        assert_eq!(
            plan_holds_query_specific_value(&ctx, case).await?,
            *expected,
            "{}: {}",
            case.name,
            case.description
        );
    }

    Ok(())
}

const COUNTED_TABLE: &str = "CREATE TABLE plan_cache_counted
(
    k INTEGER NOT NULL
)";
