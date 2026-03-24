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

const TEST_UDAF_SQL: &str = r#"
CREATE OR REPLACE FUNCTION weighted_avg (a INT, b INT) STATE { sum INT, weight INT } RETURNS FLOAT
LANGUAGE javascript AS $$
export function create_state() {
    return {sum: 0, weight: 0};
}
export function accumulate(state, value, weight) {
    state.sum += value * weight;
    state.weight += weight;
    return state;
}
export function retract(state, value, weight) {
    state.sum -= value * weight;
    state.weight -= weight;
    return state;
}
export function merge(state1, state2) {
    state1.sum += state2.sum;
    state1.weight += state2.weight;
    return state1;
}
export function finish(state) {
    return state.sum / state.weight;
}
$$
"#;

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
            name: "where_alias_to_srf_uses_project_set_binding",
            description: "A WHERE clause that references an SRF alias should keep the alias bound to the ProjectSet column instead of expanding back to the raw SRF.",
            setup_sqls: &[],
            sql: "SELECT unnest([1, 2, 3]) AS u WHERE u = 1",
        },
        SqlTestCase {
            name: "where_rejects_udaf",
            description: "A UDAF in WHERE must be rejected like any other aggregate.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT a FROM t WHERE weighted_avg(a, b) > 0",
        },
        SqlTestCase {
            name: "qualify_rejects_aggregate_alias",
            description: "An aggregate alias must still be rejected in QUALIFY.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s FROM t QUALIFY s > 0",
        },
        SqlTestCase {
            name: "qualify_rejects_direct_aggregate",
            description: "A raw aggregate expression must be rejected directly in QUALIFY.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number FROM t QUALIFY sum(number) > 0",
        },
        SqlTestCase {
            name: "qualify_rejects_udaf_alias",
            description: "A UDAF alias must still be rejected in QUALIFY.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT weighted_avg(a, b) AS s FROM t QUALIFY s > 0",
        },
        SqlTestCase {
            name: "qualify_accepts_window_alias",
            description: "A window alias should remain usable in QUALIFY.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number, row_number() OVER (ORDER BY number) AS rn FROM t QUALIFY rn = 1",
        },
        SqlTestCase {
            name: "qualify_alias_to_srf_uses_project_set_binding",
            description: "A QUALIFY clause that references an SRF alias should keep the alias bound to the ProjectSet column instead of expanding back to the raw SRF.",
            setup_sqls: &[],
            sql: "SELECT unnest([1, 2, 3]) AS u QUALIFY u = 1",
        },
        SqlTestCase {
            name: "having_accepts_aggregate_alias",
            description: "An aggregate alias should remain usable in HAVING.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s FROM t HAVING s > 0",
        },
        SqlTestCase {
            name: "having_aggregate_does_not_make_scalar_projection_valid",
            description: "Introducing an aggregate in HAVING must not make a non-aggregated SELECT list valid.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number FROM t HAVING count(*) > 0",
        },
        SqlTestCase {
            name: "order_by_can_introduce_aggregate_in_aggregate_query",
            description: "ORDER BY may introduce a new aggregate expression when the query is already aggregated.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT count(*) FROM t ORDER BY sum(number)",
        },
        SqlTestCase {
            name: "order_by_aggregate_does_not_make_scalar_projection_valid",
            description: "Introducing an aggregate in ORDER BY must not make a non-aggregated SELECT list valid.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number FROM t ORDER BY sum(number)",
        },
        SqlTestCase {
            name: "order_by_count_does_not_make_scalar_projection_valid",
            description: "The sqllogictest ORDER BY count(*) pattern must still reject a scalar projection.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number FROM t ORDER BY count(*) + 1",
        },
        SqlTestCase {
            name: "aggregate_argument_prefers_base_column_over_select_alias",
            description: "Inside an aggregate function, a same-name select alias should not shadow the base column.",
            setup_sqls: &["CREATE TABLE t(a UInt64, c2 UInt64)"],
            sql: "SELECT a AS c2, sum(c2) FROM t GROUP BY a",
        },
        SqlTestCase {
            name: "aggregate_argument_can_fallback_to_select_alias_in_select_clause",
            description: "Inside the SELECT list, an aggregate argument should still fall back to a same-select alias when no base column exists.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number % 3 AS c1, sum(c1) FROM t GROUP BY number % 3",
        },
        SqlTestCase {
            name: "window_aggregate_does_not_become_group_aggregate",
            description: "An aggregate used as a window function should stay in the window phase rather than becoming a group aggregate.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) OVER () FROM t",
        },
        SqlTestCase {
            name: "window_partition_rejects_new_aggregate",
            description: "A window PARTITION BY clause must not introduce a new aggregate expression.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (PARTITION BY sum(number)) FROM t",
        },
        SqlTestCase {
            name: "window_order_rejects_new_aggregate",
            description: "A window ORDER BY clause must not introduce a new aggregate expression.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY sum(number)) FROM t",
        },
        SqlTestCase {
            name: "duplicate_window_expression_reuses_window_binding",
            description: "Repeated identical window expressions should reuse the registered window binding.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY number), row_number() OVER (ORDER BY number) FROM t",
        },
        SqlTestCase {
            name: "laglead_window_from_sqllogictest_binds",
            description: "A sqllogictest LEAD window pattern should still bind through the lag/lead rewrite path.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT lead(number, 1, 0) OVER (PARTITION BY number % 3 ORDER BY number + 1) FROM t",
        },
        SqlTestCase {
            name: "nth_value_window_binds",
            description: "An NTH_VALUE window expression should still bind through the dedicated nth_value rewrite path.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT nth_value(number, 2) OVER (ORDER BY number) FROM t",
        },
        SqlTestCase {
            name: "window_partition_rewrites_group_item_expression",
            description: "A window partition expression over grouped aliases should rewrite non-column group items back to their group-item columns.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number % 3 AS a, number % 4 AS b, row_number() OVER (PARTITION BY b % 2 ORDER BY a) FROM t GROUP BY a, b",
        },
        SqlTestCase {
            name: "qualify_cte_then_outer_aggregate_from_sqllogictest_binds",
            description: "A sqllogictest pattern that filters with QUALIFY inside a CTE before an outer aggregate should still bind.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "WITH test AS (SELECT number % 10 AS id, number AS full_matched FROM t QUALIFY row_number() OVER (PARTITION BY id ORDER BY number DESC) = 1) SELECT full_matched, count() FROM test GROUP BY full_matched HAVING full_matched = 3",
        },
        SqlTestCase {
            name: "named_window_from_sqllogictest_binds",
            description: "A named WINDOW clause from sqllogictests should bind as a normal window specification.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, empno UInt64, salary UInt64)"],
            sql: "SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY empno)",
        },
        SqlTestCase {
            name: "named_window_aggregate_order_by_existing_group_aggregate_binds",
            description: "A grouped query should be able to introduce an aggregate inside a named window clause and reuse it across the window aggregate and ORDER BY.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT depname, sum(sum(salary)) OVER w FROM empsalary GROUP BY depname WINDOW w AS (PARTITION BY 1 ORDER BY sum(salary))",
        },
        SqlTestCase {
            name: "inherited_named_window_from_sqllogictest_binds",
            description: "An inherited named WINDOW specification should bind without losing the base partition spec.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT rank() OVER w1, dense_rank() OVER w2 FROM empsalary WINDOW w1 AS (PARTITION BY depname), w2 AS (w1 ORDER BY salary DESC)",
        },
        SqlTestCase {
            name: "recursive_named_window_chain_binds",
            description: "A recursive chain of named WINDOW references should resolve inherited partition and order specs.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT rank() OVER w3 FROM empsalary WINDOW w1 AS (PARTITION BY depname ORDER BY salary), w2 AS (w1), w3 AS (w2)",
        },
        SqlTestCase {
            name: "inherited_named_window_rejects_partition_override",
            description: "Referencing a named window must not add a new PARTITION BY clause.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT rank() OVER w2 FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (w1 PARTITION BY depname)",
        },
        SqlTestCase {
            name: "inherited_named_window_rejects_duplicate_order_by",
            description: "Referencing a named window with ORDER BY must not specify another ORDER BY.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT rank() OVER w2 FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (w1 ORDER BY depname)",
        },
        SqlTestCase {
            name: "inherited_named_window_rejects_base_frame",
            description: "Referencing a named window that already contains a frame specification must be rejected.",
            setup_sqls: &["CREATE TABLE empsalary(salary UInt64)"],
            sql: "SELECT sum(salary) OVER w2 FROM empsalary WINDOW w1 AS (ORDER BY salary ROWS CURRENT ROW), w2 AS (w1)",
        },
        SqlTestCase {
            name: "unnest_over_aggregate_is_planned_after_aggregate",
            description: "A set-returning function over an aggregate should stay above the aggregate phase instead of rewriting the aggregate away early.",
            setup_sqls: &[],
            sql: "SELECT unnest(max([11, 12]))",
        },
        SqlTestCase {
            name: "duplicate_srf_expression_reuses_project_set_binding",
            description: "Repeated identical SRF expressions should reuse the registered ProjectSet binding.",
            setup_sqls: &[],
            sql: "SELECT unnest([1, 2, 3]), unnest([1, 2, 3])",
        },
        SqlTestCase {
            name: "qualify_named_window_with_subquery_binds",
            description: "QUALIFY should preserve subqueries while checking named window aliases.",
            setup_sqls: &["CREATE TABLE qt(i UInt64, p String, o UInt64)"],
            sql: "SELECT i, p, o, row_number() OVER w AS rn FROM qt WINDOW w AS (PARTITION BY p ORDER BY o) QUALIFY rn = (SELECT i FROM qt LIMIT 1)",
        },
        SqlTestCase {
            name: "qualify_grouping_context_uses_grouping_checker",
            description: "QUALIFY in a grouped query should still accept grouped aliases while binding the window phase.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number % 2 AS a, row_number() OVER (ORDER BY a) AS rn FROM t GROUP BY a QUALIFY a = 1",
        },
        SqlTestCase {
            name: "qualify_grouping_context_rejects_aggregate_alias",
            description: "QUALIFY in a grouped query must still reject aggregate aliases while using grouping-aware binding.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number % 2 AS a, sum(number) AS s, row_number() OVER (ORDER BY a) AS rn FROM t GROUP BY a QUALIFY s > 0",
        },
        SqlTestCase {
            name: "group_by_srf_alias_from_sqllogictest_binds",
            description: "A sqllogictest GROUP BY pattern that groups by an SRF select alias should bind successfully.",
            setup_sqls: &["CREATE TABLE t_str(col1 String, col2 String)"],
            sql: "SELECT t.col1 AS col1, unnest(split(t.col2, ',')) AS col3 FROM t_str AS t GROUP BY col1, col3 ORDER BY col3",
        },
        SqlTestCase {
            name: "group_by_all_collects_non_aggregate_select_items",
            description: "GROUP BY ALL should expand to the non-aggregate SELECT items only.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number % 2 AS a, sum(number) FROM t GROUP BY ALL",
        },
        SqlTestCase {
            name: "grouped_select_udaf_binds",
            description: "A grouped SELECT should rewrite UDAF output through the aggregate path like builtin aggregates.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT a % 2 AS g, weighted_avg(a, b) FROM t GROUP BY g",
        },
        SqlTestCase {
            name: "group_by_all_collects_non_udaf_select_items",
            description: "GROUP BY ALL should also skip UDAF select items when inferring grouping keys.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT a % 2 AS g, weighted_avg(a, b) FROM t GROUP BY ALL",
        },
        SqlTestCase {
            name: "group_by_rejects_udaf_item",
            description: "Explicit GROUP BY items must reject UDAF calls just like builtin aggregates.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT weighted_avg(a, b) FROM t GROUP BY weighted_avg(a, b)",
        },
        SqlTestCase {
            name: "combined_grouping_sets_binds",
            description: "A normal GROUP BY item combined with GROUPING SETS should bind through the combined-group expansion path.",
            setup_sqls: &["CREATE TABLE sales(brand String, segment String, quantity UInt64)"],
            sql: "SELECT quantity, brand, segment, sum(quantity) FROM sales GROUP BY brand, GROUPING SETS(segment, quantity)",
        },
        SqlTestCase {
            name: "grouping_outside_grouping_sets_is_rejected",
            description: "grouping() should still be rejected when the query is not using grouping sets semantics.",
            setup_sqls: &["CREATE TABLE g(a UInt64, b UInt64, c UInt64)"],
            sql: "SELECT a, grouping(a) FROM g GROUP BY a",
        },
        SqlTestCase {
            name: "grouping_rejects_non_group_item_argument",
            description: "grouping() arguments must still be actual GROUP BY expressions inside grouping sets.",
            setup_sqls: &["CREATE TABLE g(a UInt64, b UInt64, c UInt64)"],
            sql: "SELECT a, grouping(c) FROM g GROUP BY GROUPING SETS ((a), ())",
        },
        SqlTestCase {
            name: "cube_grouping_function_binds",
            description: "CUBE should expand into grouping sets and allow grouping(...) to bind against the generated grouping id.",
            setup_sqls: &["CREATE TABLE g(a UInt64, b UInt64, c UInt64)"],
            sql: "SELECT a, b, sum(c) AS sc, grouping(a, b) FROM g GROUP BY CUBE(a, b)",
        },
        SqlTestCase {
            name: "aggregate_over_srf_from_sqllogictest_binds",
            description: "A sqllogictest aggregate-over-SRF pattern should still bind with ProjectSet below Aggregate.",
            setup_sqls: &["CREATE TABLE t_str(col2 String)"],
            sql: "SELECT max(unnest(split(t.col2, ','))) FROM t_str AS t",
        },
        SqlTestCase {
            name: "unnest_over_wrapped_aggregate_from_sqllogictest_binds",
            description: "A sqllogictest SRF-over-aggregate pattern with an extra scalar wrapper should still bind with ProjectSet above Aggregate.",
            setup_sqls: &["CREATE TABLE t_str(col2 String)"],
            sql: "SELECT unnest(split(max(t.col2), ',')) FROM t_str AS t",
        },
        SqlTestCase {
            name: "grouping_function_inside_window_over_rollup_binds",
            description: "A sqllogictest grouping() pattern should still rewrite correctly when used inside a window over rollup output.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT grouping(salary), grouping(depname), sum(grouping(salary)) OVER (PARTITION BY grouping(salary) + grouping(depname) ORDER BY grouping(depname) DESC) FROM empsalary GROUP BY ROLLUP(depname, salary)",
        },
        SqlTestCase {
            name: "within_group_window_aggregate_binds",
            description: "A WITHIN GROUP window aggregate should bind its sort descriptors without turning into a grouped aggregate.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, empno UInt64, salary UInt64)"],
            sql: "SELECT listagg(cast(salary as varchar), '|') WITHIN GROUP (ORDER BY empno DESC) OVER (PARTITION BY depname ORDER BY empno) FROM empsalary",
        },
        SqlTestCase {
            name: "within_group_group_aggregate_binds",
            description: "A non-window WITHIN GROUP aggregate should register its sort descriptors in the aggregate phase.",
            setup_sqls: &["CREATE TABLE empsalary(empno UInt64, salary UInt64)"],
            sql: "SELECT listagg(cast(salary as varchar), '|') WITHIN GROUP (ORDER BY empno DESC) FROM empsalary",
        },
    ];

    for case in &cases {
        write_case_header(&mut file, case)?;
        let outcome = bind_case(case).await?;
        write_case_outcome(&mut file, &outcome)?;
    }

    Ok(())
}
