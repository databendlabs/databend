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
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::SqlTestOutcome;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;
use crate::framework::golden::write_case_outcome;

mod alias_resolution;

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

const TEST_SCRIPT_UDF_SQL: &str = r#"
CREATE OR REPLACE FUNCTION add_one (INT) RETURNS INT
LANGUAGE javascript HANDLER = 'add_one' AS $$
export function add_one(v) {
    return v + 1;
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

async fn bind_case_with_commercial_license(case: &SqlTestCase) -> Result<SqlTestOutcome> {
    let ctx = setup_context(case).await?;
    ctx.enable_commercial_license_for_test();
    let outcome = match ctx.bind_sql(case.sql).await {
        Ok(plan) => SqlTestOutcome::Plan(plan.format_indent(Default::default())?),
        Err(err) => SqlTestOutcome::Error {
            code: err.code(),
            message: err.message(),
        },
    };
    Ok(outcome)
}

async fn run_binder_cases(file_name: &str, cases: &[SqlTestCase]) -> Result<()> {
    let mut file = open_golden_file("semantic", file_name)?;

    for case in cases {
        write_case_header(&mut file, case)?;
        let outcome = bind_case(case).await?;
        write_case_outcome(&mut file, &outcome)?;
    }

    Ok(())
}

async fn run_binder_cases_with_commercial_license(
    file_name: &str,
    cases: &[SqlTestCase],
) -> Result<()> {
    let mut file = open_golden_file("semantic", file_name)?;

    for case in cases {
        write_case_header(&mut file, case)?;
        let outcome = bind_case_with_commercial_license(case).await?;
        write_case_outcome(&mut file, &outcome)?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binder_clauses_and_ordering() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "simple_aggregate_query_binds",
            description: "A plain aggregate query should bind successfully.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT avg(number) FROM t",
        },
        SqlTestCase {
            name: "where_rejects_udaf",
            description: "A UDAF in WHERE must be rejected like any other aggregate.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT a FROM t WHERE weighted_avg(a, b) > 0",
        },
        SqlTestCase {
            name: "qualify_rejects_direct_aggregate",
            description: "A raw aggregate expression must be rejected directly in QUALIFY.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number FROM t QUALIFY sum(number) > 0",
        },
        SqlTestCase {
            name: "having_aggregate_does_not_make_scalar_projection_valid",
            description: "Introducing an aggregate in HAVING must not make a non-aggregated SELECT list valid.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number FROM t HAVING count(*) > 0",
        },
        SqlTestCase {
            name: "having_aggregate_reuses_select_alias_name_as_input_column",
            description: "A HAVING aggregate argument should resolve to the input column even when a SELECT aggregate has the same alias.",
            setup_sqls: &[
                "CREATE TABLE t(creative_name String, impressions UInt64, clicks UInt64, cost UInt64, installs UInt64)",
            ],
            sql: "SELECT creative_name, sum(cost) AS cost FROM t GROUP BY creative_name HAVING sum(impressions) > 0 OR sum(clicks) > 0 OR sum(cost) > 0 OR sum(installs) > 0",
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
            name: "distinct_order_by_reuses_same_aggregate_select_item",
            description: "SELECT DISTINCT should still accept an ORDER BY aggregate expression when it is already present in the select list.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT DISTINCT sum(number) FROM t ORDER BY sum(number)",
        },
        SqlTestCase {
            name: "distinct_order_by_reuses_same_window_select_item",
            description: "SELECT DISTINCT should still accept an ORDER BY window expression when it is already present in the select list.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT DISTINCT row_number() OVER (ORDER BY number) FROM t ORDER BY row_number() OVER (ORDER BY number)",
        },
        SqlTestCase {
            name: "table_function_named_arguments_require_fat_arrow",
            description: "A table function named argument written with '=' should produce a direct hint to use '=>'.",
            setup_sqls: &[],
            sql: "SELECT * FROM infer_schema(location = '@data/parquet/int96.parquet')",
        },
        SqlTestCase {
            name: "obfuscate_named_arguments_require_fat_arrow",
            description: "OBFUSCATE should surface the same '=>' hint when a named argument is written with '='.",
            setup_sqls: &["CREATE TABLE t1(a String)"],
            sql: "SELECT * FROM obfuscate(t1, seed = 20)",
        },
    ];

    run_binder_cases("binder_clauses.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binder_mutation_udf() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "update_where_accepts_script_udf",
            description: "A mutation filter should allow script UDFs and rewrite them before the filter is evaluated.",
            setup_sqls: &["CREATE TABLE t(a INT, b INT)", TEST_SCRIPT_UDF_SQL],
            sql: "UPDATE t SET b = a WHERE add_one(a) > 1",
        },
        SqlTestCase {
            name: "delete_where_accepts_script_udf",
            description: "DELETE should allow script UDFs in the mutation filter.",
            setup_sqls: &["CREATE TABLE t(a INT, b INT)", TEST_SCRIPT_UDF_SQL],
            sql: "DELETE FROM t WHERE add_one(a) > 1",
        },
        SqlTestCase {
            name: "merge_matched_condition_accepts_script_udf",
            description: "MERGE matched conditions should allow script UDFs.",
            setup_sqls: &[
                "CREATE TABLE t(a INT, b INT)",
                "CREATE TABLE s(a INT, b INT)",
                TEST_SCRIPT_UDF_SQL,
            ],
            sql: "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED AND add_one(s.b) > 1 THEN UPDATE SET b = add_one(s.b)",
        },
        SqlTestCase {
            name: "merge_unmatched_accepts_script_udf",
            description: "MERGE unmatched conditions and insert values should allow script UDFs.",
            setup_sqls: &[
                "CREATE TABLE t(a INT, b INT)",
                "CREATE TABLE s(a INT, b INT)",
                TEST_SCRIPT_UDF_SQL,
            ],
            sql: "MERGE INTO t USING s ON t.a = s.a WHEN NOT MATCHED AND add_one(s.b) > 1 THEN INSERT (a, b) VALUES (s.a, add_one(s.b))",
        },
    ];

    run_binder_cases("binder_mutation_udf.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binder_window_core_paths() -> Result<()> {
    let cases = [
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
            name: "window_order_reuses_having_aggregate",
            description: "A window ORDER BY clause should be able to reuse an aggregate introduced later by HAVING.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY sum(number)) FROM t HAVING sum(number) > 0",
        },
        SqlTestCase {
            name: "window_order_reuses_having_udaf",
            description: "A window ORDER BY clause should be able to reuse a UDAF introduced later by HAVING.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT row_number() OVER (ORDER BY weighted_avg(a, b)) FROM t HAVING weighted_avg(a, b) > 0",
        },
        SqlTestCase {
            name: "window_order_reuses_order_by_aggregate",
            description: "A window ORDER BY clause should be able to reuse an aggregate introduced later by ORDER BY.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY sum(number)) FROM t ORDER BY sum(number)",
        },
        SqlTestCase {
            name: "window_order_reuses_order_by_udaf",
            description: "A window ORDER BY clause should be able to reuse a UDAF introduced later by ORDER BY.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT row_number() OVER (ORDER BY weighted_avg(a, b)) FROM t ORDER BY weighted_avg(a, b)",
        },
        SqlTestCase {
            name: "duplicate_window_expression_reuses_window_binding",
            description: "Repeated identical window expressions should reuse the registered window binding.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY number), row_number() OVER (ORDER BY number) FROM t",
        },
        SqlTestCase {
            name: "multiple_window_expressions_use_window_group",
            description: "Multiple distinct window expressions should bind through WindowGroup instead of nested Window nodes.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY number), rank() OVER (PARTITION BY number % 3 ORDER BY number) FROM t",
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
            name: "within_group_window_aggregate_binds",
            description: "A WITHIN GROUP window aggregate should bind its sort descriptors without turning into a grouped aggregate.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, empno UInt64, salary UInt64)"],
            sql: "SELECT listagg(cast(salary as varchar), '|') WITHIN GROUP (ORDER BY empno DESC) OVER (PARTITION BY depname ORDER BY empno) FROM empsalary",
        },
    ];

    run_binder_cases("binder_window_core.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_many_window_expressions_bind_as_flat_window_group() -> Result<()> {
    let case = SqlTestCase {
        name: "many_window_expressions_bind_as_flat_window_group",
        description: "Many distinct window expressions should bind as one flat WindowGroup instead of a deep Window chain.",
        setup_sqls: &["CREATE TABLE t(number UInt64)"],
        sql: "SELECT 1",
    };
    let ctx = setup_context(&case).await?;

    let window_count = 128;
    let select_items = (0..window_count)
        .map(|i| format!("lead(number, {i}, number) OVER (ORDER BY number) AS w{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {select_items} FROM t");

    let plan = ctx.bind_sql(&sql).await?;
    let Plan::Query { s_expr, .. } = plan else {
        panic!("expected query plan");
    };

    let mut stats = WindowPlanStats::default();
    collect_window_plan_stats(&s_expr, &mut stats);

    assert_eq!(stats.window_group_nodes, 1);
    assert_eq!(stats.window_nodes, 0);
    assert_eq!(stats.window_group_windows, window_count);
    assert!(
        stats.max_depth < 16,
        "window plan should stay shallow, got depth {}",
        stats.max_depth
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_mixed_partition_windows_bind_as_partitioned_window_groups() -> Result<()> {
    let case = SqlTestCase {
        name: "mixed_partition_windows_bind_as_partitioned_window_groups",
        description: "Window expressions with different partition requirements should bind as separate WindowGroup nodes.",
        setup_sqls: &["CREATE TABLE t(number UInt64, value UInt64)"],
        sql: "SELECT row_number() OVER (ORDER BY value) AS w0, rank() OVER (PARTITION BY number % 3 ORDER BY value) AS w1, dense_rank() OVER (PARTITION BY number % 3 ORDER BY value DESC) AS w2 FROM t",
    };
    let ctx = setup_context(&case).await?;

    let plan = ctx.bind_sql(case.sql).await?;
    let Plan::Query { s_expr, .. } = plan else {
        panic!("expected query plan");
    };

    let mut stats = WindowPlanStats::default();
    collect_window_plan_stats(&s_expr, &mut stats);

    assert_eq!(stats.window_group_nodes, 2);
    assert_eq!(stats.window_nodes, 0);
    assert_eq!(stats.window_group_windows, 3);
    assert!(
        stats.max_depth < 8,
        "window plan should stay shallow, got depth {}",
        stats.max_depth
    );

    Ok(())
}

#[derive(Default)]
struct WindowPlanStats {
    window_group_nodes: usize,
    window_group_windows: usize,
    window_nodes: usize,
    max_depth: usize,
}

fn collect_window_plan_stats(s_expr: &SExpr, stats: &mut WindowPlanStats) {
    collect_window_plan_stats_inner(s_expr, stats, 1);
}

fn collect_window_plan_stats_inner(s_expr: &SExpr, stats: &mut WindowPlanStats, depth: usize) {
    stats.max_depth = stats.max_depth.max(depth);
    match s_expr.plan() {
        RelOperator::Window(_) => {
            stats.window_nodes += 1;
        }
        RelOperator::WindowGroup(window_group) => {
            stats.window_group_nodes += 1;
            stats.window_group_windows += window_group.windows.len();
        }
        _ => {}
    }

    for child in s_expr.children() {
        collect_window_plan_stats_inner(child, stats, depth + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binder_named_window_paths() -> Result<()> {
    let cases = [
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
            name: "named_window_aggregate_inside_nested_window_expression_binds",
            description: "A named window aggregate introduced from a referenced window spec must still bind when the window expression is nested inside a larger select expression.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT depname, sum(sum(salary)) OVER w + 1 FROM empsalary GROUP BY depname WINDOW w AS (PARTITION BY 1 ORDER BY sum(salary))",
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
            name: "missing_named_window_in_select_prebind_errors",
            description: "A missing named window referenced from a select-item window expression must fail during prebinding instead of being silently ignored.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT depname, sum(sum(salary)) OVER w FROM empsalary GROUP BY depname",
        },
        SqlTestCase {
            name: "named_window_rejects_duplicate_name",
            description: "A WINDOW clause must reject duplicate names instead of silently keeping the later definition.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, salary UInt64)"],
            sql: "SELECT rank() OVER w FROM empsalary WINDOW w AS (PARTITION BY depname), W AS (ORDER BY salary)",
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
    ];

    run_binder_cases("binder_window_named.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binder_grouping_and_srf_paths() -> Result<()> {
    let cases = [
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
            name: "group_by_all_collects_non_aggregate_select_items",
            description: "GROUP BY ALL should expand to the non-aggregate SELECT items only.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number % 2 AS a, sum(number) FROM t GROUP BY ALL",
        },
        SqlTestCase {
            name: "select_scalar_wraps_builtin_aggregate",
            description: "A scalar wrapper over a builtin aggregate should bind through the select aggregate path without requiring direct aggregate support in type checking.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) + 1 FROM t",
        },
        SqlTestCase {
            name: "grouped_select_udaf_binds",
            description: "A grouped SELECT should rewrite UDAF output through the aggregate path like builtin aggregates.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT a % 2 AS g, weighted_avg(a, b) FROM t GROUP BY g",
        },
        SqlTestCase {
            name: "select_scalar_wraps_udaf",
            description: "A scalar wrapper over a UDAF should keep binding through the normal UDAF path while coexisting with builtin aggregate prebinding.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT weighted_avg(a, b) + 1 FROM t",
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
            name: "within_group_group_aggregate_binds",
            description: "A non-window WITHIN GROUP aggregate should register its sort descriptors in the aggregate phase.",
            setup_sqls: &["CREATE TABLE empsalary(empno UInt64, salary UInt64)"],
            sql: "SELECT listagg(cast(salary as varchar), '|') WITHIN GROUP (ORDER BY empno DESC) FROM empsalary",
        },
        SqlTestCase {
            name: "grouping_sets_select_alias_with_grouping_func_does_not_shadow_column",
            description: "A SELECT alias containing grouping() must not shadow the underlying column in GROUPING SETS items.",
            setup_sqls: &[
                "CREATE TABLE events(category_id UInt64, label String, amount Decimal(18,6))",
            ],
            sql: "SELECT if(grouping(category_id)=1, 0, category_id) AS category_id, label, sum(amount) FROM events GROUP BY GROUPING SETS ((label), (category_id, label))",
        },
    ];

    run_binder_cases("binder_grouping.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clause_prepass_skips_subquery_metadata_side_effects() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "having_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) FROM t HAVING EXISTS (SELECT 1 FROM t AS inner_t WHERE inner_t.number > 0)",
        },
        SqlTestCase {
            name: "order_by_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number FROM t ORDER BY (SELECT max(number) FROM t AS inner_t)",
        },
    ];

    for case in cases {
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(case.sql).await?;
        let Plan::Query { metadata, .. } = plan else {
            panic!("expected query plan for {}", case.name);
        };

        let table_count = metadata.read().tables().len();
        assert_eq!(
            table_count, 2,
            "{} should only keep metadata for the outer query and the final subquery bind",
            case.name
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_materialized_cte_virtual_column_rewrite() -> Result<()> {
    const CREATE_VIRTUAL_COLUMN_TABLE: &str =
        "CREATE TABLE t(v JSON NULL) storage_format = 'parquet' enable_virtual_column = true";
    const CREATE_NON_VIRTUAL_COLUMN_TABLE: &str =
        "CREATE TABLE t_no_vc(v JSON NULL) storage_format = 'parquet'";
    const CREATE_OTHER_MESSAGE_TABLE: &str =
        "CREATE TABLE other_table(message JSON NULL) storage_format = 'parquet'";

    const SQL_WITH_CTE: &str = r#"
    settings (enable_experimental_virtual_column = 1, enable_auto_materialize_cte = 1) 
    WITH logs AS (SELECT v['message'] AS message FROM t) 
        SELECT message['attribute']['user_id'] FROM logs 
        UNION ALL SELECT message['attribute']['account_id'] FROM logs;
"#;

    const SQL_WITH_CHAINED_CTE: &str = r#"
    settings (enable_experimental_virtual_column = 1, enable_auto_materialize_cte = 1) 
    WITH logs AS (SELECT v['message'] AS message FROM t), 
        attrs AS (SELECT message['attribute'] AS attr FROM logs),
        users AS (SELECT attr['user_id'] AS user_id, attr['name'] AS name FROM attrs)
        SELECT user_id, name FROM users;
"#;

    const SQL_WITH_CTE_MULTI_FIELDS: &str = r#"
    settings (enable_experimental_virtual_column = 1, enable_auto_materialize_cte = 1) 
    WITH logs AS (SELECT v['message'] AS message, v['response'] AS response FROM t),
        base AS (SELECT message['attribute']['user_id']::Int32 AS user_id, 
            message['attribute']['trace_id']::Int32 AS trace_id, 
            message['attribute']['level']::String AS level,
            response['status_code']::Int64 AS status_code,
            response['error_message']::String AS error_message FROM logs)
        SELECT user_id, trace_id, level, status_code, error_message FROM base
"#;

    const SQL_WITHOUT_TABLE_VIRTUAL_COLUMNS: &str = r#"
    settings (enable_experimental_virtual_column = 1, enable_auto_materialize_cte = 1)
    WITH logs AS (SELECT v['message'] AS message FROM t_no_vc)
        SELECT message['attribute']['user_id'] FROM logs
        UNION ALL SELECT message['attribute']['account_id'] FROM logs;
    "#;

    const SQL_WITH_AMBIGUOUS_UNQUALIFIED_COLUMN: &str = r#"
    settings (enable_experimental_virtual_column = 1, enable_auto_materialize_cte = 1)
    WITH logs AS (SELECT v['message'] AS message FROM t)
        SELECT message['attribute']['user_id'] FROM logs, other_table
        UNION ALL SELECT message['attribute']['account_id'] FROM logs, other_table;
    "#;

    let cases = [
        SqlTestCase {
            name: "materialized_cte_virtual_column_rewrites",
            description: "A materialized CTE consumer should still expose the full base-table JSON path when virtual-column rewrite is enabled.",
            setup_sqls: &[CREATE_VIRTUAL_COLUMN_TABLE],
            sql: SQL_WITH_CTE,
        },
        SqlTestCase {
            name: "materialized_cte_virtual_column_rewrites_chained_ctes",
            description: "A chained materialized CTE should preserve the original base-table JSON path across multiple CTE layers.",
            setup_sqls: &[CREATE_VIRTUAL_COLUMN_TABLE],
            sql: SQL_WITH_CHAINED_CTE,
        },
        SqlTestCase {
            name: "materialized_cte_virtual_column_rewrites_multi_fields",
            description: "Multiple downstream CTE fields reading the same materialized source CTE should push their JSON path requirements back to the source.",
            setup_sqls: &[CREATE_VIRTUAL_COLUMN_TABLE],
            sql: SQL_WITH_CTE_MULTI_FIELDS,
        },
        SqlTestCase {
            name: "materialized_cte_static_json_paths_without_table_virtual_columns",
            description: "A materialized CTE should still precompute static JSON paths inside the producer even when the source table has no Fuse virtual columns.",
            setup_sqls: &[CREATE_NON_VIRTUAL_COLUMN_TABLE],
            sql: SQL_WITHOUT_TABLE_VIRTUAL_COLUMNS,
        },
        SqlTestCase {
            name: "materialized_cte_unqualified_column_preserves_ambiguity",
            description: "An unqualified materialized CTE output should not be rewritten before binder can reject an ambiguous column reference.",
            setup_sqls: &[CREATE_VIRTUAL_COLUMN_TABLE, CREATE_OTHER_MESSAGE_TABLE],
            sql: SQL_WITH_AMBIGUOUS_UNQUALIFIED_COLUMN,
        },
    ];

    run_binder_cases_with_commercial_license("binder_materialized_cte_virtual_column.txt", &cases)
        .await
}
