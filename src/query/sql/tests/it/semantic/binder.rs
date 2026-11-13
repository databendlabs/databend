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
use databend_common_sql::binder::MutationStrategy;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::SqlTestOutcome;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;
use crate::framework::golden::write_case_outcome;

mod alias_resolution;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_window_input_reuse_preserves_nondeterministic_expressions() -> Result<()> {
    let case = SqlTestCase {
        name: "window_input_reuse_preserves_nondeterministic_expressions",
        description: "Only deterministic window inputs may replace SELECT expressions.",
        setup_sqls: &["CREATE TABLE t(number UInt64)"],
        sql: "SELECT number + 1 AS n, rand() AS r, \
              row_number() OVER (PARTITION BY number + 1 ORDER BY rand()) FROM t",
    };
    let ctx = setup_context(&case).await?;
    let Plan::Query {
        s_expr,
        bind_context,
        ..
    } = ctx.bind_sql(case.sql).await?
    else {
        panic!("expected query plan");
    };
    let RelOperator::EvalScalar(projection) = s_expr.logical()?.plan() else {
        panic!("expected SELECT projection");
    };
    for (name, reused) in [("n", true), ("r", false)] {
        let column = bind_context
            .columns
            .iter()
            .find(|column| column.column_name == name)
            .unwrap();
        let item = projection
            .items
            .iter()
            .find(|item| item.index == column.index)
            .unwrap();
        assert_eq!(
            matches!(item.scalar, ScalarExpr::BoundColumnRef(_)),
            reused,
            "projection {name}"
        );
    }
    Ok(())
}

#[test]
fn test_deep_left_join_binding() -> Result<()> {
    // Leave room for AST cloning without letting the debug query runtime's larger
    // stack hide unguarded recursion in the binder.
    std::thread::Builder::new()
        .name("deep_left_join_binding".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("create test runtime")
                .block_on(async {
                    let case = SqlTestCase {
                        name: "deep_left_join_binding",
                        description: "Deep left JOIN chains must not overflow the binder stack.",
                        setup_sqls: &["CREATE TABLE t(e INTEGER)"],
                        sql: "SELECT 1",
                    };
                    let ctx = setup_context(&case).await?;

                    for count in [60, 512] {
                        let joins = (0..count)
                            .map(|i| format!(" LEFT JOIN t a{i}"))
                            .collect::<String>();
                        let sql = format!("SELECT * FROM t{joins} ON e{joins}");
                        let err = ctx.bind_sql(&sql).await.unwrap_err();
                        assert_eq!(err.code(), 1065);
                        assert_eq!(err.message(), "outer join should contain join conditions");
                    }

                    // Valid conditions must still traverse the deep tree, even if missing
                    // conditions are rejected earlier in the future.
                    let mut sql = "SELECT a0.e FROM t a0".to_string();
                    for i in 1..=256 {
                        sql.push_str(&format!(" LEFT JOIN t a{i} ON a0.e = a{i}.e"));
                    }
                    let plan = ctx.bind_sql(&sql).await?;
                    assert!(matches!(plan, Plan::Query { .. }));
                    Ok(())
                })
        })
        .expect("spawn small-stack binder thread")
        .join()
        .expect("small-stack binder thread panicked")
}

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

const TEST_LAMBDA_WRAPPING_SCRIPT_UDF_SQL: &str =
    "CREATE OR REPLACE FUNCTION wrap_add_one AS (x) -> add_one(x)";

const TEST_SCALAR_WRAPPING_SCRIPT_UDF_SQL: &str =
    "CREATE OR REPLACE FUNCTION scalar_wrap_add_one (x INT) RETURNS INT AS $$ add_one(x) $$";

async fn bind_case(case: &SqlTestCase) -> Result<SqlTestOutcome> {
    let ctx = setup_context(case).await?;
    let outcome = match ctx.bind_sql(case.sql).await {
        Ok(plan) => {
            SqlTestOutcome::Plan(plan.format_indent(Default::default(), &ctx.stat_context()?)?)
        }
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
        Ok(plan) => {
            SqlTestOutcome::Plan(plan.format_indent(Default::default(), &ctx.stat_context()?)?)
        }
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
async fn test_binder_mutation_internal_column_strategy() -> Result<()> {
    let cases = [
        (
            "UPDATE t SET b = to_string(_row_id) WHERE a = 1",
            MutationStrategy::MatchedOnly,
        ),
        (
            "UPDATE t SET b = to_string(a) WHERE a = 1",
            MutationStrategy::Direct,
        ),
    ];

    for (sql, expected_strategy) in cases {
        let case = SqlTestCase {
            name: "update_internal_column_strategy",
            description: "UPDATE strategy is selected after assignment expressions are bound.",
            setup_sqls: &["CREATE TABLE t(a INT, b STRING)"],
            sql,
        };
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(sql).await?;
        let Plan::DataMutation { s_expr, .. } = plan else {
            panic!("expected mutation plan for {sql}");
        };
        let RelOperator::Mutation(mutation) = s_expr.logical()?.plan() else {
            panic!("expected mutation operator for {sql}");
        };
        assert_eq!(mutation.strategy, expected_strategy, "sql: {sql}");
    }

    Ok(())
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
async fn test_binder_nested_udf_rewrite() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "lambda_udf_wrapping_script_udf_is_rewritten",
            description: "A script UDF called from a lambda UDF body must still be rewritten into a UdfScript node.",
            setup_sqls: &[
                "CREATE TABLE t(a INT, b INT)",
                TEST_SCRIPT_UDF_SQL,
                TEST_LAMBDA_WRAPPING_SCRIPT_UDF_SQL,
            ],
            sql: "SELECT wrap_add_one(a) FROM t",
        },
        SqlTestCase {
            name: "sql_scalar_udf_wrapping_script_udf_is_rewritten",
            description: "A script UDF called from a SQL scalar UDF body must still be rewritten into a UdfScript node.",
            setup_sqls: &[
                "CREATE TABLE t(a INT, b INT)",
                TEST_SCRIPT_UDF_SQL,
                TEST_SCALAR_WRAPPING_SCRIPT_UDF_SQL,
            ],
            sql: "SELECT scalar_wrap_add_one(a) FROM t",
        },
        SqlTestCase {
            name: "lambda_udf_wrapping_script_udf_in_filter_is_rewritten",
            description: "A script UDF reached through a lambda UDF inside WHERE must be rewritten before the filter is evaluated.",
            setup_sqls: &[
                "CREATE TABLE t(a INT, b INT)",
                TEST_SCRIPT_UDF_SQL,
                TEST_LAMBDA_WRAPPING_SCRIPT_UDF_SQL,
            ],
            sql: "SELECT a FROM t WHERE wrap_add_one(a) > 1",
        },
        SqlTestCase {
            name: "script_udf_inside_sql_lambda_body_is_rejected",
            description: "A script UDF cannot be lifted out of a SQL lambda body, so it must be rejected with a clear error instead of leaking an internal column id.",
            setup_sqls: &["CREATE TABLE t(a INT, b INT)", TEST_SCRIPT_UDF_SQL],
            sql: "SELECT array_transform([a], x -> add_one(x)) FROM t",
        },
        SqlTestCase {
            name: "lambda_udf_wrapping_script_udf_inside_sql_lambda_body_is_rejected",
            description: "The same rejection must apply when the script UDF is reached indirectly through a lambda UDF.",
            setup_sqls: &[
                "CREATE TABLE t(a INT, b INT)",
                TEST_SCRIPT_UDF_SQL,
                TEST_LAMBDA_WRAPPING_SCRIPT_UDF_SQL,
            ],
            sql: "SELECT array_transform([a], x -> wrap_add_one(x)) FROM t",
        },
        SqlTestCase {
            name: "udaf_script_inside_sql_lambda_body_is_rejected",
            description: "A UDAF script is built outside the aggregate resolution path, so the lambda body check must reject it too.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT array_transform([a], x -> weighted_avg(x, b)) FROM t",
        },
        SqlTestCase {
            name: "pure_sql_lambda_udf_inside_sql_lambda_body_is_allowed",
            description: "A lambda UDF whose body is pure SQL is inlined, so it must remain usable inside a SQL lambda body.",
            setup_sqls: &[
                "CREATE TABLE t(a INT, b INT)",
                "CREATE OR REPLACE FUNCTION sql_add_one AS (x) -> x + 1",
            ],
            sql: "SELECT array_transform([a], x -> sql_add_one(x)) FROM t",
        },
    ];

    run_binder_cases("binder_nested_udf.txt", &cases).await
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
    collect_window_plan_stats(s_expr.logical()?, &mut stats);

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
    collect_window_plan_stats(s_expr.logical()?, &mut stats);

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

/// Plans of grouped SELECT statements and set-returning functions whose shape
/// is their correctness: whether the SELECT is grouped, how often an
/// aggregate is computed, which side of the Aggregate a set-returning
/// function or a subquery is on, and whether an expression reads a key output
/// or is computed again. Results and errors are in sqllogictests, see
/// `query/grouping_sets` (`ordinary`, `key_binding`, `null_injection` and
/// `correlated`), `03_0003_select_group_by` and `03_0047_select_udaf`; name
/// resolution is in the `group_by_alias` alias-resolution matrix.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binder_grouping_and_srf_paths() -> Result<()> {
    const T: &[&str] = &["CREATE TABLE t(a Int32 NOT NULL, b Int32 NOT NULL)"];
    const T_U: &[&str] = &[
        "CREATE TABLE t(a Int32 NOT NULL, b Int32 NOT NULL)",
        "CREATE TABLE u(n Int32)",
    ];
    const V: &[&str] = &["CREATE TABLE v(j Variant)"];
    const T_AGG_UDF: &[&str] = &[
        "CREATE TABLE t(a Int32 NOT NULL, b Int32 NOT NULL)",
        "CREATE OR REPLACE FUNCTION agg_udf AS (x) -> sum(x)",
        "CREATE OR REPLACE FUNCTION agg_udf2 AS (x, y) -> sum(x) + y",
    ];
    const T_SCRIPT_UDF: &[&str] = &[
        "CREATE TABLE t(a Int32 NOT NULL, b Int32 NOT NULL)",
        TEST_SCRIPT_UDF_SQL,
    ];
    let cases = [
        // Whether the SELECT is grouped: an Aggregate is built.
        SqlTestCase {
            name: "window_argument_aggregate_makes_select_grouped",
            description: "An aggregate in a window argument makes the SELECT grouped: the Aggregate is below the Window.",
            setup_sqls: T,
            sql: "SELECT sum(sum(a)) OVER () FROM t",
        },
        SqlTestCase {
            name: "sql_udf_aggregate_makes_select_grouped",
            description: "A SQL UDF whose body aggregates is expanded where it is called, so the SELECT is grouped and the Aggregate computes its sum.",
            setup_sqls: T_AGG_UDF,
            sql: "SELECT agg_udf(a) FROM t",
        },
        // How often an aggregate is computed.
        SqlTestCase {
            name: "aggregate_shared_by_select_and_having",
            description: "The same aggregate in the SELECT list and HAVING is one aggregate function.",
            setup_sqls: T,
            sql: "SELECT count(*) FROM t HAVING count(*) > 0",
        },
        SqlTestCase {
            name: "qualify_window_argument_reuses_aggregate",
            description: "An aggregate in a QUALIFY window argument that the SELECT list has is not computed again.",
            setup_sqls: T,
            sql: "SELECT b, sum(a) FROM t GROUP BY b QUALIFY sum(sum(a)) OVER () > 0",
        },
        SqlTestCase {
            name: "order_by_expression_over_aggregate_alias_reuses_aggregate",
            description: "An ORDER BY expression over an aggregate alias reads the aggregate output.",
            setup_sqls: T,
            sql: "SELECT b, sum(a) AS s FROM t GROUP BY b ORDER BY s + 1",
        },
        // Which side of the Aggregate a set-returning function or a subquery is on, and
        // how often a set-returning function is evaluated.
        SqlTestCase {
            name: "srf_over_aggregate_is_above_aggregate",
            description: "A set-returning function over an aggregate is evaluated in a ProjectSet above the aggregate.",
            setup_sqls: &[],
            sql: "SELECT unnest(max([11, 12]))",
        },
        SqlTestCase {
            name: "srf_key_and_output_are_two_functions",
            description: "The same set-returning function as a key and in the SELECT list is evaluated twice, below and above the aggregate.",
            setup_sqls: T,
            sql: "SELECT unnest([1, 2]), count(*) FROM t GROUP BY unnest([1, 2])",
        },
        SqlTestCase {
            name: "where_alias_of_srf_key_is_above_project_set",
            description: "WHERE over the alias of a set-returning key is above the ProjectSet below the aggregate.",
            setup_sqls: T,
            sql: "SELECT unnest([a, a + 10]) AS s, count(*) FROM t WHERE s > 2 GROUP BY ROLLUP(s)",
        },
        SqlTestCase {
            name: "subquery_in_aggregate_argument_is_below_aggregate",
            description: "A subquery in an aggregate argument is evaluated below the aggregate.",
            setup_sqls: T_U,
            sql: "SELECT b FROM t GROUP BY b HAVING sum((SELECT max(n) FROM u)) > 0",
        },
        SqlTestCase {
            name: "correlated_subquery_in_aggregate_argument_is_below_aggregate",
            description: "A correlated subquery in an aggregate argument reads the input below the aggregate, not the key output.",
            setup_sqls: T,
            sql: "SELECT b, sum((SELECT t.a * 10)) FROM t GROUP BY ROLLUP(b)",
        },
        SqlTestCase {
            name: "duplicate_srf_is_one_project_set_function",
            description: "Repeated identical set-returning functions are one ProjectSet function.",
            setup_sqls: &[],
            sql: "SELECT unnest([1, 2, 3]), unnest([1, 2, 3])",
        },
        SqlTestCase {
            name: "duplicate_srf_over_aggregate_is_one_project_set_function",
            description: "Repeated set-returning functions over the same aggregate share its output symbol, so they are one ProjectSet function.",
            setup_sqls: T,
            sql: "SELECT unnest([sum(a)]), unnest([sum(a)]) + 1 FROM t",
        },
        SqlTestCase {
            name: "order_by_srf_of_select_list_is_its_output",
            description: "An ORDER BY set-returning function that is in the SELECT list sorts by the output of the same ProjectSet function.",
            setup_sqls: T,
            sql: "SELECT unnest([a, b]) AS u FROM t ORDER BY unnest([a, b])",
        },
        SqlTestCase {
            name: "order_by_srf_over_aggregate_is_its_output",
            description: "An ORDER BY set-returning function over an aggregate is the function in the ProjectSet above the aggregate.",
            setup_sqls: T,
            sql: "SELECT unnest([sum(a)]) FROM t ORDER BY unnest([sum(a)])",
        },
        SqlTestCase {
            name: "order_by_only_srf_is_below_where",
            description: "A set-returning function only in ORDER BY joins the ProjectSet of the SELECT list, below the WHERE that refers to an SRF alias.",
            setup_sqls: T,
            sql: "SELECT unnest([a]) AS u FROM t WHERE u > 1 ORDER BY unnest([b])",
        },
        SqlTestCase {
            name: "order_by_only_srf_in_grouped_select_is_above_aggregate",
            description: "A set-returning function only in the ORDER BY of a grouped SELECT is in the ProjectSet above the aggregate.",
            setup_sqls: T,
            sql: "SELECT b FROM t GROUP BY b ORDER BY unnest([b, 1])",
        },
        // Whether an expression reads a key output or is computed again.
        SqlTestCase {
            name: "ordinary_key_output_is_read_after_aggregate",
            description: "An expression that is an ordinary key reads the key output above the aggregate instead of computing it again.",
            setup_sqls: T,
            sql: "SELECT a + 1, sum(b) FROM t GROUP BY a + 1",
        },
        SqlTestCase {
            name: "ordinary_key_is_folded",
            description: "An ordinary key folds like any expression, here to a constant, while grouping-set keys keep their column structure.",
            setup_sqls: T,
            sql: "SELECT a IS NULL, count(*) FROM t GROUP BY a IS NULL",
        },
        SqlTestCase {
            name: "constant_grouping_set_key_is_read_as_its_output",
            description: "`1 + 1` reads the output of the grouping-set key `1 + 1`, which is NULL in the sets without it, and `coalesce(1 + 1, 100)` is an expression over that output, as the key is not folded. The literal `2` is a constant above the aggregate.",
            setup_sqls: T,
            sql: "SELECT 2, 1 + 1, coalesce(1 + 1, 100), count(*) FROM t GROUP BY ROLLUP(1 + 1)",
        },
        SqlTestCase {
            name: "dependent_key_is_pruned",
            description: "A key over another key, `a + 1` over `a`, is not a group item of its own and is computed above the aggregate.",
            setup_sqls: T,
            sql: "SELECT a, a + 1 AS k, sum(b) FROM t GROUP BY 1, 2",
        },
        SqlTestCase {
            name: "cast_over_tuple_access_key_allocates_no_extra_symbol",
            description: "Matching `s.2::INT` to the key `s.2` binds the tuple access once: the plan has one symbol for it and no other.",
            setup_sqls: T,
            sql: "SELECT s.2::INT, count(*) FROM (SELECT (a, b) AS s FROM t) d GROUP BY s.2",
        },
        SqlTestCase {
            name: "sql_udf_argument_outside_aggregate_reads_key_output",
            description: "The argument of a SQL UDF used outside the aggregate in its body reads the key output.",
            setup_sqls: T_AGG_UDF,
            sql: "SELECT agg_udf2(a, b) FROM t GROUP BY ROLLUP(b)",
        },
        SqlTestCase {
            name: "srf_key_by_alias_reads_key_output",
            description: "A set-returning function named by its alias as a key is evaluated once, below the aggregate, and read as the key output.",
            setup_sqls: T,
            sql: "SELECT unnest([a, a + 1]) AS s, count(*) FROM t GROUP BY ROLLUP(s)",
        },
        SqlTestCase {
            name: "in_list_key_stays_a_list",
            description: "An IN list that is a key is not rewritten to a join, so the SELECT item matches the key.",
            setup_sqls: T,
            sql: "SETTINGS (inlist_to_join_threshold = 2) SELECT a IN (1, 2, 3), count(*) FROM t GROUP BY ROLLUP(a IN (1, 2, 3))",
        },
        SqlTestCase {
            name: "in_list_join_over_key_reads_key_output",
            description: "An IN list over a key that is rewritten to a join reads the key output above the aggregate.",
            setup_sqls: T,
            sql: "SETTINGS (inlist_to_join_threshold = 2) SELECT a, a IN (1, 2, 3) FROM t GROUP BY ROLLUP(a)",
        },
        SqlTestCase {
            name: "script_udf_key_reads_key_output",
            description: "A script UDF call that is a key is read as the key output, not called again.",
            setup_sqls: T_SCRIPT_UDF,
            sql: "SELECT add_one(a), count(*) FROM t GROUP BY ROLLUP(add_one(a))",
        },
        SqlTestCase {
            name: "nondeterministic_key_reads_key_output",
            description: "A non-deterministic key is read as the key output, so the SELECT item has the value grouped by.",
            setup_sqls: T,
            sql: "SELECT rand(), count(*) FROM t GROUP BY rand()",
        },
        SqlTestCase {
            name: "async_function_key_reads_key_output",
            description: "An async function call that is a key is read as the key output.",
            setup_sqls: T,
            sql: "SELECT sleep(a), count(*) FROM t GROUP BY ROLLUP(sleep(a))",
        },
        SqlTestCase {
            name: "internal_column_key_reads_key_output",
            description: "An internal column that is a key is read as the key output.",
            setup_sqls: T,
            sql: "SELECT _row_id, count(*) FROM t GROUP BY ROLLUP(_row_id)",
        },
        SqlTestCase {
            name: "variant_cast_over_key_reads_key_output",
            description: "A cast over a variant path key is the cast of the key output.",
            setup_sqls: V,
            sql: "SELECT j['a']::INT, count(*) FROM v GROUP BY ROLLUP(j['a'])",
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

/// The SELECT list of a GROUPING SETS query is bound after the keys are known.
/// Before that, only the items GROUP BY, WHERE or key matching refer to are
/// bound, so a subquery elsewhere in the SELECT list is bound once.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grouping_sets_binds_select_subqueries_once() -> Result<()> {
    let setup_sqls: &[&str] = &[
        "CREATE TABLE t1(a Int32)",
        "CREATE TABLE t2(b Int32)",
        "CREATE TABLE t3(n Int32)",
    ];
    let cases = [
        (
            "correlated_select_subquery",
            "SELECT a, (SELECT max(n) FROM t3 WHERE n <= t1.a) FROM t1 GROUP BY ROLLUP(a)",
            2,
        ),
        // Binding every SELECT item twice would bind t3 twice and t2 four times.
        (
            "nested_grouping_sets",
            "SELECT a, (SELECT (SELECT max(b) FROM t2) FROM t3 GROUP BY ROLLUP(n) LIMIT 1) FROM t1 GROUP BY ROLLUP(a)",
            3,
        ),
        (
            "order_by_alias_of_subquery",
            "SELECT a, (SELECT max(n) FROM t3) AS m FROM t1 GROUP BY ROLLUP(a) ORDER BY m",
            2,
        ),
        (
            "having_alias_of_subquery",
            "SELECT a, (SELECT max(n) FROM t3) AS m FROM t1 GROUP BY ROLLUP(a) HAVING m > 0",
            2,
        ),
        // A key referenced by position is bound once, before the aggregate.
        (
            "subquery_key_by_position",
            "SELECT (SELECT max(n) FROM t3) AS m, a FROM t1 GROUP BY ROLLUP(1, a)",
            2,
        ),
    ];

    for (name, sql, tables) in cases {
        let case = SqlTestCase {
            name,
            description: "",
            setup_sqls,
            sql,
        };
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(case.sql).await?;
        let Plan::Query { metadata, .. } = plan else {
            panic!("expected query plan for {name}");
        };
        assert_eq!(metadata.read().tables().len(), tables, "{name}");
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
