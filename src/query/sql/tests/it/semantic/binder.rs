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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::plans::Plan;

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

async fn run_binder_cases(file_name: &str, cases: &[SqlTestCase]) -> Result<()> {
    let mut file = open_golden_file("semantic", file_name)?;

    for case in cases {
        write_case_header(&mut file, case)?;
        let outcome = bind_case(case).await?;
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
            name: "order_by_expression_reuses_scalar_alias_semantics",
            description: "ORDER BY expressions should still inline scalar aliases from the select semantic view when they are used inside a larger expression.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number + 1 AS s FROM t ORDER BY s + 1",
        },
        SqlTestCase {
            name: "order_by_duplicate_aggregate_alias_is_ambiguous",
            description: "ORDER BY should keep duplicate aggregate aliases ambiguous instead of pre-expanding one candidate.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s, max(number) AS s FROM t ORDER BY s",
        },
        SqlTestCase {
            name: "order_by_expression_reuses_aggregate_alias_semantics",
            description: "ORDER BY expressions should keep aggregate aliases on the original semantic view instead of depending on rewritten select-item state.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s FROM t ORDER BY s + 1",
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
            name: "order_by_window_alias_does_not_seed_window_aggregate",
            description: "ORDER BY on a window alias must not pre-register aggregates that only appear inside that alias's window specification.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY sum(number)) AS rn FROM t ORDER BY rn",
        },
        SqlTestCase {
            name: "order_by_expression_reuses_window_alias_semantics",
            description: "ORDER BY expressions should keep window aliases on the original semantic view instead of depending on rewritten select-item state.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number, row_number() OVER (ORDER BY number) AS rn FROM t ORDER BY rn + 1",
        },
        SqlTestCase {
            name: "window_order_reuses_having_aggregate",
            description: "A window ORDER BY clause should be able to reuse an aggregate introduced later by HAVING.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER (ORDER BY sum(number)) FROM t HAVING sum(number) > 0",
        },
        SqlTestCase {
            name: "window_order_reuses_having_aggregate_alias",
            description: "A window ORDER BY clause should be able to reuse an aggregate reached through a HAVING alias reference.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s, row_number() OVER (ORDER BY s) FROM t HAVING s > 0",
        },
        SqlTestCase {
            name: "window_order_reuses_having_udaf",
            description: "A window ORDER BY clause should be able to reuse a UDAF introduced later by HAVING.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT row_number() OVER (ORDER BY weighted_avg(a, b)) FROM t HAVING weighted_avg(a, b) > 0",
        },
        SqlTestCase {
            name: "window_order_reuses_having_udaf_alias",
            description: "A window ORDER BY clause should be able to reuse a UDAF reached through a HAVING alias reference.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT weighted_avg(a, b) AS s, row_number() OVER (ORDER BY s) FROM t HAVING s > 0",
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
            name: "window_order_reuses_order_by_aggregate_alias",
            description: "A window ORDER BY clause should be able to reuse an aggregate reached through an ORDER BY alias reference.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s, row_number() OVER (ORDER BY s) FROM t ORDER BY s",
        },
        SqlTestCase {
            name: "window_order_reuses_order_by_udaf_alias",
            description: "A window ORDER BY clause should be able to reuse a UDAF reached through an ORDER BY alias reference.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)", TEST_UDAF_SQL],
            sql: "SELECT weighted_avg(a, b) AS s, row_number() OVER (ORDER BY s) FROM t ORDER BY s",
        },
        SqlTestCase {
            name: "window_order_rejects_window_alias_expansion",
            description: "A window ORDER BY clause must still reject aliases that expand to a prior window expression.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT row_number() OVER () AS rn, row_number() OVER (ORDER BY rn) FROM t",
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
            name: "within_group_window_aggregate_binds",
            description: "A WITHIN GROUP window aggregate should bind its sort descriptors without turning into a grouped aggregate.",
            setup_sqls: &["CREATE TABLE empsalary(depname String, empno UInt64, salary UInt64)"],
            sql: "SELECT listagg(cast(salary as varchar), '|') WITHIN GROUP (ORDER BY empno DESC) OVER (PARTITION BY depname ORDER BY empno) FROM empsalary",
        },
    ];

    run_binder_cases("binder_window_core.txt", &cases).await
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
async fn test_binder_qualify_paths() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "qualify_cte_then_outer_aggregate_from_sqllogictest_binds",
            description: "A sqllogictest pattern that filters with QUALIFY inside a CTE before an outer aggregate should still bind.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "WITH test AS (SELECT number % 10 AS id, number AS full_matched FROM t QUALIFY row_number() OVER (PARTITION BY id ORDER BY number DESC) = 1) SELECT full_matched, count() FROM test GROUP BY full_matched HAVING full_matched = 3",
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
    ];

    run_binder_cases("binder_qualify.txt", &cases).await
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
            name: "group_by_srf_alias_from_sqllogictest_binds",
            description: "A sqllogictest GROUP BY pattern that groups by an SRF select alias should bind successfully.",
            setup_sqls: &["CREATE TABLE t_str(col1 String, col2 String)"],
            sql: "SELECT t.col1 AS col1, unnest(split(t.col2, ',')) AS col3 FROM t_str AS t GROUP BY col1, col3 ORDER BY col3",
        },
        SqlTestCase {
            name: "group_by_prefers_input_column_over_same_name_srf_alias",
            description: "GROUP BY should keep a same-name input column ahead of an SRF alias, while still allowing non-conflicting SRF aliases.",
            setup_sqls: &["CREATE TABLE t_str(col1 String, col2 String)"],
            sql: "SELECT t.col1 AS col1, unnest(split(t.col2, ',')) AS col2 FROM t_str AS t GROUP BY col1, col2 ORDER BY col2",
        },
        SqlTestCase {
            name: "group_by_prefers_select_alias_over_same_name_base_column",
            description: "GROUP BY should keep resolving an unqualified name to the SELECT alias before the input column by default.",
            setup_sqls: &["CREATE TABLE t(a UInt64, b UInt64)"],
            sql: "SELECT a AS b, count(*) FROM t GROUP BY b",
        },
        SqlTestCase {
            name: "group_by_prefers_input_column_over_aggregate_alias",
            description: "GROUP BY should keep a same-name input column ahead of an aggregate alias, because the alias itself is not a valid grouping key.",
            setup_sqls: &[],
            sql: "SELECT count(*) AS x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) GROUP BY x",
        },
        SqlTestCase {
            name: "group_by_prefers_input_column_over_same_name_aggregate_alias_in_subquery",
            description: "GROUP BY inside a subquery should still resolve a same-name source column before an aggregate alias.",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT * FROM (SELECT sum(number) AS number FROM t GROUP BY number) AS s",
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
    ];

    run_binder_cases("binder_grouping.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binder_enable_group_by_column_first_setting() -> Result<()> {
    let case = SqlTestCase {
        name: "group_by_column_first_disabled_prefers_alias",
        description: "",
        setup_sqls: &["CREATE TABLE t(i UInt64, j UInt64)"],
        sql: "SELECT 1 AS i, sum(i) FROM t GROUP BY i",
    };
    let ctx = setup_context(&case).await?;
    ctx.get_settings()
        .set_setting("enable_group_by_column_first".to_string(), "0".to_string())?;
    let plan = ctx.bind_sql(case.sql).await?;
    let plan = plan.format_indent(Default::default())?;
    assert!(
        plan.contains("group items: [1 AS"),
        "GROUP BY should bind the SELECT alias when enable_group_by_column_first=0:\n{plan}"
    );

    let case = SqlTestCase {
        name: "enable_group_by_column_first_setting_prefers_column",
        description: "",
        setup_sqls: &["CREATE TABLE t(i UInt64, j UInt64)"],
        sql: "SELECT 1 AS i, sum(i) FROM t GROUP BY i",
    };
    let ctx = setup_context(&case).await?;
    ctx.get_settings()
        .set_setting("enable_group_by_column_first".to_string(), "1".to_string())?;
    let plan = ctx.bind_sql(case.sql).await?;
    let plan = plan.format_indent(Default::default())?;
    assert!(
        plan.contains("group items: [t.i (#0) AS (#0)]"),
        "GROUP BY should bind the input column when enable_group_by_column_first=1:\n{plan}"
    );

    let case = SqlTestCase {
        name: "group_by_column_first_enabled_keeps_alias_fallback",
        description: "",
        setup_sqls: &["CREATE TABLE t(i UInt64, j UInt64)"],
        sql: "SELECT i % 2 AS k, sum(i) FROM t GROUP BY k",
    };
    let ctx = setup_context(&case).await?;
    ctx.get_settings()
        .set_setting("enable_group_by_column_first".to_string(), "1".to_string())?;
    let plan = ctx.bind_sql(case.sql).await?;
    let plan = plan.format_indent(Default::default())?;
    assert!(
        plan.contains("group items: [modulo(t.i (#0), 2) AS"),
        "GROUP BY should still bind non-conflicting aliases when enable_group_by_column_first=1:\n{plan}"
    );

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum AliasUseSite {
    Select,
    Where,
    GroupBy,
    Having,
    Qualify,
    OrderBy,
    AggregateArgument,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum AliasKind {
    Scalar,
    Aggregate,
    Window,
    Srf,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum AliasReferenceShape {
    SimpleName,
    ComplexExpression,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum AliasNameRelation {
    ConflictsWithInputColumn,
    NoInputColumn,
}

type AliasMatrixDimension = (
    AliasUseSite,
    AliasKind,
    AliasReferenceShape,
    AliasNameRelation,
    bool,
);

#[derive(Clone, Copy, Debug)]
enum MatrixExpectation {
    PlanOk,
    PlanContains(&'static [&'static str]),
    ErrorContains(&'static str),
}

#[derive(Clone, Copy, Debug)]
struct AliasMatrixRun {
    enable_group_by_column_first: bool,
    sql: &'static str,
    expectation: MatrixExpectation,
}

#[derive(Debug)]
struct AliasMatrixCase {
    name: &'static str,
    setup_sqls: &'static [&'static str],
    runs: Vec<AliasMatrixRun>,
    covered_dimensions: Vec<AliasMatrixDimension>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_alias_resolution_matrix() -> Result<()> {
    let mut cases = Vec::<AliasMatrixCase>::new();
    for dimension in alias_matrix_dimensions() {
        let case = alias_matrix_case(dimension);
        assert!(
            case.covered_dimensions.contains(&dimension),
            "alias matrix case {} does not declare coverage for {:?}",
            case.name,
            dimension,
        );
        if !cases.iter().any(|existing| existing.name == case.name) {
            cases.push(case);
        }
    }

    for case in cases {
        let test_case = SqlTestCase {
            name: case.name,
            description: "",
            setup_sqls: case.setup_sqls,
            sql: case.runs[0].sql,
        };
        let ctx = setup_context(&test_case).await?;
        for run in case.runs {
            ctx.get_settings().set_setting(
                "enable_group_by_column_first".to_string(),
                if run.enable_group_by_column_first {
                    "1"
                } else {
                    "0"
                }
                .to_string(),
            )?;

            let result = ctx.bind_sql(run.sql).await;
            match (&run.expectation, result) {
                (MatrixExpectation::PlanOk, Ok(_)) => {}
                (MatrixExpectation::PlanContains(expected), Ok(plan)) => {
                    let plan = plan.format_indent(Default::default())?;
                    for fragment in *expected {
                        assert!(
                            plan.contains(fragment),
                            "matrix case {} expected plan fragment `{fragment}`:\n{plan}",
                            case.name,
                        );
                    }
                }
                (MatrixExpectation::ErrorContains(expected), Err(err)) => {
                    assert!(
                        err.message().contains(expected),
                        "matrix case {} expected error fragment `{expected}`, got: {}",
                        case.name,
                        err.message(),
                    );
                }
                (MatrixExpectation::PlanOk | MatrixExpectation::PlanContains(_), Err(err)) => {
                    panic!(
                        "matrix case {} expected plan, got error: {err:?}",
                        case.name
                    );
                }
                (MatrixExpectation::ErrorContains(expected), Ok(plan)) => {
                    let plan = plan.format_indent(Default::default())?;
                    panic!(
                        "matrix case {} expected error fragment `{expected}`, got plan:\n{plan}",
                        case.name
                    );
                }
            }
        }
    }

    Ok(())
}

fn alias_matrix_dimensions() -> [AliasMatrixDimension; 28] {
    [
        alias_matrix_dimension(
            AliasUseSite::Select,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::Select,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::Where,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::Where,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::Where,
            AliasKind::Srf,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::Having,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::Qualify,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::Qualify,
            AliasKind::Window,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::Qualify,
            AliasKind::Srf,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::OrderBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::OrderBy,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::OrderBy,
            AliasKind::Window,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::AggregateArgument,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::AggregateArgument,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            true,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            true,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn,
            true,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            true,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Srf,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ),
        alias_matrix_dimension(
            AliasUseSite::GroupBy,
            AliasKind::Srf,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            false,
        ),
    ]
}

const fn alias_matrix_dimension(
    use_site: AliasUseSite,
    alias_kind: AliasKind,
    reference_shape: AliasReferenceShape,
    name_relation: AliasNameRelation,
    enable_group_by_column_first: bool,
) -> AliasMatrixDimension {
    match (
        use_site,
        alias_kind,
        reference_shape,
        name_relation,
        enable_group_by_column_first,
    ) {
        (
            AliasUseSite::Select,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn | AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::Where,
            AliasKind::Scalar | AliasKind::Aggregate | AliasKind::Srf,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::Having,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::Qualify,
            AliasKind::Aggregate | AliasKind::Window | AliasKind::Srf,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::OrderBy,
            AliasKind::Scalar | AliasKind::Aggregate | AliasKind::Window,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::AggregateArgument,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn | AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName | AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn | AliasNameRelation::NoInputColumn,
            false | true,
        )
        | (
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::SimpleName | AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn | AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::GroupBy,
            AliasKind::Srf,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn | AliasNameRelation::NoInputColumn,
            false,
        ) => (
            use_site,
            alias_kind,
            reference_shape,
            name_relation,
            enable_group_by_column_first,
        ),
        _ => panic!("unsupported alias matrix dimension"),
    }
}

fn alias_matrix_case(dimension: AliasMatrixDimension) -> AliasMatrixCase {
    match dimension {
        (
            AliasUseSite::Select,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "select_scalar_alias_conflict_prefers_input_column",
            setup_sqls: &["CREATE TABLE t(i Int32, x Int32)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT i AS x, x + 1 FROM t",
                expectation: MatrixExpectation::PlanContains(&["plus(t.x (#1), 1)"]),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::Select,
                AliasKind::Scalar,
                AliasReferenceShape::ComplexExpression,
                AliasNameRelation::ConflictsWithInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::Select,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::OrderBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "select_and_order_by_scalar_alias_in_expression",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT number + 1 AS s, s + 1 AS next FROM t ORDER BY s + 1",
                expectation: MatrixExpectation::PlanOk,
            }],
            covered_dimensions: vec![
                alias_matrix_dimension(
                    AliasUseSite::Select,
                    AliasKind::Scalar,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
                alias_matrix_dimension(
                    AliasUseSite::OrderBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
            ],
        },
        (
            AliasUseSite::Where,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "where_scalar_alias_in_expression",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT number + 1 AS s FROM t WHERE s > 1",
                expectation: MatrixExpectation::PlanOk,
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::Where,
                AliasKind::Scalar,
                AliasReferenceShape::ComplexExpression,
                AliasNameRelation::NoInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::Where,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "where_rejects_aggregate_alias",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT sum(number) AS s FROM t WHERE s > 0",
                expectation: MatrixExpectation::ErrorContains(
                    "Where clause can't contain aggregate or window functions",
                ),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::Where,
                AliasKind::Aggregate,
                AliasReferenceShape::ComplexExpression,
                AliasNameRelation::NoInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::Where,
            AliasKind::Srf,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::Qualify,
            AliasKind::Srf,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "where_and_qualify_srf_alias_in_expression",
            setup_sqls: &[],
            runs: vec![
                AliasMatrixRun {
                    enable_group_by_column_first: false,
                    sql: "SELECT unnest([1, 2, 3]) AS u WHERE u = 1",
                    expectation: MatrixExpectation::PlanOk,
                },
                AliasMatrixRun {
                    enable_group_by_column_first: false,
                    sql: "SELECT unnest([1, 2, 3]) AS u QUALIFY u = 1",
                    expectation: MatrixExpectation::PlanOk,
                },
            ],
            covered_dimensions: vec![
                alias_matrix_dimension(
                    AliasUseSite::Where,
                    AliasKind::Srf,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
                alias_matrix_dimension(
                    AliasUseSite::Qualify,
                    AliasKind::Srf,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
            ],
        },
        (
            AliasUseSite::Having,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::OrderBy,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "having_and_order_by_aggregate_alias_in_expression",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT sum(number) AS s FROM t HAVING s > 0 ORDER BY s + 1",
                expectation: MatrixExpectation::PlanOk,
            }],
            covered_dimensions: vec![
                alias_matrix_dimension(
                    AliasUseSite::Having,
                    AliasKind::Aggregate,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
                alias_matrix_dimension(
                    AliasUseSite::OrderBy,
                    AliasKind::Aggregate,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
            ],
        },
        (
            AliasUseSite::Qualify,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "qualify_rejects_aggregate_alias",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT sum(number) AS s FROM t QUALIFY s > 0",
                expectation: MatrixExpectation::ErrorContains(
                    "Qualify clause must not contain aggregate functions",
                ),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::Qualify,
                AliasKind::Aggregate,
                AliasReferenceShape::ComplexExpression,
                AliasNameRelation::NoInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::Qualify,
            AliasKind::Window,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::OrderBy,
            AliasKind::Window,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "qualify_and_order_by_window_alias_in_expression",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT number, row_number() OVER (ORDER BY number) AS rn FROM t QUALIFY rn = 1 ORDER BY rn + 1",
                expectation: MatrixExpectation::PlanOk,
            }],
            covered_dimensions: vec![
                alias_matrix_dimension(
                    AliasUseSite::Qualify,
                    AliasKind::Window,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
                alias_matrix_dimension(
                    AliasUseSite::OrderBy,
                    AliasKind::Window,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
            ],
        },
        (
            AliasUseSite::AggregateArgument,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "aggregate_argument_prefers_base_column_over_select_alias",
            setup_sqls: &["CREATE TABLE t(a UInt64, c2 UInt64)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT a AS c2, sum(c2) FROM t GROUP BY a",
                expectation: MatrixExpectation::PlanContains(&["sum(t.c2 (#1))"]),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::AggregateArgument,
                AliasKind::Scalar,
                AliasReferenceShape::SimpleName,
                AliasNameRelation::ConflictsWithInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::AggregateArgument,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "aggregate_argument_falls_back_to_select_alias",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT number % 3 AS c1, sum(c1) FROM t GROUP BY number % 3",
                expectation: MatrixExpectation::PlanContains(&["sum(number % 3 (#1))"]),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::AggregateArgument,
                AliasKind::Scalar,
                AliasReferenceShape::SimpleName,
                AliasNameRelation::NoInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        )
        | (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            true,
        ) => AliasMatrixCase {
            name: "group_by_scalar_simple_conflict_setting_switch",
            setup_sqls: &["CREATE TABLE t(i Int32, x Int32)"],
            runs: vec![
                AliasMatrixRun {
                    enable_group_by_column_first: false,
                    sql: "SELECT i AS x, count(*) FROM t GROUP BY x",
                    expectation: MatrixExpectation::PlanContains(&[
                        "group items: [t.i (#0) AS (#0)]",
                    ]),
                },
                AliasMatrixRun {
                    enable_group_by_column_first: true,
                    sql: "SELECT i AS x, count(*) FROM t GROUP BY x",
                    expectation: MatrixExpectation::ErrorContains(
                        "must appear in the GROUP BY clause or be used in an aggregate function",
                    ),
                },
            ],
            covered_dimensions: vec![
                alias_matrix_dimension(
                    AliasUseSite::GroupBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::SimpleName,
                    AliasNameRelation::ConflictsWithInputColumn,
                    false,
                ),
                alias_matrix_dimension(
                    AliasUseSite::GroupBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::SimpleName,
                    AliasNameRelation::ConflictsWithInputColumn,
                    true,
                ),
            ],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            true,
        ) => AliasMatrixCase {
            name: "group_by_scalar_simple_no_conflict_falls_back_to_alias",
            setup_sqls: &["CREATE TABLE t(i Int32, x Int32)"],
            runs: vec![
                AliasMatrixRun {
                    enable_group_by_column_first: false,
                    sql: "SELECT i AS k, count(*) FROM t GROUP BY k",
                    expectation: MatrixExpectation::PlanContains(&[
                        "group items: [t.i (#0) AS (#0)]",
                    ]),
                },
                AliasMatrixRun {
                    enable_group_by_column_first: true,
                    sql: "SELECT i AS k, count(*) FROM t GROUP BY k",
                    expectation: MatrixExpectation::PlanContains(&[
                        "group items: [t.i (#0) AS (#0)]",
                    ]),
                },
            ],
            covered_dimensions: vec![
                alias_matrix_dimension(
                    AliasUseSite::GroupBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::SimpleName,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
                alias_matrix_dimension(
                    AliasUseSite::GroupBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::SimpleName,
                    AliasNameRelation::NoInputColumn,
                    true,
                ),
            ],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        )
        | (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn,
            true,
        ) => AliasMatrixCase {
            name: "group_by_scalar_complex_conflict_prefers_input_column",
            setup_sqls: &["CREATE TABLE t(type Int32)"],
            runs: vec![
                AliasMatrixRun {
                    enable_group_by_column_first: false,
                    sql: "SELECT CASE WHEN type = 0 THEN 'A' ELSE 'B' END AS type, count(*) FROM t GROUP BY CASE WHEN type = 0 THEN 'A' ELSE 'B' END",
                    expectation: MatrixExpectation::PlanContains(&[
                        "group items: [if(eq(t.type (#0), 0), 'A', 'B') AS",
                    ]),
                },
                AliasMatrixRun {
                    enable_group_by_column_first: true,
                    sql: "SELECT CASE WHEN type = 0 THEN 'A' ELSE 'B' END AS type, count(*) FROM t GROUP BY CASE WHEN type = 0 THEN 'A' ELSE 'B' END",
                    expectation: MatrixExpectation::PlanContains(&[
                        "group items: [if(eq(t.type (#0), 0), 'A', 'B') AS",
                    ]),
                },
            ],
            covered_dimensions: vec![
                alias_matrix_dimension(
                    AliasUseSite::GroupBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::ConflictsWithInputColumn,
                    false,
                ),
                alias_matrix_dimension(
                    AliasUseSite::GroupBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::ConflictsWithInputColumn,
                    true,
                ),
            ],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        )
        | (
            AliasUseSite::GroupBy,
            AliasKind::Scalar,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            true,
        ) => AliasMatrixCase {
            name: "group_by_scalar_complex_uses_prior_simple_alias_group_item",
            setup_sqls: &["CREATE TABLE t(i Int32)"],
            runs: vec![
                AliasMatrixRun {
                    enable_group_by_column_first: false,
                    sql: "SELECT i AS k, abs(i) AS ak, count(*) FROM t GROUP BY k, abs(k)",
                    expectation: MatrixExpectation::PlanContains(&[
                        "group items: [t.i (#0) AS (#0)]",
                        "abs(t.i (#0)) AS (#3)",
                    ]),
                },
                AliasMatrixRun {
                    enable_group_by_column_first: true,
                    sql: "SELECT i AS k, abs(i) AS ak, count(*) FROM t GROUP BY k, abs(k)",
                    expectation: MatrixExpectation::PlanContains(&[
                        "group items: [t.i (#0) AS (#0)]",
                        "abs(t.i (#0)) AS (#3)",
                    ]),
                },
                AliasMatrixRun {
                    enable_group_by_column_first: false,
                    sql: "SELECT i AS k, abs(i) AS ak, count(*) FROM t GROUP BY abs(k)",
                    expectation: MatrixExpectation::ErrorContains("column k doesn't exist"),
                },
                AliasMatrixRun {
                    enable_group_by_column_first: true,
                    sql: "SELECT i AS k, abs(i) AS ak, count(*) FROM t GROUP BY abs(k)",
                    expectation: MatrixExpectation::ErrorContains("column k doesn't exist"),
                },
            ],
            covered_dimensions: vec![
                alias_matrix_dimension(
                    AliasUseSite::GroupBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    false,
                ),
                alias_matrix_dimension(
                    AliasUseSite::GroupBy,
                    AliasKind::Scalar,
                    AliasReferenceShape::ComplexExpression,
                    AliasNameRelation::NoInputColumn,
                    true,
                ),
            ],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "group_by_aggregate_simple_conflict_prefers_input_column",
            setup_sqls: &[],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT count(*) AS x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) GROUP BY x",
                expectation: MatrixExpectation::PlanContains(&["group items: [x (#2) AS (#2)]"]),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::GroupBy,
                AliasKind::Aggregate,
                AliasReferenceShape::SimpleName,
                AliasNameRelation::ConflictsWithInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "group_by_aggregate_simple_no_conflict_rejects_alias_group_item",
            setup_sqls: &["CREATE TABLE t(i Int32)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT count(*) AS c FROM t GROUP BY c",
                expectation: MatrixExpectation::ErrorContains(
                    "GROUP BY items can't contain aggregate functions",
                ),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::GroupBy,
                AliasKind::Aggregate,
                AliasReferenceShape::SimpleName,
                AliasNameRelation::NoInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "group_by_aggregate_complex_conflict_prefers_input_column",
            setup_sqls: &[],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT count(*) AS x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) GROUP BY x + 1",
                expectation: MatrixExpectation::PlanContains(&["group items: [plus(x (#2), 1) AS"]),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::GroupBy,
                AliasKind::Aggregate,
                AliasReferenceShape::ComplexExpression,
                AliasNameRelation::ConflictsWithInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Aggregate,
            AliasReferenceShape::ComplexExpression,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "group_by_aggregate_complex_no_conflict_rejects_alias_group_item",
            setup_sqls: &["CREATE TABLE t(i Int32)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT count(*) AS c FROM t GROUP BY c + 1",
                expectation: MatrixExpectation::ErrorContains("column c doesn't exist"),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::GroupBy,
                AliasKind::Aggregate,
                AliasReferenceShape::ComplexExpression,
                AliasNameRelation::NoInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Srf,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::ConflictsWithInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "group_by_srf_simple_conflict_prefers_input_column",
            setup_sqls: &["CREATE TABLE t_str(col1 String, col2 String)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT t.col1 AS col1, unnest(split(t.col2, ',')) AS col2 FROM t_str AS t GROUP BY col1, col2 ORDER BY col2",
                expectation: MatrixExpectation::PlanContains(&[
                    "group items: [t_str.col1 (#0) AS (#0), t_str.col2 (#1) AS (#1)]",
                ]),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::GroupBy,
                AliasKind::Srf,
                AliasReferenceShape::SimpleName,
                AliasNameRelation::ConflictsWithInputColumn,
                false,
            )],
        },
        (
            AliasUseSite::GroupBy,
            AliasKind::Srf,
            AliasReferenceShape::SimpleName,
            AliasNameRelation::NoInputColumn,
            false,
        ) => AliasMatrixCase {
            name: "group_by_srf_simple_no_conflict_falls_back_to_alias",
            setup_sqls: &["CREATE TABLE t_str(col1 String, col2 String)"],
            runs: vec![AliasMatrixRun {
                enable_group_by_column_first: false,
                sql: "SELECT t.col1 AS col1, unnest(split(t.col2, ',')) AS col3 FROM t_str AS t GROUP BY col1, col3 ORDER BY col3",
                expectation: MatrixExpectation::PlanContains(&[
                    "get(unnest(split(t.col2 (#1), ',')) (#2)) AS (#3)",
                ]),
            }],
            covered_dimensions: vec![alias_matrix_dimension(
                AliasUseSite::GroupBy,
                AliasKind::Srf,
                AliasReferenceShape::SimpleName,
                AliasNameRelation::NoInputColumn,
                false,
            )],
        },
        _ => panic!("unmapped alias matrix dimension: {dimension:?}"),
    }
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
            name: "having_alias_and_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT sum(number) AS s FROM t HAVING s > 0 AND EXISTS (SELECT 1 FROM t AS inner_t WHERE inner_t.number > 0)",
        },
        SqlTestCase {
            name: "having_alias_to_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT (SELECT max(number) FROM t AS inner_t) AS s FROM t HAVING s > 0",
        },
        SqlTestCase {
            name: "order_by_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT number FROM t ORDER BY (SELECT max(number) FROM t AS inner_t)",
        },
        SqlTestCase {
            name: "order_by_alias_to_subquery_prepass_metadata",
            description: "",
            setup_sqls: &["CREATE TABLE t(number UInt64)"],
            sql: "SELECT (SELECT max(number) FROM t AS inner_t) AS s FROM t ORDER BY s",
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
