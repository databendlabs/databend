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

//! When the effects of binding happen in a grouped SELECT.
//!
//! Binding an expression can have effects that depend on the phase it is
//! bound in: registering an aggregate function or a set-returning function,
//! binding a subquery, allocating a symbol. Some decisions also depend on the
//! whole statement, e.g. whether the SELECT aggregates. The scenarios are:
//!
//! - `fact_*`: a decision needs to know about aggregates of a later clause
//!   (HAVING, ORDER BY, QUALIFY); knowing they exist must not require
//!   registering them early.
//! - `grouped_*`: whether the SELECT is grouped is decided before its list is
//!   bound.
//! - `speculative_*`: expressions bound speculatively, to match grouping keys
//!   or as pre-aggregate forms of SELECT items, leave no phase-sensitive
//!   effect. The metadata line counts the tables and symbols of the plan, so
//!   a subquery bound twice or a leaked symbol shows up.
//! - `pre_aggregate_*`: effects of expressions evaluated before the aggregate
//!   happen below it.
//!
//! This file only holds the cases that no other binder test covers. The
//! covered ones, by scenario, in `semantic/binder.rs` unless noted:
//!
//! - fact: an aggregate only in HAVING or ORDER BY
//!   (`test_unified_grouping_binding`: `global_aggregate_only_in_having`,
//!   `global_aggregate_only_in_order`); an aggregate in QUALIFY
//!   (`binder_clauses.txt`); a window spec reusing an aggregate of HAVING or
//!   ORDER BY, or rejecting a new one (`test_binder_window_core_paths`);
//!   window arguments over aggregates with GROUP BY
//!   (`test_binder_named_window_paths`); aliases of aggregates in HAVING and
//!   ORDER BY (`binder/alias_resolution.rs` and its matrices); subqueries of
//!   HAVING and ORDER BY bound once
//!   (`test_clause_prepass_skips_subquery_metadata_side_effects`,
//!   `test_alias_resolution_prepass_metadata`).
//! - grouped: `grouping()` without grouping sets (`binder_grouping.txt`);
//!   GROUP BY ALL over aliases, UDAFs and a shadowing input column
//!   (`test_unified_grouping_binding`: `all_*`); a UDAF only in ORDER BY
//!   (`test_binder_window_core_paths`); an aggregate only in a subquery
//!   (`optimizer/decorrelate_scalar_aggregate.rs`).
//! - speculative: SELECT subqueries bound once under GROUPING SETS
//!   (`test_grouping_sets_binds_select_subqueries_once`); a set-returning
//!   function over a key (`test_binder_grouping_and_srf_paths`); a cast over
//!   a tuple access key (`test_unified_grouping_binding`).
//! - pre_aggregate: a set-returning function in aggregate arguments and an
//!   aggregate in a set-returning key (`test_unified_grouping_binding`); a
//!   subquery key by position (`test_grouping_sets_binds_select_subqueries_once`).

use std::io::Write;

use databend_common_exception::Result;
use databend_common_sql::plans::Plan;

use super::TEST_SCRIPT_UDF_SQL;
use super::TEST_UDAF_SQL;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::SqlTestOutcome;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;
use crate::framework::golden::write_case_outcome;

const SQL_UDF_WITH_AGGREGATE: &str = "CREATE OR REPLACE FUNCTION agg_udf AS (x) -> sum(x)";
const SQL_UDF_WITH_AGGREGATE_AND_KEY: &str =
    "CREATE OR REPLACE FUNCTION agg_udf2 AS (x, y) -> sum(x) + y";
const TYPED_SQL_UDF_WITH_AGGREGATE: &str =
    "CREATE OR REPLACE FUNCTION agg_typed (x INT) RETURNS BIGINT AS $$ sum(x) $$";
const NESTED_SQL_UDF_WITH_AGGREGATE: &str =
    "CREATE OR REPLACE FUNCTION agg_nested AS (x) -> agg_udf(x) + 1";
const SQL_UDF_WITH_AGGREGATE_AND_LAMBDA: &str =
    "CREATE OR REPLACE FUNCTION agg_lambda AS (x) -> sum(x) + array_transform([1], x -> x + 1)[1]";

// A UDF can't call itself directly, but two can call each other.
const MUTUALLY_RECURSIVE_SQL_UDFS: [&str; 2] = [
    "CREATE OR REPLACE FUNCTION rec_a AS (x) -> rec_b(x) + 1",
    "CREATE OR REPLACE FUNCTION rec_b AS (x) -> rec_a(x) + 1",
];

const SETUP: &[&str] = &[
    "CREATE TABLE t(a Int32 NOT NULL, b Int32 NOT NULL)",
    "CREATE TABLE u(n Int32)",
    "CREATE TABLE v(j Variant)",
    TEST_UDAF_SQL,
    TEST_SCRIPT_UDF_SQL,
    SQL_UDF_WITH_AGGREGATE,
    SQL_UDF_WITH_AGGREGATE_AND_KEY,
    TYPED_SQL_UDF_WITH_AGGREGATE,
    NESTED_SQL_UDF_WITH_AGGREGATE,
    SQL_UDF_WITH_AGGREGATE_AND_LAMBDA,
    MUTUALLY_RECURSIVE_SQL_UDFS[0],
    MUTUALLY_RECURSIVE_SQL_UDFS[1],
];

struct PhaseCase {
    name: &'static str,
    sql: &'static str,
}

const fn case(name: &'static str, sql: &'static str) -> PhaseCase {
    PhaseCase { name, sql }
}

const CASES: &[PhaseCase] = &[
    // Aggregates of later clauses.
    case(
        "fact_window_argument_aggregate_without_group_by",
        "SELECT sum(sum(a)) OVER () FROM t",
    ),
    case(
        "fact_window_argument_aggregate_with_ungrouped_column",
        "SELECT b, sum(sum(a)) OVER () FROM t",
    ),
    case(
        "fact_qualify_window_argument_reuses_aggregate",
        "SELECT b, sum(a) FROM t GROUP BY b QUALIFY sum(sum(a)) OVER () > 0",
    ),
    case(
        "fact_order_by_expression_over_aggregate_alias",
        "SELECT b, sum(a) AS s FROM t GROUP BY b ORDER BY s + 1",
    ),
    case(
        "fact_having_aggregate_over_subquery",
        "SELECT b FROM t GROUP BY b HAVING sum((SELECT max(n) FROM u)) > 0",
    ),
    case(
        "fact_aggregate_shared_by_select_and_having",
        "SELECT count(*) FROM t HAVING count(*) > 0",
    ),
    // Whether the SELECT is grouped.
    case(
        "grouped_aggregate_in_lambda_body",
        "SELECT array_transform([1, 2], x -> x + sum(a)) FROM t",
    ),
    // Results only in sqllogictests `query/subquery.test`.
    case(
        "grouped_outer_aggregate_in_subquery",
        "SELECT (SELECT sum(t.a) FROM u) FROM t",
    ),
    case("grouped_sql_udf_with_aggregate", "SELECT agg_udf(a) FROM t"),
    case(
        "grouped_sql_udf_with_aggregate_and_ungrouped_column",
        "SELECT b, agg_udf(a) FROM t",
    ),
    case(
        "grouped_udaf_with_window",
        "SELECT weighted_avg(a, b) OVER () FROM t",
    ),
    case(
        "grouped_sql_udf_with_window",
        "SELECT agg_udf(a) OVER () FROM t",
    ),
    case(
        "grouped_sql_udf_with_aggregate_and_key",
        "SELECT b, agg_udf(a) FROM t GROUP BY b",
    ),
    case(
        "grouped_sql_udf_arguments_inside_and_outside_aggregate",
        "SELECT agg_udf2(a, b) FROM t GROUP BY ROLLUP(b)",
    ),
    case(
        "grouped_sql_udf_argument_both_inside_and_outside_aggregate",
        "SELECT agg_udf2(a, a) FROM t GROUP BY b",
    ),
    case(
        "grouped_typed_sql_udf_with_aggregate",
        "SELECT agg_typed(a) FROM t",
    ),
    case(
        "grouped_nested_sql_udf_with_aggregate",
        "SELECT agg_nested(a) FROM t",
    ),
    case(
        "grouped_sql_udf_lambda_parameter_shadows_udf_parameter",
        "SELECT agg_lambda(a) FROM t",
    ),
    case("grouped_recursive_sql_udf", "SELECT rec_a(a) FROM t"),
    case(
        "grouped_all_sql_udf_with_aggregate",
        "SELECT a, agg_udf(b) FROM t GROUP BY ALL",
    ),
    // Speculative binding.
    // Results only in sqllogictests `query/grouping_sets/key_binding.test`.
    case(
        "speculative_srf_key_by_alias",
        "SELECT unnest([a, a + 1]) AS s, count(*) FROM t GROUP BY ROLLUP(s)",
    ),
    case(
        "speculative_in_list_join_as_key",
        "SETTINGS (inlist_to_join_threshold = 2) SELECT a IN (1, 2, 3), count(*) FROM t GROUP BY ROLLUP(a IN (1, 2, 3))",
    ),
    case(
        "speculative_in_list_over_key",
        "SELECT a, a IN (1, 2, 3) FROM t GROUP BY ROLLUP(a)",
    ),
    case(
        "speculative_in_list_join_over_key",
        "SETTINGS (inlist_to_join_threshold = 2) SELECT a, a IN (1, 2, 3) FROM t GROUP BY ROLLUP(a)",
    ),
    // Results only in sqllogictests `query/grouping_sets/key_binding.test`.
    case(
        "speculative_script_udf_key",
        "SELECT add_one(a), count(*) FROM t GROUP BY ROLLUP(add_one(a))",
    ),
    case(
        "speculative_internal_column_key",
        "SELECT _row_id, count(*) FROM t GROUP BY ROLLUP(_row_id)",
    ),
    case(
        "speculative_variant_cast_over_key",
        "SELECT j['a']::INT, count(*) FROM v GROUP BY ROLLUP(j['a'])",
    ),
    // Effects before the aggregate. Results of both only in sqllogictests
    // `query/grouping_sets/key_binding.test`.
    case(
        "pre_aggregate_where_alias_of_srf",
        "SELECT unnest([a, a + 10]) AS s, count(*) FROM t WHERE s > 2 GROUP BY ROLLUP(s)",
    ),
    case(
        "pre_aggregate_srf_key_and_output_with_one_name",
        "SELECT unnest([1, 2]), count(*) FROM t GROUP BY unnest([1, 2])",
    ),
    case(
        "pre_aggregate_srf_over_aggregate",
        "SELECT b, unnest([sum(a), b]) FROM t GROUP BY b",
    ),
    case(
        "pre_aggregate_correlated_subquery_in_aggregate_argument",
        "SELECT b, sum((SELECT t.a * 10)) FROM t GROUP BY ROLLUP(b)",
    ),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_binding_phases() -> Result<()> {
    let mut file = open_golden_file("semantic", "binder_binding_phases.txt")?;
    for phase_case in CASES {
        let case = SqlTestCase {
            name: phase_case.name,
            description: "",
            setup_sqls: SETUP,
            sql: phase_case.sql,
        };
        write_case_header(&mut file, &case)?;
        let ctx = setup_context(&case).await?;
        let outcome = match ctx.bind_sql(case.sql).await {
            Ok(plan) => {
                if let Plan::Query { metadata, .. } = &plan {
                    let metadata = metadata.read();
                    writeln!(
                        file,
                        "metadata: tables={}, columns={}",
                        metadata.tables().len(),
                        metadata.columns().len()
                    )?;
                }
                SqlTestOutcome::Plan(plan.format_indent(Default::default(), &ctx.stat_context()?)?)
            }
            Err(err) => SqlTestOutcome::Error {
                code: err.code(),
                message: err.message(),
            },
        };
        write_case_outcome(&mut file, &outcome)?;
    }
    Ok(())
}
