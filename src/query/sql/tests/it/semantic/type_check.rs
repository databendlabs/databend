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

use std::sync::Arc;

use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Metadata;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::Symbol;
use databend_common_sql::TypeChecker;
use databend_common_sql::Visibility;
use databend_common_sql::format_scalar;
use parking_lot::RwLock;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::SqlTestOutcome;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;
use crate::framework::golden::write_case_outcome;

fn add_test_column(bind_context: &mut BindContext, index: usize, name: &str, data_type: DataType) {
    bind_context.add_column_binding(
        ColumnBindingBuilder::new(
            name.to_string(),
            Symbol::new(index),
            Box::new(data_type.wrap_nullable()),
            Visibility::Visible,
        )
        .build(),
    );
}

async fn type_check_case(case: &SqlTestCase) -> Result<SqlTestOutcome> {
    let ctx = setup_context(case).await?;
    let tokens = tokenize_sql(case.sql)?;
    let dialect = ctx.get_settings().get_sql_dialect()?;
    let expr = parse_expr(&tokens, dialect)?;

    let settings = ctx.get_settings();
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut bind_context = BindContext::new();
    add_test_column(
        &mut bind_context,
        0,
        "number",
        DataType::Number(NumberDataType::Int64),
    );
    add_test_column(&mut bind_context, 1, "text", DataType::String);
    add_test_column(&mut bind_context, 2, "pattern", DataType::String);
    add_test_column(
        &mut bind_context,
        3,
        "delta",
        DataType::Number(NumberDataType::Int32),
    );
    add_test_column(&mut bind_context, 4, "flag", DataType::Boolean);
    add_test_column(&mut bind_context, 5, "ts", DataType::Timestamp);
    add_test_column(&mut bind_context, 6, "date", DataType::Date);
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let mut type_checker = TypeChecker::try_create(
        &mut bind_context,
        ctx,
        &name_resolution_ctx,
        metadata,
        &[],
        false,
    )?;

    let outcome = match type_checker.resolve(&expr).map(|resolved| *resolved) {
        Ok((scalar, data_type)) => SqlTestOutcome::Plan(format!(
            "scalar: {}\ntype: {}",
            format_scalar(&scalar),
            data_type
        )),
        Err(err) => SqlTestOutcome::Error {
            code: err.code(),
            message: err.message(),
        },
    };
    Ok(outcome)
}

async fn run_type_check_cases(file_name: &str, cases: &[SqlTestCase]) -> Result<()> {
    let mut file = open_golden_file("semantic", file_name)?;

    for case in cases {
        write_case_header(&mut file, case)?;
        let outcome = type_check_case(case).await?;
        write_case_outcome(&mut file, &outcome)?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_core_lowering() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "minus_literal_is_resolved_during_lowering",
            description: "Unary minus on a literal should become a negative literal before scalar function resolution.",
            setup_sqls: &[],
            sql: "-1",
        },
        SqlTestCase {
            name: "minus_non_literal_remains_scalar_operator",
            description: "Unary minus on a non-literal should still resolve through the scalar operator path.",
            setup_sqls: &[],
            sql: "-number",
        },
        SqlTestCase {
            name: "is_null_wrapper_lowers_as_boolean_expression",
            description: "IS NULL carries its own wrapper semantics and should not collapse to the child expression.",
            setup_sqls: &[],
            sql: "number IS NULL",
        },
        SqlTestCase {
            name: "between_expression_lowers_to_comparison_core",
            description: "BETWEEN should type check after lowering into comparison expressions.",
            setup_sqls: &[],
            sql: "number BETWEEN 1 AND 5",
        },
        SqlTestCase {
            name: "aggregate_function_uses_aggregate_core_path",
            description: "A plain aggregate call should be separated from scalar calls during CoreExpr lowering.",
            setup_sqls: &[],
            sql: "sum(number)",
        },
        SqlTestCase {
            name: "aggregate_window_function_uses_window_core_path",
            description: "An aggregate with OVER should resolve as a window expression, not as a grouped aggregate.",
            setup_sqls: &[],
            sql: "sum(number) OVER ()",
        },
        SqlTestCase {
            name: "general_window_function_uses_window_core_path",
            description: "A general window function with OVER should resolve through the window CoreExpr branch.",
            setup_sqls: &[],
            sql: "row_number() OVER ()",
        },
        SqlTestCase {
            name: "general_window_function_without_over_errors_early",
            description: "A general window function without OVER should fail from the CoreExpr window split.",
            setup_sqls: &[],
            sql: "row_number()",
        },
    ];

    run_type_check_cases("type_check_core_lowering.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_scalar_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "not_between_rewrites_to_or_comparisons",
            description: "A NOT BETWEEN predicate from range-join tests should type check as the explicit disjunction.",
            setup_sqls: &[],
            sql: "number NOT BETWEEN 1 AND 5",
        },
        SqlTestCase {
            name: "is_distinct_from_rewrites_null_safe_comparison",
            description: "IS DISTINCT FROM appears in join tests and should lower through the null-aware comparison rewrite.",
            setup_sqls: &[],
            sql: "number IS DISTINCT FROM delta",
        },
        SqlTestCase {
            name: "is_not_distinct_from_rewrites_null_safe_comparison",
            description: "IS NOT DISTINCT FROM should share the same null-aware comparison path with the inverted result.",
            setup_sqls: &[],
            sql: "number IS NOT DISTINCT FROM delta",
        },
        SqlTestCase {
            name: "searched_case_from_sqllogictest_binds",
            description: "A searched CASE expression from sqllogictest patterns should rewrite into the IF function shape.",
            setup_sqls: &[],
            sql: "CASE WHEN number > 1 THEN text WHEN number < 0 THEN pattern ELSE 'fallback' END",
        },
        SqlTestCase {
            name: "simple_case_compares_operand_to_each_condition",
            description: "A simple CASE expression should preserve the operand-comparison rewrite.",
            setup_sqls: &[],
            sql: "CASE number WHEN 1 THEN text WHEN 2 THEN pattern ELSE 'fallback' END",
        },
        SqlTestCase {
            name: "small_in_list_rewrites_to_balanced_or",
            description: "A small IN list should stay in scalar type checking rather than using the subquery conversion path.",
            setup_sqls: &[],
            sql: "number IN (1, 2, delta)",
        },
        SqlTestCase {
            name: "not_in_list_wraps_rewritten_predicate",
            description: "NOT IN should wrap the scalar IN-list rewrite with a NOT function.",
            setup_sqls: &[],
            sql: "number NOT IN (1, 2, delta)",
        },
    ];

    run_type_check_cases("type_check_scalar_rewrites.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_literals_and_collections() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "float_literal_from_parser_tests_binds",
            description: "A float literal shape from parser tests should resolve as a typed constant.",
            setup_sqls: &[],
            sql: "1.925e-3",
        },
        SqlTestCase {
            name: "binary_literal_from_parser_tests_binds",
            description: "A binary literal shape from parser tests should resolve as a typed constant.",
            setup_sqls: &[],
            sql: "x'deedbeef'",
        },
        SqlTestCase {
            name: "null_literal_binds",
            description: "A NULL literal should stay on the literal type-check path.",
            setup_sqls: &[],
            sql: "NULL",
        },
        SqlTestCase {
            name: "constant_array_from_comparison_tests_binds",
            description: "A constant array from scalar comparison tests should use the constant-array fast path.",
            setup_sqls: &[],
            sql: "[1, 2, 3]",
        },
        SqlTestCase {
            name: "mixed_array_with_column_binds",
            description: "An array with a column element should fall back to the scalar array function path.",
            setup_sqls: &[],
            sql: "[1, delta, NULL]",
        },
        SqlTestCase {
            name: "map_literal_from_sqllogictest_binds",
            description: "A map literal shape from map sqllogictests should resolve through map construction.",
            setup_sqls: &[],
            sql: "{'k1': 1, 'k2': delta}",
        },
        SqlTestCase {
            name: "tuple_literal_from_parser_tests_binds",
            description: "A tuple expression from parser tests should resolve through tuple construction.",
            setup_sqls: &[],
            sql: "(number, text, true)",
        },
        SqlTestCase {
            name: "array_index_access_binds",
            description: "Array index access should preserve the existing get-function rewrite and nullable result type.",
            setup_sqls: &[],
            sql: "[10, 20, 30][1]",
        },
        SqlTestCase {
            name: "map_key_access_binds",
            description: "Map key access should preserve the existing get-function rewrite.",
            setup_sqls: &[],
            sql: "{'k1': 1, 'k2': delta}['k1']",
        },
        SqlTestCase {
            name: "variant_colon_access_binds",
            description: "Variant colon access should preserve the get_by_keypath rewrite.",
            setup_sqls: &[],
            sql: "to_variant({'k1': 1}):k1",
        },
    ];

    run_type_check_cases("type_check_literals_collections.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_sugar_function_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "current_catalog_rewrites_to_literal",
            description: "A context sugar function should rewrite to a literal without needing full binder planning.",
            setup_sqls: &[],
            sql: "current_catalog()",
        },
        SqlTestCase {
            name: "current_database_rewrites_to_literal",
            description: "current_database() should use the TypeChecker sugar rewrite.",
            setup_sqls: &[],
            sql: "current_database()",
        },
        SqlTestCase {
            name: "version_rewrites_to_literal",
            description: "version() should use the TypeChecker sugar rewrite.",
            setup_sqls: &[],
            sql: "version()",
        },
        SqlTestCase {
            name: "current_user_rewrites_to_literal",
            description: "current_user() should use the TypeChecker sugar rewrite.",
            setup_sqls: &[],
            sql: "current_user()",
        },
        SqlTestCase {
            name: "current_role_rewrites_to_literal",
            description: "current_role() should use the TypeChecker sugar rewrite.",
            setup_sqls: &[],
            sql: "current_role()",
        },
        SqlTestCase {
            name: "timezone_rewrites_to_literal",
            description: "timezone() should read settings through the existing LiteTableContext.",
            setup_sqls: &[],
            sql: "timezone()",
        },
        SqlTestCase {
            name: "connection_id_rewrites_to_literal",
            description: "connection_id() should rewrite from context state.",
            setup_sqls: &[],
            sql: "connection_id()",
        },
        SqlTestCase {
            name: "nullif_rewrites_to_if",
            description: "nullif(x, y) should rewrite to the IF expression shape.",
            setup_sqls: &[],
            sql: "nullif(number, delta)",
        },
        SqlTestCase {
            name: "equal_null_rewrites_to_null_safe_if",
            description: "equal_null(x, y) should use the explicit null-safe rewrite.",
            setup_sqls: &[],
            sql: "equal_null(number, delta)",
        },
        SqlTestCase {
            name: "iff_alias_rewrites_to_if",
            description: "iff(cond, x, y) should type check through the IF function.",
            setup_sqls: &[],
            sql: "iff(flag, text, pattern)",
        },
        SqlTestCase {
            name: "ifnull_rewrites_to_if",
            description: "ifnull(x, y) should rewrite through IS NULL and IF.",
            setup_sqls: &[],
            sql: "ifnull(text, pattern)",
        },
        SqlTestCase {
            name: "nvl_alias_rewrites_to_if",
            description: "nvl(x, y) should share the ifnull rewrite.",
            setup_sqls: &[],
            sql: "nvl(text, pattern)",
        },
        SqlTestCase {
            name: "nvl2_rewrites_to_if",
            description: "nvl2(x, y, z) should rewrite through IS NOT NULL and IF.",
            setup_sqls: &[],
            sql: "nvl2(text, pattern, 'fallback')",
        },
        SqlTestCase {
            name: "coalesce_rewrites_to_if_chain",
            description: "coalesce should remove literal NULLs and rewrite to the IF chain.",
            setup_sqls: &[],
            sql: "coalesce(NULL, text, pattern)",
        },
        SqlTestCase {
            name: "decode_rewrites_to_null_safe_if_chain",
            description: "decode should rewrite to a null-safe IF chain.",
            setup_sqls: &[],
            sql: "decode(number, 1, text, 2, pattern, 'fallback')",
        },
        SqlTestCase {
            name: "array_sort_with_constant_options_rewrites",
            description: "array_sort with constant order options should select the concrete sort function.",
            setup_sqls: &[],
            sql: "array_sort([3, 1, 2], 'desc', 'nulls last')",
        },
        SqlTestCase {
            name: "array_aggregate_with_constant_function_rewrites",
            description: "array_aggregate should rewrite to the selected array aggregate function.",
            setup_sqls: &[],
            sql: "array_aggregate([1, 2, 3], 'sum')",
        },
        SqlTestCase {
            name: "to_variant_rewrites_to_variant_cast",
            description: "to_variant should use the variant cast rewrite.",
            setup_sqls: &[],
            sql: "to_variant({'k1': 1})",
        },
        SqlTestCase {
            name: "try_to_variant_rewrites_to_variant_cast",
            description: "try_to_variant should use the fallible variant cast rewrite.",
            setup_sqls: &[],
            sql: "try_to_variant([number, delta])",
        },
        SqlTestCase {
            name: "greatest_rewrites_through_array_max",
            description: "greatest should rewrite through an array and null-aware max.",
            setup_sqls: &[],
            sql: "greatest(number, delta, 1)",
        },
        SqlTestCase {
            name: "least_rewrites_through_array_min",
            description: "least should rewrite through an array and null-aware min.",
            setup_sqls: &[],
            sql: "least(number, delta, 1)",
        },
        SqlTestCase {
            name: "greatest_ignore_nulls_rewrites_through_array_max",
            description: "greatest_ignore_nulls should rewrite directly to array_max.",
            setup_sqls: &[],
            sql: "greatest_ignore_nulls(number, delta, 1)",
        },
        SqlTestCase {
            name: "least_ignore_nulls_rewrites_through_array_min",
            description: "least_ignore_nulls should rewrite directly to array_min.",
            setup_sqls: &[],
            sql: "least_ignore_nulls(number, delta, 1)",
        },
        SqlTestCase {
            name: "getvariable_constant_name_rewrites_to_context_value",
            description: "getvariable should resolve a constant variable name through TableContext.",
            setup_sqls: &[],
            sql: "getvariable('missing_var')",
        },
        SqlTestCase {
            name: "hex_decode_string_rewrites_via_binary_decode",
            description: "hex_decode_string should rewrite through the binary decoder and cast back to string.",
            setup_sqls: &[],
            sql: "hex_decode_string('64617461')",
        },
        SqlTestCase {
            name: "try_base64_decode_string_rewrites_via_binary_decode",
            description: "try_base64_decode_string should rewrite through the binary decoder and cast back to string.",
            setup_sqls: &[],
            sql: "try_base64_decode_string('ZGF0YQ==')",
        },
    ];

    run_type_check_cases("type_check_sugar_functions.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_aggregate_and_window_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "count_star_removes_count_args",
            description: "count(*) should type check through the aggregate path that removes redundant count arguments.",
            setup_sqls: &[],
            sql: "count(*)",
        },
        SqlTestCase {
            name: "count_distinct_rewrites_to_count_distinct",
            description: "count(distinct x) should select the count_distinct aggregate implementation.",
            setup_sqls: &[],
            sql: "count(distinct number)",
        },
        SqlTestCase {
            name: "sum_distinct_uses_distinct_aggregate_name",
            description: "A non-count DISTINCT aggregate should append the _distinct suffix during type checking.",
            setup_sqls: &[],
            sql: "sum(distinct number)",
        },
        SqlTestCase {
            name: "string_agg_delimiter_becomes_param",
            description: "string_agg with a constant delimiter should move the delimiter into aggregate params.",
            setup_sqls: &[],
            sql: "string_agg(text, '|')",
        },
        SqlTestCase {
            name: "listagg_within_group_preserves_sort_desc",
            description: "WITHIN GROUP should resolve aggregate sort descriptors at type-check time.",
            setup_sqls: &[],
            sql: "listagg(text, '|') WITHIN GROUP (ORDER BY number DESC NULLS LAST)",
        },
        SqlTestCase {
            name: "histogram_bucket_argument_becomes_param",
            description: "histogram(expr, buckets) should fold the bucket count into aggregate params.",
            setup_sqls: &[],
            sql: "histogram(number, 10)",
        },
        SqlTestCase {
            name: "nested_aggregate_errors",
            description: "Nested grouped aggregates should be rejected while resolving aggregate arguments.",
            setup_sqls: &[],
            sql: "sum(count(number))",
        },
        SqlTestCase {
            name: "rank_order_by_uses_default_order_frame",
            description: "A rank window with ORDER BY should keep the ordered default frame.",
            setup_sqls: &[],
            sql: "rank() OVER (ORDER BY number)",
        },
        SqlTestCase {
            name: "percent_rank_uses_full_rows_frame",
            description: "percent_rank has a dedicated full ROWS frame regardless of the ORDER BY clause.",
            setup_sqls: &[],
            sql: "percent_rank() OVER (PARTITION BY delta ORDER BY number)",
        },
        SqlTestCase {
            name: "cume_dist_uses_full_range_frame",
            description: "cume_dist has a dedicated full RANGE frame.",
            setup_sqls: &[],
            sql: "cume_dist() OVER (ORDER BY number)",
        },
        SqlTestCase {
            name: "lag_with_default_builds_preceding_frame",
            description: "lag(value, offset, default) should cast the default and build a preceding frame.",
            setup_sqls: &[],
            sql: "lag(number, 2, 0) OVER (PARTITION BY delta ORDER BY number)",
        },
        SqlTestCase {
            name: "lead_negative_offset_flips_direction",
            description: "lead with a negative offset should flip to the lag-side frame direction.",
            setup_sqls: &[],
            sql: "lead(number, -1) OVER (ORDER BY number)",
        },
        SqlTestCase {
            name: "first_value_ignore_nulls_rows_frame",
            description: "first_value IGNORE NULLS should resolve the ignore-null flag and ROWS frame offsets.",
            setup_sqls: &[],
            sql: "first_value(text) IGNORE NULLS OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        },
        SqlTestCase {
            name: "nth_value_window_binds",
            description: "nth_value should validate the constant positive index and use the nth-value window type.",
            setup_sqls: &[],
            sql: "nth_value(number, 2) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
        },
        SqlTestCase {
            name: "ntile_window_binds",
            description: "ntile should validate a constant positive bucket count.",
            setup_sqls: &[],
            sql: "ntile(3) OVER (PARTITION BY delta ORDER BY number)",
        },
        SqlTestCase {
            name: "range_offset_frame_binds",
            description: "A RANGE frame with constant offsets should fold the frame bounds.",
            setup_sqls: &[],
            sql: "sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING)",
        },
        SqlTestCase {
            name: "range_offset_requires_single_order_by",
            description: "A RANGE offset frame with multiple ORDER BY expressions should fail during type checking.",
            setup_sqls: &[],
            sql: "sum(number) OVER (ORDER BY number, delta RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING)",
        },
        SqlTestCase {
            name: "unbounded_following_start_errors",
            description: "A frame start cannot be UNBOUNDED FOLLOWING.",
            setup_sqls: &[],
            sql: "sum(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING)",
        },
        SqlTestCase {
            name: "unbounded_preceding_end_errors",
            description: "A frame end cannot be UNBOUNDED PRECEDING.",
            setup_sqls: &[],
            sql: "sum(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING)",
        },
    ];

    run_type_check_cases("type_check_aggregate_window.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_string_and_like_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "substring_from_for_from_tpc_h_binds",
            description: "The TPCH substring(expr FROM start FOR len) form should route to the substring function.",
            setup_sqls: &[],
            sql: "substring(text FROM 1 FOR 2)",
        },
        SqlTestCase {
            name: "position_in_from_function_tests_binds",
            description: "The POSITION(substr IN str) syntax should type check as locate(substr, str).",
            setup_sqls: &[],
            sql: "position(pattern IN text)",
        },
        SqlTestCase {
            name: "trim_leading_from_function_tests_binds",
            description: "The SQL TRIM LEADING form should type check through trim_leading.",
            setup_sqls: &[],
            sql: "trim(leading 'a' from text)",
        },
        SqlTestCase {
            name: "trim_both_dynamic_pattern_binds",
            description: "The SQL TRIM BOTH form should keep dynamic trim patterns supported.",
            setup_sqls: &[],
            sql: "trim(both pattern from text)",
        },
        SqlTestCase {
            name: "like_prefix_rewrites_to_range",
            description: "A LIKE prefix pattern from comparison tests should type check as a range predicate.",
            setup_sqls: &[],
            sql: "text LIKE 'ab%'",
        },
        SqlTestCase {
            name: "like_percent_rewrites_to_is_not_null",
            description: "A LIKE pattern made only of percent wildcards should type check as an IS NOT NULL predicate.",
            setup_sqls: &[],
            sql: "text LIKE '%%'",
        },
        SqlTestCase {
            name: "like_escape_from_sqllogictest_binds",
            description: "A LIKE ESCAPE expression from sqllogictests should preserve the escape-aware function path.",
            setup_sqls: &[],
            sql: "text LIKE 'a!_%' ESCAPE '!'",
        },
        SqlTestCase {
            name: "dynamic_like_pattern_keeps_like_function",
            description: "A dynamic LIKE pattern should not be rewritten as a constant-prefix optimization.",
            setup_sqls: &[],
            sql: "text LIKE pattern",
        },
        SqlTestCase {
            name: "regexp_operator_from_function_tests_binds",
            description: "The regexp operator form from scalar comparison tests should type check as regexp_like.",
            setup_sqls: &[],
            sql: "text REGEXP pattern",
        },
        SqlTestCase {
            name: "rlike_operator_from_function_tests_binds",
            description: "The rlike alias should share the regexp_like type-check path.",
            setup_sqls: &[],
            sql: "text RLIKE pattern",
        },
    ];

    run_type_check_cases("type_check_string_like.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_datetime_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "date_add_date_from_function_tests_binds",
            description: "The date_add(day, n, date) scalar-test shape should type check through date arithmetic rewrite.",
            setup_sqls: &[],
            sql: "date_add(day, delta, date)",
        },
        SqlTestCase {
            name: "date_sub_timestamp_from_function_tests_binds",
            description: "The date_sub(hour, n, timestamp) shape should type check as negative date arithmetic.",
            setup_sqls: &[],
            sql: "date_sub(hour, delta, ts)",
        },
        SqlTestCase {
            name: "date_diff_day_from_function_tests_binds",
            description: "The date_diff(day, start, end) scalar-test shape should resolve to diff_days.",
            setup_sqls: &[],
            sql: "date_diff(day, date, to_date('2024-12-31'))",
        },
        SqlTestCase {
            name: "date_trunc_month_from_sqllogictest_binds",
            description: "The date_trunc(month, ts) form should resolve through the date truncation sugar path.",
            setup_sqls: &[],
            sql: "date_trunc(month, ts)",
        },
        SqlTestCase {
            name: "time_slice_from_sqllogictest_binds",
            description: "TIME_SLICE with unit and boundary arguments should type check through the dedicated rewrite.",
            setup_sqls: &[],
            sql: "time_slice(ts, 8, 'day', 'end')",
        },
        SqlTestCase {
            name: "last_day_month_from_function_tests_binds",
            description: "last_day(timestamp, month) should type check through the adjacent calendar rewrite.",
            setup_sqls: &[],
            sql: "last_day(ts, month)",
        },
        SqlTestCase {
            name: "previous_day_from_function_tests_binds",
            description: "previous_day(timestamp, monday) should resolve the weekday literal during type checking.",
            setup_sqls: &[],
            sql: "previous_day(ts, monday)",
        },
        SqlTestCase {
            name: "next_day_from_function_tests_binds",
            description: "next_day(timestamp, friday) should resolve the weekday literal during type checking.",
            setup_sqls: &[],
            sql: "next_day(ts, friday)",
        },
    ];

    run_type_check_cases("type_check_datetime.txt", &cases).await
}
