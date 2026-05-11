use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_special_functions() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "current_catalog_rewrites_to_literal",
            description: "A context special function should rewrite to a literal without needing full binder planning.",
            setup_sqls: &[],
            sql: "current_catalog()",
        },
        SqlTestCase {
            name: "current_database_rewrites_to_literal",
            description: "current_database() should use the TypeChecker special function path.",
            setup_sqls: &[],
            sql: "current_database()",
        },
        SqlTestCase {
            name: "version_rewrites_to_literal",
            description: "version() should use the TypeChecker special function path.",
            setup_sqls: &[],
            sql: "version()",
        },
        SqlTestCase {
            name: "current_user_rewrites_to_literal",
            description: "current_user() should use the TypeChecker special function path.",
            setup_sqls: &[],
            sql: "current_user()",
        },
        SqlTestCase {
            name: "current_role_rewrites_to_literal",
            description: "current_role() should use the TypeChecker special function path.",
            setup_sqls: &[],
            sql: "current_role()",
        },
        SqlTestCase {
            name: "timezone_rewrites_to_literal",
            description: "timezone() should read settings through the explicit type-check adapter.",
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
            name: "current_database_rejects_arguments",
            description: "context literal special functions should validate arity while lowering.",
            setup_sqls: &[],
            sql: "current_database(1)",
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
            description: "getvariable should resolve a constant variable name through the explicit type-check adapter.",
            setup_sqls: &[],
            sql: "getvariable('missing_var')",
        },
        SqlTestCase {
            name: "getvariable_rejects_missing_name",
            description: "getvariable should validate its single-argument shape while lowering.",
            setup_sqls: &[],
            sql: "getvariable()",
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

    run_type_check_cases("special_functions.txt", &cases).await
}
