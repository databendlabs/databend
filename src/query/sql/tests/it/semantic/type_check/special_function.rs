use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_special_function() -> Result<()> {
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
            name: "current_tenant_id_rewrites_to_literal",
            description: "current_tenant_id() should resolve the session tenant through the type-check adapter.",
            setup_sqls: &[],
            sql: "current_tenant_id()",
        },
        SqlTestCase {
            name: "current_tenant_id_rejects_arguments",
            description: "current_tenant_id() takes no arguments and must not allow tenant lookup by argument.",
            setup_sqls: &[],
            sql: "current_tenant_id('other')",
        },
        SqlTestCase {
            name: "is_role_in_session_matches_effective_role",
            description: "is_role_in_session should compare its argument against effective roles through the type-check adapter.",
            setup_sqls: &[],
            sql: "is_role_in_session('reader')",
        },
        SqlTestCase {
            name: "is_role_in_session_rejects_missing_role",
            description: "is_role_in_session should fold to false when no effective role matches.",
            setup_sqls: &[],
            sql: "is_role_in_session('owner')",
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

    run_type_check_cases("special_function.txt", &cases).await
}

/// Persisted and storage-level expressions must reject every read of session
/// or query context, since such values are folded into literals (issue #19833).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_context_independent_policy() -> Result<()> {
    init_testing_globals();
    let context_udf = UserDefinedFunction::create_lambda_udf(
        "context_udf",
        vec!["x".to_string()],
        "concat(x, current_database())",
        "",
    );
    let adapter = TestTypeCheckAdapter::new(Settings::create(Tenant::new_literal("default")))
        .with_udf_adapter(TestUdfAdapter::with_definitions([context_udf]))
        .with_context_independent(true);

    let cases = [
        SqlTestCase {
            name: "current_database_rejected",
            description: "current_database() must be rejected instead of folding to a literal.",
            setup_sqls: &[],
            sql: "current_database()",
        },
        SqlTestCase {
            name: "database_alias_rejected",
            description: "Aliases of current_database() must be rejected with the canonical name.",
            setup_sqls: &[],
            sql: "database()",
        },
        SqlTestCase {
            name: "nested_current_database_rejected",
            description: "A context function nested inside a deterministic expression must be rejected.",
            setup_sqls: &[],
            sql: "concat(to_string(number), current_database())",
        },
        SqlTestCase {
            name: "current_catalog_rejected",
            description: "current_catalog() depends on the session.",
            setup_sqls: &[],
            sql: "current_catalog()",
        },
        SqlTestCase {
            name: "version_rejected",
            description: "version() depends on the running server build.",
            setup_sqls: &[],
            sql: "version()",
        },
        SqlTestCase {
            name: "connection_id_rejected",
            description: "connection_id() depends on the session.",
            setup_sqls: &[],
            sql: "connection_id()",
        },
        SqlTestCase {
            name: "client_session_id_rejected",
            description: "client_session_id() depends on the session.",
            setup_sqls: &[],
            sql: "client_session_id()",
        },
        SqlTestCase {
            name: "current_user_rejected",
            description: "current_user() depends on the authenticated user.",
            setup_sqls: &[],
            sql: "current_user()",
        },
        SqlTestCase {
            name: "current_role_rejected",
            description: "current_role() depends on the session role.",
            setup_sqls: &[],
            sql: "current_role()",
        },
        SqlTestCase {
            name: "current_secondary_roles_rejected",
            description: "current_secondary_roles() depends on the session roles.",
            setup_sqls: &[],
            sql: "current_secondary_roles()",
        },
        SqlTestCase {
            name: "current_available_roles_rejected",
            description: "current_available_roles() depends on the session roles.",
            setup_sqls: &[],
            sql: "current_available_roles()",
        },
        SqlTestCase {
            name: "current_tenant_id_rejected",
            description: "current_tenant_id() depends on the session tenant.",
            setup_sqls: &[],
            sql: "current_tenant_id()",
        },
        SqlTestCase {
            name: "is_role_in_session_rejected",
            description: "is_role_in_session() depends on the effective roles.",
            setup_sqls: &[],
            sql: "is_role_in_session('reader')",
        },
        SqlTestCase {
            name: "timezone_rejected",
            description: "timezone() depends on the session settings.",
            setup_sqls: &[],
            sql: "timezone()",
        },
        SqlTestCase {
            name: "last_query_id_rejected",
            description: "last_query_id() depends on the session query history.",
            setup_sqls: &[],
            sql: "last_query_id(-1)",
        },
        SqlTestCase {
            name: "getvariable_rejected",
            description: "getvariable() depends on session variables.",
            setup_sqls: &[],
            sql: "getvariable('x')",
        },
        SqlTestCase {
            name: "session_variable_rejected",
            description: "$var lowers to getvariable() and must be rejected too.",
            setup_sqls: &[],
            sql: "number + $x",
        },
        SqlTestCase {
            name: "sql_udf_body_rejected",
            description: "A context function inside a SQL UDF body must be rejected when the UDF is expanded.",
            setup_sqls: &[],
            sql: "context_udf(text)",
        },
        SqlTestCase {
            name: "array_sort_default_nulls_order_rejected",
            description: "array_sort() without NULLS FIRST/LAST reads the session null-order setting.",
            setup_sqls: &[],
            sql: "array_sort([3, 1, 2])",
        },
        SqlTestCase {
            name: "array_sort_default_nulls_order_with_sort_order_rejected",
            description: "An explicit sort order alone still leaves the null order to the session.",
            setup_sqls: &[],
            sql: "array_sort([3, 1, 2], 'desc')",
        },
        SqlTestCase {
            name: "array_sort_explicit_nulls_order_allowed",
            description: "array_sort() with an explicit null order does not read session context.",
            setup_sqls: &[],
            sql: "array_sort([3, 1, 2], 'desc', 'nulls last')",
        },
        SqlTestCase {
            name: "coalesce_allowed",
            description: "Pure special-function rewrites stay allowed.",
            setup_sqls: &[],
            sql: "coalesce(text, pattern)",
        },
        SqlTestCase {
            name: "greatest_allowed",
            description: "Pure special-function rewrites stay allowed.",
            setup_sqls: &[],
            sql: "greatest(number, 1)",
        },
    ];

    let mut file = open_golden_file("semantic/type_check", "special_function_context.txt")?;
    for (index, case) in cases.iter().enumerate() {
        if index > 0 {
            writeln!(file)?;
        }
        write_case_header(&mut file, case)?;
        let mut bind_context = test_bind_context(ExprContext::Unknown);
        let outcome = match resolve_type_check_sql(case.sql, adapter.clone(), &mut bind_context) {
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
        write_case_outcome_body(&mut file, &outcome)?;
    }

    Ok(())
}
