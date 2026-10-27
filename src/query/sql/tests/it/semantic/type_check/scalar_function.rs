use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_scalar_function_rules() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "decimal_division_error_keeps_checked_argument_types",
            description: "Failed parent folding must not shrink its checked Decimal arguments into integers and select another overload.",
            setup_sqls: &[],
            sql: "1::DECIMAL(38,0) / 0::DECIMAL(38,0)",
        },
        SqlTestCase {
            name: "decimal_round_overflow_keeps_checked_argument_types",
            description: "A rounding overflow remains a Decimal call rather than a stale Decimal return type on a Float64 overload.",
            setup_sqls: &[],
            sql: "round(99::DECIMAL(2,0), -2)",
        },
        SqlTestCase {
            name: "decimal_constant_argument_keeps_precision",
            description: "Folding a constant Decimal argument must not change the precision used to resolve a nonconstant division.",
            setup_sqls: &[],
            sql: "1::DECIMAL(38,0) / number::DECIMAL(38,0)",
        },
        SqlTestCase {
            name: "mixed_numeric_decimal_error_keeps_checked_argument_types",
            description: "The integer argument of a Decimal-valued call also contributes to its inferred precision.",
            setup_sqls: &[],
            sql: "1::UInt64 / 0::DECIMAL(38,0)",
        },
        SqlTestCase {
            name: "unknown_function_keeps_suggestion",
            description: "Unknown scalar functions should keep the suggestion-oriented error.",
            setup_sqls: &[],
            sql: "abss(number)",
        },
        SqlTestCase {
            name: "scalar_parameter_must_be_constant",
            description: "Parameterized scalar functions should reject non-constant parameters before scalar resolution.",
            setup_sqls: &[],
            sql: "to_decimal(number, number)",
        },
        SqlTestCase {
            name: "identity_cast_preserves_complete_argument",
            description: "An eliminated identity cast must not remap an inner function argument onto the original call.",
            setup_sqls: &[],
            sql: "to_int64(100000000 + ((854435761::UInt64 * number + 123456789) % 900000000))",
        },
        SqlTestCase {
            name: "direct_cast_function",
            description: "A function is rewritten when Expr::Cast directly resolves to the same checked FunctionCall.",
            setup_sqls: &[],
            sql: "to_int64(assume_not_null(text))",
        },
        SqlTestCase {
            name: "direct_try_cast_function",
            description: "TRY_CAST equivalence comes from its resolved function instead of the function-name prefix.",
            setup_sqls: &[],
            sql: "try_to_int64(assume_not_null(text))",
        },
        SqlTestCase {
            name: "decimal_cast_factory_params",
            description: "Decimal factory params must match the params reconstructed by Expr::Cast.",
            setup_sqls: &[],
            sql: "to_decimal(10, 2)(assume_not_null(number))",
        },
        SqlTestCase {
            name: "variant_function_differs_from_string_cast",
            description: "to_variant(String) must stay a call because CAST(String AS Variant) executes parse_json.",
            setup_sqls: &[],
            sql: "to_variant(assume_not_null(text))",
        },
        SqlTestCase {
            name: "parse_json_matches_string_cast",
            description: "parse_json(String) is rewritten because it is the function selected by Expr::Cast.",
            setup_sqls: &[],
            sql: "parse_json(assume_not_null(text))",
        },
        SqlTestCase {
            name: "nullable_cast_function",
            description: "A nullable function is rewritten when Expr::Cast resolves the same complete nullable call.",
            setup_sqls: &[],
            sql: "to_int64(text)",
        },
        SqlTestCase {
            name: "nullable_number_to_variant",
            description: "CAST of a nullable number resolves the same nullable to_variant call.",
            setup_sqls: &[],
            sql: "to_variant(number)",
        },
        SqlTestCase {
            name: "partially_folded_if_keeps_argument_alignment",
            description: "Removing constant IF branches must preserve alignment with planner arguments.",
            setup_sqls: &[],
            sql: "if(false, 1, number > 0, 2, 3)",
        },
        SqlTestCase {
            name: "partially_folded_if_with_nested_cast",
            description: "A nested cast selected by constant folding must not be treated as a top-level cast rewrite.",
            setup_sqls: &[],
            sql: "if(true, CAST(number AS BIGINT), 0)",
        },
    ];

    run_type_check_cases("scalar_function.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_checked_decimal_constant_types_survive_partial_folding() -> Result<()> {
    init_testing_globals();
    for sql in [
        "1::DECIMAL(38,0) / 0::DECIMAL(38,0)",
        "round(99::DECIMAL(2,0), -2)",
        "1::DECIMAL(38,0) / number::DECIMAL(38,0)",
        "1::UInt64 / 0::DECIMAL(38,0)",
        "1::Int64 / number::DECIMAL(38,0)",
    ] {
        let settings = Settings::create(Tenant::new_literal("default"));
        let adapter = TestTypeCheckAdapter::new(settings);
        let mut context = test_bind_context(ExprContext::Unknown);
        let (scalar, stored_type) = resolve_type_check_sql(sql, adapter, &mut context)?;
        assert_eq!(stored_type, *scalar.as_expr()?.data_type(), "{sql}");
    }
    Ok(())
}
