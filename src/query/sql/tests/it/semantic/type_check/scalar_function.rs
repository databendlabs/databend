use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_scalar_function_rules() -> Result<()> {
    let cases = [
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
