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
