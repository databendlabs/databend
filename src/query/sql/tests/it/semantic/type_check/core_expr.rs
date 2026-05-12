use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_core_expr_lowering() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "minus_literal_is_folded_after_scalar_resolution",
            description: "Unary minus on a literal should use the normal scalar path and fold to a negative literal.",
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
        SqlTestCase {
            name: "within_group_check_precedes_window_lowering",
            description: "WITHIN GROUP legality should match the old function dispatch order before window-specific lowering.",
            setup_sqls: &[],
            sql: "row_number() WITHIN GROUP (ORDER BY number)",
        },
    ];

    run_type_check_cases("core_expr.txt", &cases).await
}
