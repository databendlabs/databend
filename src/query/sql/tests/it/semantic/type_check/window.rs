use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_window_rewrites() -> Result<()> {
    let cases = [
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
            name: "window_display_name_preserves_original_call",
            description: "Window display names should keep the original SQL spelling while resolving arguments.",
            setup_sqls: &[],
            sql: "LAG(number, 1, 0) OVER (ORDER BY delta)",
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
        SqlTestCase {
            name: "window_function_inside_lambda_errors",
            description: "Window functions should remain rejected while resolving lambda bodies.",
            setup_sqls: &[],
            sql: "array_transform([number], x -> row_number() OVER ())",
        },
        SqlTestCase {
            name: "non_window_function_rejects_over_clause",
            description: "A scalar function should not be accepted with window syntax.",
            setup_sqls: &[],
            sql: "abs(number) OVER ()",
        },
        SqlTestCase {
            name: "window_function_rejects_ignore_nulls_when_unsupported",
            description: "Non-value window functions should reject IGNORE/RESPECT NULLS options.",
            setup_sqls: &[],
            sql: "row_number() IGNORE NULLS OVER ()",
        },
    ];

    run_type_check_cases("window.txt", &cases).await
}
