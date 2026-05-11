use super::*;

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
            name: "aggregate_display_name_preserves_original_call",
            description: "Aggregate display names should keep the original SQL spelling while resolving arguments.",
            setup_sqls: &[],
            sql: "SUM(number + delta)",
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
            name: "listagg_within_group_lowers_order_expression",
            description: "WITHIN GROUP order expressions should keep resolving through aggregate sort descriptors.",
            setup_sqls: &[],
            sql: "listagg(text, '|') WITHIN GROUP (ORDER BY number + delta DESC NULLS LAST)",
        },
        SqlTestCase {
            name: "histogram_bucket_argument_becomes_param",
            description: "histogram(expr, buckets) should fold the bucket count into aggregate params.",
            setup_sqls: &[],
            sql: "histogram(number, 10)",
        },
        SqlTestCase {
            name: "aggregate_parameterized_call_binds",
            description: "Parameterized aggregate syntax should fold constant params and resolve aggregate arguments.",
            setup_sqls: &[],
            sql: "quantile_cont(0.6)(number)",
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
    ];

    run_type_check_cases("aggregate_window.txt", &cases).await
}
