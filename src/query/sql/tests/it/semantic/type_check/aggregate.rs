use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_aggregate_rewrites() -> Result<()> {
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
            name: "non_aggregate_function_rejects_within_group",
            description: "WITHIN GROUP syntax should remain limited to aggregate functions.",
            setup_sqls: &[],
            sql: "abs(number) WITHIN GROUP (ORDER BY number)",
        },
        SqlTestCase {
            name: "aggregate_parameter_must_be_constant",
            description: "Parameterized aggregate arguments should be constant before aggregate resolution.",
            setup_sqls: &[],
            sql: "quantile_cont(number)(number)",
        },
    ];

    run_type_check_cases("aggregate.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_aggregate_window_error_restores_type_checker_state() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter = TestTypeCheckAdapter::new(settings.clone());
    let mut bind_context = test_bind_context(ExprContext::Unknown);
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let mut type_checker = TypeChecker::try_create_with_adapter(
        &mut bind_context,
        adapter,
        &name_resolution_ctx,
        metadata,
        &[],
    )?;

    let tokens = tokenize_sql("sum(text) OVER ()")?;
    let expr = parse_expr(&tokens, settings.get_sql_dialect()?)?;
    assert!(type_checker.resolve(&expr).is_err());

    let tokens = tokenize_sql("lag(number, 1) OVER (ORDER BY number)")?;
    let expr = parse_expr(&tokens, settings.get_sql_dialect()?)?;
    let _ = type_checker.resolve(&expr)?;

    Ok(())
}
