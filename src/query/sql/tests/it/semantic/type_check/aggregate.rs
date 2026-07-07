use databend_common_expression::types::NumberScalar;

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
            name: "aggregate_argument_order_by_preserves_sort_desc",
            description: "ORDER BY inside aggregate arguments should resolve aggregate sort descriptors.",
            setup_sqls: &[],
            sql: "string_agg(text, '|' ORDER BY number DESC NULLS LAST)",
        },
        SqlTestCase {
            name: "ordered_aggregate_filter_uses_base_name_for_validation",
            description: "FILTER should not make ordered aggregate validation check the internal _if combinator name.",
            setup_sqls: &[],
            sql: "string_agg(text, '|' ORDER BY number DESC NULLS LAST) FILTER (WHERE flag)",
        },
        SqlTestCase {
            name: "aggregate_filter_lowers_to_if_combinator",
            description: "Aggregate FILTER should lower to the existing _if aggregate combinator.",
            setup_sqls: &[],
            sql: "sum(number) FILTER (WHERE flag)",
        },
        SqlTestCase {
            name: "count_star_filter_lowers_to_count_if",
            description: "COUNT(*) FILTER should add a constant count argument before the filter predicate.",
            setup_sqls: &[],
            sql: "count(*) FILTER (WHERE flag)",
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
            name: "non_aggregate_function_rejects_filter",
            description: "FILTER syntax should return a clear semantic error for non-aggregate functions.",
            setup_sqls: &[],
            sql: "abs(number) FILTER (WHERE flag)",
        },
        SqlTestCase {
            name: "distinct_aggregate_filter_reports_unsupported",
            description: "Parser accepts DISTINCT aggregate FILTER, but execution semantics are not wired yet.",
            setup_sqls: &[],
            sql: "sum(DISTINCT number) FILTER (WHERE flag)",
        },
        SqlTestCase {
            name: "combinator_aggregate_filter_reports_unsupported",
            description: "FILTER on an aggregate that already targets a combinator (sum_if) should be rejected instead of producing sum_if_if.",
            setup_sqls: &[],
            sql: "sum_if(number, flag) FILTER (WHERE flag)",
        },
        SqlTestCase {
            name: "distinct_aggregate_order_by_reports_unsupported",
            description: "Parser accepts DISTINCT aggregate ORDER BY, but execution semantics are not wired yet.",
            setup_sqls: &[],
            sql: "string_agg(DISTINCT text ORDER BY number)",
        },
        SqlTestCase {
            name: "count_distinct_aggregate_order_by_reports_unsupported",
            description: "DISTINCT aggregate ORDER BY should be rejected before ordered aggregate function validation.",
            setup_sqls: &[],
            sql: "count(DISTINCT number ORDER BY delta)",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_aggregate_filter_uses_if_combinator() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter = TestTypeCheckAdapter::new(settings);
    let mut bind_context = test_bind_context(ExprContext::Unknown);

    let (scalar, _) = resolve_type_check_sql(
        "sum(number) FILTER (WHERE flag)",
        adapter,
        &mut bind_context,
    )?;
    let ScalarExpr::AggregateFunction(agg) = scalar else {
        panic!("expected aggregate function");
    };
    assert_eq!(agg.func_name, "sum_if");
    assert_eq!(agg.args.len(), 2);

    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter = TestTypeCheckAdapter::new(settings);
    let mut bind_context = test_bind_context(ExprContext::Unknown);
    let (scalar, _) =
        resolve_type_check_sql("count(*) FILTER (WHERE flag)", adapter, &mut bind_context)?;
    let ScalarExpr::AggregateFunction(agg) = scalar else {
        panic!("expected aggregate function");
    };
    assert_eq!(agg.func_name, "count_if");
    assert_eq!(agg.args.len(), 2);

    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter = TestTypeCheckAdapter::new(settings);
    let mut bind_context = test_bind_context(ExprContext::Unknown);
    let (scalar, _) = resolve_type_check_sql(
        "histogram(number, 2) FILTER (WHERE flag)",
        adapter,
        &mut bind_context,
    )?;
    let ScalarExpr::AggregateFunction(agg) = scalar else {
        panic!("expected aggregate function");
    };
    assert_eq!(agg.func_name, "histogram_if");
    assert_eq!(agg.args.len(), 3);
    assert_eq!(agg.params, vec![Scalar::Number(NumberScalar::UInt64(2))]);

    Ok(())
}
