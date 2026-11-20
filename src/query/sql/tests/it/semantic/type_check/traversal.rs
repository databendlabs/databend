use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_traversal_boundaries() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "array_children_keep_input_order_and_types",
            description: "Collection node typing consumes bound children without changing their order or common type.",
            setup_sqls: &[],
            sql: "[1, number, delta + 1]",
        },
        SqlTestCase {
            name: "map_children_keep_key_value_association",
            description: "The traversal driver binds each map key/value pair before constructing the map.",
            setup_sqls: &[],
            sql: "{'left': number, 'right': delta + 1}",
        },
        SqlTestCase {
            name: "tuple_scalar_cast_and_array_children",
            description: "Ordinary node kernels receive bound scalar, cast and collection children.",
            setup_sqls: &[],
            sql: "(number + 1, CAST(delta AS BIGINT), [number])",
        },
        SqlTestCase {
            name: "special_function_keeps_its_argument_semantics",
            description: "Special-function traversal still performs its original scalar rewrite.",
            setup_sqls: &[],
            sql: "coalesce(NULL, number + 1)",
        },
        SqlTestCase {
            name: "window_partition_order_and_range_contexts",
            description: "Window arguments, partition/order expressions and frame offsets retain their traversal phases.",
            setup_sqls: &[],
            sql: "sum(number) OVER (PARTITION BY flag ORDER BY delta RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
        },
        SqlTestCase {
            name: "lambda_body_keeps_its_parameter_scope",
            description: "A lambda body is resolved by a new checker with its own parameter bindings.",
            setup_sqls: &[],
            sql: "array_transform([number], x -> x + delta)",
        },
        SqlTestCase {
            name: "parameter_error_precedes_argument_binding",
            description: "Function parameters are validated before visiting an effectful argument.",
            setup_sqls: &[],
            sql: "round(number)(never_loaded(number))",
        },
        SqlTestCase {
            name: "in_comparison_error_precedes_next_item",
            description: "IN comparisons stay interleaved with item traversal rather than eagerly binding every item.",
            setup_sqls: &[],
            sql: "number IN ((1, 2), never_loaded(number))",
        },
        SqlTestCase {
            name: "dictionary_lookup_precedes_key_binding",
            description: "The async traversal resolves the dictionary before visiting its key expression.",
            setup_sqls: &[],
            sql: "dict_get(missing_dict, 'field', never_loaded(number))",
        },
        SqlTestCase {
            name: "nested_srf_keeps_argument_context",
            description: "Set-returning argument traversal still rejects nested SRFs before node construction.",
            setup_sqls: &[],
            sql: "unnest(unnest([number]))",
        },
    ];
    run_type_check_cases("traversal.txt", &cases).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_srf_child_error_restores_traversal_context() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    for sql in ["unnest(unnest([number]))", "unnest(abs(number, number))"] {
        let adapter = TestTypeCheckAdapter::new(settings.clone());
        let mut context = test_bind_context(ExprContext::Unknown);
        assert!(resolve_type_check_sql(sql, adapter.clone(), &mut context).is_err());
        assert_eq!(context.expr_context, ExprContext::Unknown, "{sql}");
        resolve_type_check_sql("unnest([number])", adapter, &mut context)?;
        assert_eq!(context.expr_context, ExprContext::Unknown);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_traversal_does_not_eagerly_bind_later_arguments() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    for sql in [
        "round(number)(never_loaded(number))",
        "number IN ((1, 2), never_loaded(number))",
        "dict_get(missing_dict, 'field', never_loaded(number))",
    ] {
        let adapter = TestTypeCheckAdapter::new(settings.clone());
        let mut context = test_bind_context(ExprContext::Unknown);
        let result = resolve_type_check_sql(sql, adapter.clone(), &mut context);
        assert!(result.is_err(), "{sql}");
        assert_eq!(
            adapter.udf_adapter.definition_load_count(),
            0,
            "must preserve validation/child-binding order for {sql}",
        );
    }
    Ok(())
}
