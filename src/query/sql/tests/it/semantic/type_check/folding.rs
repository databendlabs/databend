// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The folding policy keeps column-dependent expressions unfolded, as when
//! binding the pre-aggregate forms that GROUPING SETS keys are matched against.

use databend_common_sql::binder::ScalarFoldingPolicy;

use super::*;

fn non_null_context() -> BindContext {
    let mut context = test_bind_context(ExprContext::Unknown);
    *context.columns[0].data_type = DataType::Number(NumberDataType::Int64);
    context
}

/// Bind `sql` over a non-null `number` with `policy`.
fn resolve_with_policy(
    sql: &str,
    adapter: TestTypeCheckAdapter,
    policy: ScalarFoldingPolicy,
) -> Result<(ScalarExpr, DataType)> {
    let mut context = non_null_context();
    let tokens = tokenize_sql(sql)?;
    let expr = parse_expr(&tokens, adapter.sql_dialect()?)?;
    let name_resolution_ctx = NameResolutionContext::try_from(adapter.settings.as_ref())?;
    let mut type_checker = TypeChecker::try_create_with_adapter(
        &mut context,
        adapter,
        &name_resolution_ctx,
        Arc::new(RwLock::new(Metadata::default())),
        &[],
    )?;
    type_checker.set_folding_policy(policy);
    type_checker.resolve(&expr).map(|resolved| *resolved)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_deferred_column_folding_preserves_column_predicates() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    for name in ["is_null", "is_not_null"] {
        let sql = format!("{name}(number)");
        let (ordinary, _) = resolve_with_policy(
            &sql,
            TestTypeCheckAdapter::new(settings.clone()),
            ScalarFoldingPolicy::Normal,
        )?;
        assert!(matches!(ordinary, ScalarExpr::ConstantExpr(_)));

        let (deferred, _) = resolve_with_policy(
            &sql,
            TestTypeCheckAdapter::new(settings.clone()),
            ScalarFoldingPolicy::PreserveColumnStructure,
        )?;
        assert!(matches!(deferred, ScalarExpr::FunctionCall(_)));
        assert!(deferred.used_columns().contains(&Symbol::new(0)));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_deferred_column_folding_preserves_nested_bound_children() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    for sql in [
        "CAST(is_null(number) AS INT)",
        "CAST(is_null(number) AS TINYINT UNSIGNED) + CAST(1 AS BIGINT)",
        "is_null(number + (40 + 2))",
        "if(is_not_null(number), number, 40 + 2)",
        "(number > 0) AND (number < 0)",
    ] {
        let (scalar, _) = resolve_with_policy(
            sql,
            TestTypeCheckAdapter::new(settings.clone()),
            ScalarFoldingPolicy::PreserveColumnStructure,
        )?;
        assert!(scalar.used_columns().contains(&Symbol::new(0)), "{sql}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_deferred_column_folding_preserves_udf_condition() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let adapter =
        TestTypeCheckAdapter::new(settings).with_udf_adapter(TestUdfAdapter::with_definitions([
            UserDefinedFunction::create_lambda_udf(
                "with_default",
                vec!["x".to_string()],
                "coalesce(x, 100)",
                "",
            ),
        ]));
    let (scalar, _) = resolve_with_policy(
        "with_default(number)",
        adapter,
        ScalarFoldingPolicy::PreserveColumnStructure,
    )?;
    let ScalarExpr::UDFLambdaCall(udf) = scalar else {
        panic!("expected an expanded lambda UDF")
    };
    let ScalarExpr::FunctionCall(function) = *udf.scalar else {
        panic!("the UDF must retain its conditional expression")
    };
    assert_eq!(function.func_name, "if");
    let ScalarExpr::FunctionCall(condition) = &function.arguments[0] else {
        panic!("the UDF parameter's non-null test must not become a constant")
    };
    assert_eq!(condition.func_name, "is_not_null");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_deferred_column_folding_keeps_pure_constant_evaluation() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let (scalar, _) = resolve_with_policy(
        "coalesce(NULL, 40 + 2)",
        TestTypeCheckAdapter::new(settings),
        ScalarFoldingPolicy::PreserveColumnStructure,
    )?;
    let ScalarExpr::ConstantExpr(constant) = scalar else {
        panic!("literal-only expressions should still be evaluated during binding")
    };
    assert_eq!(constant.value.get_i64(), Some(42));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_preserved_column_structure_still_folds_literal_children() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    let (scalar, data_type) = resolve_with_policy(
        "number + (40 + 2)",
        TestTypeCheckAdapter::new(settings),
        ScalarFoldingPolicy::PreserveColumnStructure,
    )?;
    let ScalarExpr::FunctionCall(call) = &scalar else {
        panic!("a column-dependent call must retain its structure")
    };
    assert!(matches!(call.arguments[0], ScalarExpr::BoundColumnRef(_)));
    let ScalarExpr::ConstantExpr(constant) = &call.arguments[1] else {
        panic!("a literal-only child must still fold")
    };
    assert_eq!(constant.value.get_i64(), Some(42));
    assert_eq!(scalar.data_type().as_ref(), &data_type);
    assert_eq!(scalar.as_expr()?.data_type(), &data_type);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_nested_context_does_not_inherit_scalar_folding_policy() -> Result<()> {
    init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    // The type checker of a nested query folds normally, whatever the policy
    // of the enclosing type checker.
    let mut child = BindContext::with_parent(non_null_context())?;
    let (scalar, _) = resolve_type_check_sql(
        "is_null(number)",
        TestTypeCheckAdapter::new(settings),
        &mut child,
    )?;
    let ScalarExpr::ConstantExpr(constant) = scalar else {
        panic!("a nested query can fold a predicate on its non-null input")
    };
    assert_eq!(constant.value, Scalar::Boolean(false));
    Ok(())
}
