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

use std::sync::Arc;

use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use databend_common_sql::BindContext;
use databend_common_sql::Metadata;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::PersistedTypeCheckAdapter;
use databend_common_sql::TypeCheckAdapter;
use databend_common_sql::TypeChecker;
use databend_common_sql::parse_exprs;
use databend_common_sql::parse_to_filters;
use parking_lot::RwLock;

use crate::framework::LiteTableContext;

/// Storage filters must reject both nondeterministic functions and context
/// functions that would otherwise fold into deterministic literals (#19833).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parse_to_filters_determinism() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql("CREATE TABLE filter_test(a INT, b STRING)")
        .await?;
    let table = ctx.get_table("default", "default", "filter_test").await?;

    for sql in ["a > 3", "a > 0 AND b = 'literal'"] {
        parse_to_filters(ctx.clone(), table.clone(), sql)?;
    }

    for (sql, expected_message) in [
        ("a > rand()", "is not deterministic"),
        (
            "b = current_database()",
            "`current_database()` depends on the session or query context",
        ),
        (
            "b = concat('prefix', current_database())",
            "`current_database()` depends on the session or query context",
        ),
        (
            "a > filter_udf(a)",
            "UDFs are not allowed in persisted or storage-level expressions",
        ),
    ] {
        let err = parse_to_filters(ctx.clone(), table.clone(), sql).unwrap_err();
        assert_eq!(err.code(), ErrorCode::SemanticError("").code(), "{sql}");
        assert!(
            err.message().contains(expected_message),
            "unexpected error for `{sql}`: {err}"
        );
        if sql.contains("concat") {
            let span = err.span().expect("context errors should have a span");
            assert_eq!(
                &sql[span.start() as usize..span.end() as usize],
                "current_database()"
            );
        }
    }

    // DEFAULT expressions use this permissive entry point and may read context.
    parse_exprs(ctx.clone(), table, "current_database()")?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_persisted_adapter_snapshot_and_cached_udf() -> Result<()> {
    crate::framework::init_testing_globals();
    let settings = Settings::create(Tenant::new_literal("default"));
    settings.set_setting("inlist_to_join_threshold".into(), "4".into())?;
    let func_ctx = FunctionContext {
        week_start: 1,
        ..Default::default()
    };
    let adapter = PersistedTypeCheckAdapter::new(&settings, func_ctx.clone())?;
    assert_eq!(adapter.function_context()?, func_ctx);
    settings.set_setting("inlist_to_join_threshold".into(), "8".into())?;
    assert_eq!(adapter.inlist_to_join_threshold()?, 4);
    assert!(adapter.timezone().is_err());
    assert!(adapter.default_nulls_first(true).is_err());

    let mut bind_context = BindContext::new();
    bind_context.udf_cache.write().insert(
        "cached_udf".into(),
        Some(UserDefinedFunction::create_lambda_udf(
            "cached_udf",
            vec!["x".into()],
            "x + 1",
            "",
        )),
    );
    let names = NameResolutionContext::try_from(settings.as_ref())?;
    let mut checker = TypeChecker::try_create_with_adapter(
        &mut bind_context,
        adapter.clone(),
        &names,
        Arc::new(RwLock::new(Metadata::default())),
        &[],
    )?;
    let expr = parse_expr(&tokenize_sql("cached_udf(1)")?, adapter.sql_dialect()?)?;
    let err = checker.resolve(&expr).unwrap_err();
    assert_eq!(err.code(), ErrorCode::SemanticError("").code());
    assert!(err.message().contains("UDFs are not allowed"));
    assert!(err.span().is_some());
    Ok(())
}
