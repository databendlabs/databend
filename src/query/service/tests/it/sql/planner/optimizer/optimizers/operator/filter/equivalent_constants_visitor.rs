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

use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnBinding;
use databend_common_sql::Metadata;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::TypeChecker;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::optimizers::operator::EquivalentConstantsVisitor;
use databend_common_sql::plans::VisitorMut;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;
use parking_lot::RwLock;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_equivalent_constants() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let cases = [
        ("n = 1 and m = n", "n = 1 and m = 1"),
        (
            "n = 1 and (m > n and m < (n + m))",
            "n = 1 and (m > 1 and m < (1 + m))",
        ),
        (
            "(m > n and m < (n + m)) and n = 1",
            "(m > 1 and m < (1 + m)) and n = 1",
        ),
        (
            "n = 1 or (m > n and m < (n + 2))",
            "n = 1 or (m > n and m < (n + 2))",
        ),
        (
            "n = 1 or (n = 2 and m > n and m < (n + m))",
            "n = 1 or (n = 2 and m > 2 and m < (2 + m))",
        ),
        ("a = 1 and b = a", "a = 1 and b = a"),
        (
            "a = 1 and (b > a and b < (a + b))",
            "a = 1 and (b > a and b < (a + b))",
        ),
        (
            "a = 1 or (a = 2 and b > a and b < (a + b))",
            "a = 1 or (a = 2 and b > a and b < (a + b))",
        ),
    ];

    for (expr, expect) in cases {
        check(expr, expect, &ctx)?;
    }
    Ok(())
}

fn check(expr: &str, expect: &str, query_ctx: &Arc<QueryContext>) -> Result<()> {
    let mut expr = resolve_expr(query_ctx, expr)?;
    let expect = resolve_expr(query_ctx, expect)?;

    let mut visitor = EquivalentConstantsVisitor::default();
    visitor.visit(&mut expr)?;

    assert_eq!(expr, expect);
    Ok(())
}

fn resolve_expr(query_ctx: &Arc<QueryContext>, text: &str) -> Result<ScalarExpr> {
    let settings = query_ctx.get_settings();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut bind_context = test_bind_context();

    let mut checker = TypeChecker::try_create(
        &mut bind_context,
        query_ctx.clone(),
        &name_resolution_ctx,
        metadata,
        &[],
        true,
    )?;

    let tokens = tokenize_sql(text)?;
    let expr = parse_expr(&tokens, Dialect::PostgreSQL)?;
    let (scalar_expr, _) = *checker.resolve(&expr)?;
    Ok(scalar_expr)
}

fn test_bind_context() -> BindContext {
    let mut bind_context = BindContext::new();

    bind_context.add_column_binding(ColumnBinding {
        database_name: None,
        table_name: None,
        column_position: None,
        table_index: None,
        column_name: "a".to_string(),
        index: Symbol::new(0),
        data_type: Box::new(DataType::String),
        visibility: Visibility::Visible,
        virtual_expr: None,
        is_srf: false,
        column_name_lower: None,
    });
    bind_context.add_column_binding(ColumnBinding {
        database_name: None,
        table_name: None,
        column_position: None,
        table_index: None,
        column_name: "b".to_string(),
        index: Symbol::new(1),
        data_type: Box::new(DataType::String),
        visibility: Visibility::Visible,
        virtual_expr: None,
        is_srf: false,
        column_name_lower: None,
    });
    bind_context.add_column_binding(ColumnBinding {
        database_name: None,
        table_name: None,
        column_position: None,
        table_index: None,
        column_name: "n".to_string(),
        index: Symbol::new(2),
        data_type: Box::new(DataType::Number(NumberDataType::Int64)),
        visibility: Visibility::Visible,
        virtual_expr: None,
        is_srf: false,
        column_name_lower: None,
    });
    bind_context.add_column_binding(ColumnBinding {
        database_name: None,
        table_name: None,
        column_position: None,
        table_index: None,
        column_name: "m".to_string(),
        index: Symbol::new(3),
        data_type: Box::new(DataType::Number(NumberDataType::Int64)),
        visibility: Visibility::Visible,
        virtual_expr: None,
        is_srf: false,
        column_name_lower: None,
    });
    bind_context
}
