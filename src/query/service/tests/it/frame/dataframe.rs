// Copyright 2023 Datafuse Labs.
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

use common_ast::ast::Engine;
use common_ast::ast::Identifier;
use common_ast::ast::TableReference;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_base::base::tokio;
use common_catalog::catalog::CatalogManager;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_sql::optimizer::SExpr;
use common_sql::plans::CreateTablePlan;
use common_sql::plans::Plan;
use common_sql::BindContext;
use common_sql::Binder;
use common_sql::Metadata;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use databend_query::frame::dataframe::Dataframe;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::test_kits::TestFixture;
use parking_lot::RwLock;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;

fn assert_same_sexpr(s_expr1: &SExpr, s_expr2: &SExpr) {
    assert_eq!(format!("{s_expr1:?}"), format!("{s_expr2:?}"));
}

async fn plan_sql(
    ctx: Arc<dyn TableContext>,
    sql: &str,
) -> Result<(SExpr, Box<BindContext>, MetadataRef)> {
    let settings = ctx.get_settings();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let binder = Binder::new(
        ctx.clone(),
        CatalogManager::instance(),
        name_resolution_ctx,
        metadata,
    );
    let tokens = tokenize_sql(sql)?;
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;
    let plan = binder.bind(&stmt).await?;
    if let Plan::Query {
        s_expr,
        metadata,
        bind_context,
        ..
    } = plan
    {
        return Ok((*s_expr, bind_context, metadata));
    }
    unreachable!()
}

fn create_table_plan(fixture: &TestFixture) -> CreateTablePlan {
    CreateTablePlan {
        if_not_exists: false,
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: "default".to_string(),
        table: "t".to_string(),
        schema: TableSchemaRefExt::create(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("c", TableDataType::Number(NumberDataType::Int32)),
        ]),
        engine: Engine::Fuse,
        storage_params: None,
        part_prefix: "".to_string(),
        options: [
            // database id is required for FUSE
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_default_exprs: vec![],
        field_comments: vec![],
        as_select: None,
        cluster_key: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select() -> Result<()> {
    let fixture = TestFixture::new().await;
    let query_ctx = fixture.ctx();

    let df = Dataframe::new(query_ctx.clone(), None, None).await?;
    let create_table_plan = create_table_plan(&fixture);
    let interpreter = CreateTableInterpreter::try_create(query_ctx.clone(), create_table_plan)?;
    interpreter.execute(query_ctx.clone()).await?;

    // select single table
    {
        let (s_expr, _, _) = plan_sql(query_ctx, "select * from default.t").await?;

        let table_ref: Vec<TableReference> = vec![TableReference::Table {
            span: None,
            catalog: None,
            database: Some(Identifier {
                name: "default".to_string(),
                quote: None,
                span: None,
            }),
            table: Identifier {
                name: "t".to_string(),
                quote: None,
                span: None,
            },
            alias: None,
            travel_point: None,
            pivot: None,
            unpivot: None,
        }];

        let df_s_expr = df.select(table_ref).await?.get_expr().unwrap();
        assert_same_sexpr(&df_s_expr, &s_expr);
    }

    Ok(())
}
