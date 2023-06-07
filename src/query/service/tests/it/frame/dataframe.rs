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

use arrow_flight::sql;
use common_ast::ast::Engine;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_ast::ast::TableReference;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_base::base::tokio;
use common_catalog::catalog::CatalogManager;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::block_debug::assert_blocks_eq;
use common_expression::block_debug::assert_blocks_sorted_eq;
use common_expression::block_debug::box_render;
use common_expression::types::NumberDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_sql::dataframe::Dataframe;
use common_sql::optimizer::SExpr;
use common_sql::plans::CreateTablePlan;
use common_sql::plans::Plan;
use common_sql::BindContext;
use common_sql::Binder;
use common_sql::Metadata;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use tokio_stream::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dataframe() -> Result<()> {
    let fixture = TestFixture::new().await;
    let query_ctx = fixture.ctx();
    fixture.create_default_table().await.unwrap();

    let db = fixture.default_db_name();
    let table = fixture.default_table_name();

    // select single table
    {
        let df = Dataframe::scan(query_ctx.clone(), Some(&db), &table)
            .await
            .unwrap()
            .select_columns(&["id"])
            .await
            .unwrap();

        let sql = format!("select id from {}.{}", db, table);
        test_case(&sql, df, fixture.ctx()).await;
    }

    {
        let df = Dataframe::scan_one(query_ctx.clone())
            .await
            .unwrap()
            .select(vec![Expr::Literal {
                span: None,
                lit: Literal::UInt64(3),
            }])
            .await
            .unwrap();

        test_case("select 3 from system.one", df, fixture.ctx()).await;
    }

    {
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "tables")
            .await
            .unwrap()
            .limit(Some(3), 1)
            .await
            .unwrap();

        test_case(
            "select * from system.tables limit 3 offset 1",
            df,
            fixture.ctx(),
        )
        .await;
    }

    Ok(())
}

async fn test_case(sql: &str, df: Dataframe, ctx: Arc<QueryContext>) {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await.unwrap();

    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await.unwrap();
    let stream = interpreter.execute(ctx.clone()).await.unwrap();
    let blocks = stream.map(|v| v).collect::<Vec<_>>().await;

    let interpreter = InterpreterFactory::get(ctx.clone(), &df.into_plan())
        .await
        .unwrap();
    let stream = interpreter.execute(ctx.clone()).await.unwrap();
    let blocks2 = stream.map(|v| v).collect::<Vec<_>>().await;

    assert_eq!(blocks.len(), blocks2.len());

    let schema = plan.schema();

    for (a, b) in blocks.iter().zip(blocks2.iter()) {
        let a = box_render(&schema, &[a.clone().unwrap()]).unwrap();
        let b = box_render(&schema, &[b.clone().unwrap()]).unwrap();

        assert_eq!(a, b);
    }
}
