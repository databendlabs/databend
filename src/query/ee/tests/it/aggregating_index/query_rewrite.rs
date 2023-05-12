// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_base::base::tokio;
use common_catalog::catalog::CatalogManager;
use common_exception::Result;
use common_sql::optimizer::SExpr;
use common_sql::plans::Plan;
use common_sql::Binder;
use common_sql::Metadata;
use common_sql::NameResolutionContext;
use common_storages_fuse::TableContext;
use databend_query::test_kits::TestFixture;
use enterprise_query::aggregating_index::query_rewrite;
use parking_lot::RwLock;

struct TestSuite {
    query: &'static str,
    index: &'static str,
    is_matched: bool,
}

fn get_test_suites() -> Vec<TestSuite> {
    vec![]
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_rewrite() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    let test_suites = get_test_suites();
    for suite in test_suites {
        let query = plan_sql(ctx.clone(), suite.query).await?;
        let index = plan_sql(ctx.clone(), suite.index).await?;
        let is_matched = query_rewrite::rewrite(&query, &vec![index])?.is_some();
        assert_eq!(is_matched, suite.is_matched);
    }

    Ok(())
}

async fn plan_sql(ctx: Arc<dyn TableContext>, sql: &str) -> Result<SExpr> {
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
    if let Plan::Query { s_expr, .. } = plan {
        return Ok(*s_expr);
    }
    unreachable!()
}
