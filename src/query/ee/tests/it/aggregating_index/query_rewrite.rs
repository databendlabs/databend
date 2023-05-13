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

use common_ast::ast::Engine;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_base::base::tokio;
use common_catalog::catalog::CatalogManager;
use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_sql::optimizer::SExpr;
use common_sql::plans::AggIndexInfo;
use common_sql::plans::CreateTablePlan;
use common_sql::plans::Plan;
use common_sql::plans::RelOperator;
use common_sql::Binder;
use common_sql::Metadata;
use common_sql::NameResolutionContext;
use common_storages_fuse::TableContext;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::test_kits::TestFixture;
use enterprise_query::aggregating_index::query_rewrite;
use parking_lot::RwLock;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;

#[derive(Default)]
struct TestSuite {
    query: &'static str,
    index: &'static str,
    is_matched: bool,
    _rewritten_selection: Vec<&'static str>,
    _rewritten_filter: Vec<&'static str>,
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

/// Generate test suites.
///
/// Table:
///
/// ```sql
/// create table t (a int, b int, c int);
/// ```
fn get_test_suites() -> Vec<TestSuite> {
    vec![
        TestSuite {
            query: "select to_string(c + 1) from t",
            index: "select c + 1 from t",
            is_matched: true,
            _rewritten_selection: vec!["to_string(index_col_0 (#0))"],
            _rewritten_filter: vec![],
        },
        //  query: eval-scan, index: eval-scan
        TestSuite {
            query: "select a from t",
            index: "select a from t",
            is_matched: true,
            _rewritten_selection: vec!["index_col_0 (#0)"],
            _rewritten_filter: vec![],
        },
        TestSuite {
            query: "select a + 1, to_string(a) from t",
            index: "select a from t",
            is_matched: true,
            _rewritten_selection: vec!["plus(index_col_0 (#0), 1)", "to_string(index_col_0 (#0))"],
            _rewritten_filter: vec![],
        },
        TestSuite {
            query: "select b from t",
            index: "select a, b from t",
            is_matched: true,
            _rewritten_selection: vec!["index_col_1 (#1)"],
            _rewritten_filter: vec![],
        },
        TestSuite {
            query: "select a from t",
            index: "select b, c from t",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-filter-scan, index: eval-scan
        TestSuite {
            query: "select a from t where b > 1",
            index: "select b, c from t",
            is_matched: false,
            ..Default::default()
        },
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t",
            is_matched: true,
            _rewritten_selection: vec!["index_col_0 (#0)"],
            _rewritten_filter: vec!["gt(index_col_1 (#1), 1)"],
        },
        // query: eval-agg-eval-scan, index: eval-scan
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select a from t",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-agg-eval-filter-scan, index: eval-scan
        TestSuite {
            query: "select sum(a) from t where a > 1 group by b",
            index: "select a from t",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-scan, index: eval-filter-scan
        TestSuite {
            query: "select a from t",
            index: "select a from t where b > 1",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t where b > 2",
            is_matched: false,
            ..Default::default()
        },
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t where b > 0",
            is_matched: true,
            _rewritten_selection: vec!["index_col_0 (#0)"],
            _rewritten_filter: vec!["gt(index_col_1 (#1), 1)"],
        },
        TestSuite {
            query: "select a from t where b < 5",
            index: "select a, b from t where b > 0",
            is_matched: false,
            ..Default::default()
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5",
            index: "select a, b from t where b > 0",
            is_matched: true,
            _rewritten_selection: vec!["index_col_0 (#0)"],
            _rewritten_filter: vec!["gt(index_col_1 (#1), 1)", "lt(index_col_1 (#1), 5)"],
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5",
            index: "select a, b from t where b > 0 and b < 6",
            is_matched: true,
            _rewritten_selection: vec!["index_col_0 (#0)"],
            _rewritten_filter: vec!["gt(index_col_1 (#1), 1)", "lt(index_col_1 (#1), 5)"],
        },
        TestSuite {
            query: "select a from t where b > 1 and a + 1 = c",
            index: "select a, b from t where a + 1 = c",
            is_matched: true,
            _rewritten_selection: vec!["index_col_0 (#0)"],
            _rewritten_filter: vec!["gt(index_col_1 (#1), 1)"],
        },
        TestSuite {
            query: "select a from t where b > 1 and a + 1 = c",
            index: "select a, b from t where b > 1 and a + 2 = c",
            is_matched: false,
            ..Default::default()
        },
        TestSuite {
            query: "select a from t where b > 1 and a + 1 = c",
            index: "select a, b from t where b > 1 and a + 1 = c",
            is_matched: true,
            _rewritten_selection: vec!["index_col_0 (#0)"],
            _rewritten_filter: vec![],
        },
        // query: eval-agg-eval-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select a from t where b > 1",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-agg-eval-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t where b > 1 group by b",
            index: "select a from t where b > 1",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select b from t",
            index: "select b, sum(a) from t group by b",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select b from t where c > 1",
            index: "select b, sum(a) from t group by b",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-agg-eval-scan, index: eval-agg-eval-scan
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-scan

        // query: eval-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select b from t",
            index: "select b, sum(a) from t where a > 1 group by b",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-filter-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select b from t where a > 1",
            index: "select b, sum(a) from t where a > 1 group by b",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-agg-eval-scan, index: eval-agg-eval-filter-scan
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-filter-scan
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_rewrite() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let create_table_plan = create_table_plan(&fixture);
    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(ctx.clone()).await?;

    let test_suites = get_test_suites();
    for suite in test_suites {
        let query = plan_sql(ctx.clone(), suite.query).await?;
        let index = plan_sql(ctx.clone(), suite.index).await?;
        let result = query_rewrite::rewrite(&query, &vec![index])?;
        assert_eq!(
            suite.is_matched,
            result.is_some(),
            "query: {}, index: {}",
            suite.query,
            suite.index
        );
        if let Some(result) = result {
            let agg_index = find_push_down_index_info(&result)?;
            assert!(agg_index.is_some());
            let agg_index = agg_index.as_ref().unwrap();
            let selection = format_selection(agg_index);
            let predicates = format_filter(agg_index);
            println!(
                "rewritten_selection: vec!{:?}, rewritten_filter: vec!{:?}",
                selection, predicates
            );
        }
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

fn find_push_down_index_info(s_expr: &SExpr) -> Result<&Option<AggIndexInfo>> {
    match s_expr.plan() {
        RelOperator::Scan(scan) => Ok(&scan.agg_index),
        _ => find_push_down_index_info(s_expr.child(0)?),
    }
}

fn format_selection(info: &AggIndexInfo) -> Vec<String> {
    info.selection
        .iter()
        .map(|item| common_sql::format_scalar(&item.scalar))
        .collect()
}

fn format_filter(info: &AggIndexInfo) -> Vec<String> {
    info.predicates
        .iter()
        .map(|scalar| common_sql::format_scalar(&scalar))
        .collect()
}
