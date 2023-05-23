// Copyright 2021 Datafuse Labs.
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
use common_sql::optimizer::agg_index;
use common_sql::optimizer::HeuristicOptimizer;
use common_sql::optimizer::SExpr;
use common_sql::plans::AggIndexInfo;
use common_sql::plans::CreateTablePlan;
use common_sql::plans::Plan;
use common_sql::plans::RelOperator;
use common_sql::BindContext;
use common_sql::Binder;
use common_sql::Metadata;
use common_sql::MetadataRef;
use common_sql::NameResolutionContext;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::test_kits::TestFixture;
use parking_lot::RwLock;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;

#[derive(Default)]
struct TestSuite {
    // Test cases
    query: &'static str,
    index: &'static str,
    // Expected results
    is_matched: bool,
    rewritten_selection: Vec<&'static str>,
    rewritten_predicates: Vec<&'static str>,
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
        //  query: eval-scan, index: eval-scan
        TestSuite {
            query: "select to_string(c + 1) from t",
            index: "select c + 1 from t",
            is_matched: true,
            rewritten_selection: vec!["to_string(index_col_0 (#0))"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select c + 1 from t",
            index: "select c + 1 from t",
            is_matched: true,
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select a from t",
            index: "select a from t",
            is_matched: true,
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select a as z from t",
            index: "select a from t",
            is_matched: true,
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select a + 1, to_string(a) from t",
            index: "select a from t",
            is_matched: true,
            rewritten_selection: vec!["plus(index_col_0 (#0), 1)", "to_string(index_col_0 (#0))"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select a + 1 as z, to_string(a) from t",
            index: "select a from t",
            is_matched: true,
            rewritten_selection: vec!["plus(index_col_0 (#0), 1)", "to_string(index_col_0 (#0))"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select b from t",
            index: "select a, b from t",
            is_matched: true,
            rewritten_selection: vec!["index_col_1 (#1)"],
            rewritten_predicates: vec![],
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
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec!["gt(index_col_1 (#1), 1)"],
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
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec!["gt(index_col_1 (#1), 1)"],
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
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec!["gt(index_col_1 (#1), 1)", "lt(index_col_1 (#1), 5)"],
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5",
            index: "select a, b from t where b > 0 and b < 6",
            is_matched: true,
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec!["gt(index_col_1 (#1), 1)", "lt(index_col_1 (#1), 5)"],
        },
        TestSuite {
            query: "select a from t where b > 1 and a + 1 = c",
            index: "select a, b from t where a + 1 = c",
            is_matched: true,
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec!["gt(index_col_1 (#1), 1)"],
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
            rewritten_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec![],
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
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select b, sum(a) from t group by b",
            is_matched: true,
            rewritten_selection: vec!["index_col_1 (#1)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select sum(a) from t group by c",
            index: "select b, sum(a) from t group by b",
            is_matched: false,
            ..Default::default()
        },
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select b, sum(a) from t group by b",
            is_matched: true,
            rewritten_selection: vec!["plus(index_col_1 (#1), 1)"],
            rewritten_predicates: vec![],
        },
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) + 1 from t where b > 1 group by b",
            index: "select b, sum(a) from t group by b",
            is_matched: true,
            rewritten_selection: vec!["plus(index_col_1 (#1), 1)"],
            rewritten_predicates: vec!["gt(index_col_0 (#0), 1)"],
        },
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b",
            index: "select b, sum(a) from t group by b",
            is_matched: false,
            ..Default::default()
        },
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
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select b, sum(a) from t where c > 1 group by b",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b",
            index: "select b, sum(a) from t where c > 1 group by b",
            is_matched: true,
            rewritten_selection: vec!["plus(index_col_1 (#1), 1)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select sum(a) + 1, b + 2 from t where b > 1 group by b",
            index: "select b, sum(a) from t where b > 0 group by b",
            is_matched: true,
            rewritten_selection: vec!["plus(index_col_1 (#1), 1)", "plus(index_col_0 (#0), 2)"],
            rewritten_predicates: vec!["gt(index_col_0 (#0), 1)"],
        },
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_rewrite() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let create_table_plan = create_table_plan(&fixture);
    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(ctx.clone()).await?;
    let func_ctx = ctx.get_function_context()?;

    let test_suites = get_test_suites();
    for suite in test_suites {
        let (query, bind_context, metadata) = plan_sql(ctx.clone(), suite.query, true).await?;
        let (index, _, _) = plan_sql(ctx.clone(), suite.index, false).await?;
        let optimzier = HeuristicOptimizer::new(func_ctx.clone(), bind_context, metadata.clone());
        let meta = metadata.read();
        let base_columns = meta.columns_by_table_index(0);
        let result = agg_index::try_rewrite(&optimzier, &base_columns, &query, &vec![(
            0,
            suite.index.to_string(),
            index,
        )])?;
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
            assert_eq!(
                suite.rewritten_selection, selection,
                "query: {}, index: {}",
                suite.query, suite.index
            );

            let predicates = format_filter(agg_index);
            assert_eq!(
                suite.rewritten_predicates, predicates,
                "query: {}, index: {}",
                suite.query, suite.index
            );
        }
    }

    Ok(())
}

async fn plan_sql(
    ctx: Arc<dyn TableContext>,
    sql: &str,
    optimize: bool,
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
        let s_expr = if optimize {
            let optimizer = HeuristicOptimizer::new(
                ctx.get_function_context()?,
                bind_context.clone(),
                metadata.clone(),
            );
            optimizer.optimize(*s_expr)?
        } else {
            *s_expr
        };

        return Ok((s_expr, bind_context, metadata));
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
        .map(common_sql::format_scalar)
        .collect()
}

fn format_filter(info: &AggIndexInfo) -> Vec<String> {
    info.predicates
        .iter()
        .map(common_sql::format_scalar)
        .collect()
}
