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

use databend_common_ast::ast::Engine;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_sql::BindContext;
use databend_common_sql::MetadataRef;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use databend_common_sql::optimizer::optimizers::rule::DEFAULT_REWRITE_RULES;
use databend_common_sql::optimizer::optimizers::rule::RuleID;
use databend_common_sql::plans::AggIndexInfo;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;

use super::test_utils::raw_plan;

#[derive(Default)]
struct TestSuite {
    // Test cases
    query: &'static str,
    index: &'static str,
    // Expected results
    is_matched: bool,
    index_selection: Vec<&'static str>,
    rewritten_predicates: Vec<&'static str>,
}

fn create_table_plan(fixture: &TestFixture, format: &str) -> CreateTablePlan {
    CreateTablePlan {
        create_option: CreateOption::Create,
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
        engine_options: Default::default(),
        storage_params: None,
        options: [
            // database id is required for FUSE
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
            (OPT_KEY_STORAGE_FORMAT.to_owned(), format.to_owned()),
        ]
        .into(),
        field_comments: vec![],
        as_select: None,
        cluster_key: None,
        table_indexes: None,
        table_constraints: None,
        attached_columns: None,
        table_partition: None,
        table_properties: None,
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
            index_selection: vec!["index_col_0 (#0)"],
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
            index_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec!["gt(index_col_1 (#1), 1)", "lt(index_col_1 (#1), 5)"],
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5",
            index: "select a, b from t where b > 0 and b < 6",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec!["gt(index_col_1 (#1), 1)", "lt(index_col_1 (#1), 5)"],
        },
        TestSuite {
            query: "select a from t where b > 1 and a + 1 = c",
            index: "select a, b from t where a + 1 = c",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)"],
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
            index_selection: vec!["index_col_0 (#0)"],
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
        TestSuite {
            query: "select sum(a) from t where b > 1 group by b",
            index: "select a, b from t where b > 1",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec![],
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
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select sum(a) from t group by b",
            is_matched: false,
            ..Default::default()
        },
        TestSuite {
            query: "select sum(a) + 1, b + 1 from t group by b",
            index: "select sum(a), b from t group by b",
            is_matched: true,
            index_selection: vec![
                "index_col_0 (#0)",
                "index_col_1 (#1)",
                "plus(index_col_0 (#0), 1)",
            ],
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
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec![],
        },
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) + 1 from t where b > 1 group by b",
            index: "select b, sum(a) from t group by b",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
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
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select sum(a) + 1, b + 2 from t where b > 1 group by b",
            index: "select b, sum(a) from t where b > 0 group by b",
            is_matched: true,
            index_selection: vec![
                "index_col_0 (#0)",
                "index_col_1 (#1)",
                "plus(index_col_0 (#0), 2)",
            ],
            rewritten_predicates: vec!["gt(index_col_0 (#0), 1)"],
        },
        // query: eval-sort-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select a from t where b > 1 order by a",
            index: "select a, b from t where b > 0",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)"],
            rewritten_predicates: vec!["gt(index_col_1 (#1), 1)"],
        },
        // query: eval-sort-agg-scan, index: eval-agg-scan
        TestSuite {
            query: "select b, sum(a) from t group by b order by b",
            index: "select b, sum(a) from t group by b",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec![],
        },
        // query: eval-sort-agg-eval-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t where b > 1 group by b order by b",
            index: "select a, b from t where b > 1",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec![],
        },
        // query: eval-sort-agg-eval-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) from t group by b order by b",
            index: "select b, sum(a) from t group by b",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select sum(a) + 1, b + 1 from t group by b order by b",
            index: "select sum(a), b from t group by b",
            is_matched: true,
            index_selection: vec![
                "index_col_0 (#0)",
                "index_col_1 (#1)",
                "plus(index_col_0 (#0), 1)",
            ],
            rewritten_predicates: vec![],
        },
        // query: eval-sort-agg-eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) + 1 from t where b > 1 group by b order by b",
            index: "select b, sum(a) from t group by b",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec!["gt(index_col_0 (#0), 1)"],
        },
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b order by b",
            index: "select b, sum(a) from t group by b",
            is_matched: false,
            ..Default::default()
        },
        // query: eval-sort-agg-eval-filter-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b order by b",
            index: "select b, sum(a) from t where c > 1 group by b",
            is_matched: true,
            index_selection: vec!["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: vec![],
        },
        TestSuite {
            query: "select sum(a) + 1, b + 2 from t where b > 1 group by b order by b",
            index: "select b, sum(a) from t where b > 0 group by b",
            is_matched: true,
            index_selection: vec![
                "index_col_0 (#0)",
                "index_col_1 (#1)",
                "plus(index_col_0 (#0), 2)",
            ],
            rewritten_predicates: vec!["gt(index_col_0 (#0), 1)"],
        },
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_rewrite() -> anyhow::Result<()> {
    test_query_rewrite_impl("parquet").await?;
    test_query_rewrite_impl("native").await
}

async fn test_query_rewrite_impl(format: &str) -> Result<()> {
    let fixture = TestFixture::setup().await?;

    let ctx = fixture.new_query_ctx().await?;
    let create_table_plan = create_table_plan(&fixture, format);
    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let test_suites = get_test_suites();
    for suite in test_suites.into_iter() {
        let (index, _, _) = plan_sql(ctx.clone(), suite.index, false).await?;
        let (mut query, _, metadata) = plan_sql(ctx.clone(), suite.query, true).await?;
        {
            let mut metadata = metadata.write();
            metadata.add_agg_indices("default.default.t".to_string(), vec![(
                0,
                suite.index.to_string(),
                index,
            )]);
        }
        query.clear_applied_rules();

        let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone());
        let result = RecursiveRuleOptimizer::new(opt_ctx.clone(), &[RuleID::TryApplyAggIndex])
            .optimize_sync(&query)?;
        let agg_index = find_push_down_index_info(&result)?;
        assert_eq!(
            suite.is_matched,
            agg_index.is_some(),
            "query: {}, index: {}",
            suite.query,
            suite.index
        );
        if let Some(agg_index) = agg_index {
            let selection = format_selection(agg_index);
            assert_eq!(
                suite.index_selection, selection,
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
    ctx: Arc<QueryContext>,
    sql: &str,
    optimize: bool,
) -> Result<(SExpr, Box<BindContext>, MetadataRef)> {
    let plan = raw_plan(&ctx, sql).await?;
    let Plan::Query {
        s_expr,
        metadata,
        bind_context,
        ..
    } = plan
    else {
        unreachable!();
    };

    let s_expr = if optimize {
        let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone());
        RecursiveRuleOptimizer::new(opt_ctx.clone(), &DEFAULT_REWRITE_RULES)
            .optimize_sync(&s_expr)?
    } else {
        *s_expr
    };

    Ok((s_expr, bind_context, metadata))
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
        .map(|sel| databend_common_sql::format_scalar(&sel.scalar))
        .collect()
}

fn format_filter(info: &AggIndexInfo) -> Vec<String> {
    info.predicates
        .iter()
        .map(databend_common_sql::format_scalar)
        .collect()
}
