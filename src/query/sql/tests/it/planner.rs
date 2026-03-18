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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::AggIndexPlan;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::AggIndexInfo;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql_test_support::TestCase;
use databend_common_sql_test_support::TestCaseRunner;
use databend_common_sql_test_support::TestSuite;
use databend_common_sql_test_support::TestSuiteMints;
use databend_common_sql_test_support::run_test_case_core;

mod fixture;
pub(crate) use self::fixture::LiteTableContext;

struct LiteRunner(Arc<LiteTableContext>);

struct AggIndexLiteRunner {
    ctx: Arc<LiteTableContext>,
    agg_index_table: &'static str,
    agg_index_sqls: &'static [&'static str],
}

struct AggIndexLiteCase {
    case: TestCase,
    agg_index_table: &'static str,
    agg_index_sqls: &'static [&'static str],
    is_matched: bool,
    index_selection: &'static [&'static str],
    rewritten_predicates: &'static [&'static str],
}

struct LiteReplayCaseSpec {
    name: &'static str,
    warehouse_distribution: bool,
    optimizer_skip_list: &'static [&'static str],
    default_node_num: u64,
}

impl LiteReplayCaseSpec {
    fn matches(&self, case: &TestCase) -> bool {
        case.stem == self.name || case.name == self.name
    }

    fn configure(&self, ctx: &Arc<LiteTableContext>, case: &TestCase) -> Result<()> {
        ctx.configure_for_optimizer_case(case.auto_stats)?;
        ctx.set_table_warehouse_distribution(self.warehouse_distribution);

        if !self.optimizer_skip_list.is_empty() {
            ctx.get_settings()
                .set_optimizer_skip_list(self.optimizer_skip_list.join(","))?;
        }

        ctx.set_cluster_node_num(case.node_num.unwrap_or(self.default_node_num));
        Ok(())
    }
}

const LITE_REPLAY_CASE_SPECS: &[LiteReplayCaseSpec] = &[
    LiteReplayCaseSpec {
        name: "01_cross_join_aggregation",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "01_multi_join_avg_case_expression",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "01_multi_join_sum_case_expression",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "Q01",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "Q03",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "eager_q0",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "eager_q1",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "eager_q2",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "eager_q3",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
];

impl TestCaseRunner for LiteRunner {
    async fn bind_sql(&self, sql: &str) -> Result<databend_common_sql::plans::Plan> {
        self.0.bind_sql(sql).await
    }

    async fn optimize_plan(
        &self,
        plan: databend_common_sql::plans::Plan,
    ) -> Result<databend_common_sql::plans::Plan> {
        self.0.optimize_plan(plan).await
    }
}

impl TestCaseRunner for AggIndexLiteRunner {
    async fn bind_sql(&self, sql: &str) -> Result<Plan> {
        self.ctx.bind_sql(sql).await
    }

    async fn optimize_plan(&self, plan: Plan) -> Result<Plan> {
        if let Plan::Query { metadata, .. } = &plan {
            let mut agg_index_plans = Vec::with_capacity(self.agg_index_sqls.len());
            for (index_id, sql) in self.agg_index_sqls.iter().enumerate() {
                let index_plan = self.ctx.bind_sql(sql).await?;
                let Plan::Query { s_expr, .. } = index_plan else {
                    unreachable!("agg index sql must bind to a query plan");
                };
                agg_index_plans.push(AggIndexPlan {
                    index_id: index_id as u64,
                    sql: sql.to_string(),
                    s_expr: *s_expr,
                });
            }
            metadata.write().add_agg_indices(
                format!("default.default.{}", self.agg_index_table),
                agg_index_plans,
            );
        }

        self.ctx.optimize_plan(plan).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lite_replay_service_optimizer_cases() -> Result<()> {
    let suite = TestSuite::new(
        TestSuite::optimizer_data_dir(),
        std::env::var("TEST_SUBDIR").ok(),
    );
    let mut mints = suite.create_mints();

    for (case, spec) in suite.load_cases()?.into_iter().filter_map(|case| {
        LITE_REPLAY_CASE_SPECS
            .iter()
            .find(|spec| spec.matches(&case))
            .map(|spec| (case, spec))
    }) {
        let ctx = LiteTableContext::create().await?;
        run_test_case(&ctx, &case, spec, &mut mints).await?;
    }
    Ok(())
}

async fn setup_tables(ctx: &Arc<LiteTableContext>, case: &TestCase) -> Result<()> {
    for sql in case.tables.values() {
        for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
            ctx.register_table_sql(statement).await?;
        }
    }
    Ok(())
}

async fn run_test_case(
    ctx: &Arc<LiteTableContext>,
    case: &TestCase,
    spec: &LiteReplayCaseSpec,
    mints: &mut TestSuiteMints,
) -> Result<()> {
    spec.configure(ctx, case)?;
    setup_tables(ctx, case).await?;

    let runner = LiteRunner(ctx.clone());
    run_test_case_core(case, mints.mint_for(case), &runner).await?;
    Ok(())
}

fn agg_index_test_case(name: &'static str, sql: &'static str) -> TestCase {
    TestCase {
        name: name.to_string(),
        sql: sql.to_string(),
        table_stats: HashMap::new(),
        column_stats: HashMap::new(),
        auto_stats: false,
        stem: name.to_string(),
        subdir: None,
        node_num: None,
        tables: HashMap::from([(
            "t".to_string(),
            "create table t(a int, b int, c int)".to_string(),
        )]),
    }
}

fn agg_index_test_cases() -> Vec<AggIndexLiteCase> {
    vec![
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_expression_exact_match",
                "select a + 1 from t where b > 1",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select a + 1, b from t where b > 0"],
            is_matched: true,
            index_selection: &["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: &["gt(index_col_0 (#0), 1)"],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_expression_structure_mismatch",
                "select a + 1 from t where b > 1",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select a + 2, b from t where b > 0"],
            is_matched: false,
            index_selection: &[],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_expression_alias_output_match",
                "select x from (select a + 1 as x from t) s",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select a + 1 from t"],
            is_matched: true,
            index_selection: &["index_col_0 (#0)"],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_aggregate_group_mismatch",
                "select sum(a) + 1 from t group by b",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select sum(a), c from t group by c"],
            is_matched: false,
            index_selection: &[],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_aggregate_expression_output_match",
                "select sum(a) + 1 from t group by b",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select sum(a), b from t group by b"],
            is_matched: true,
            index_selection: &["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_aggregate_distinct_mismatch",
                "select count(distinct a) from t",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select count(a) from t"],
            is_matched: false,
            index_selection: &[],
            rewritten_predicates: &[],
        },
    ]
}

fn find_push_down_index_info_from_plan(plan: &Plan) -> Result<Option<&AggIndexInfo>> {
    let Plan::Query { s_expr, .. } = plan else {
        return Ok(None);
    };
    find_push_down_index_info(s_expr)
}

fn find_push_down_index_info(s_expr: &SExpr) -> Result<Option<&AggIndexInfo>> {
    match s_expr.plan() {
        RelOperator::Scan(scan) => Ok(scan.agg_index.as_ref()),
        _ => find_push_down_index_info(s_expr.child(0)?),
    }
}

fn format_selection(info: &AggIndexInfo) -> Vec<String> {
    let mut selection: Vec<_> = info
        .selection
        .iter()
        .map(|sel| databend_common_sql::format_scalar(&sel.scalar))
        .collect();
    selection.sort();
    selection
}

fn format_filter(info: &AggIndexInfo) -> Vec<String> {
    let mut predicates: Vec<_> = info
        .predicates
        .iter()
        .map(databend_common_sql::format_scalar)
        .collect();
    predicates.sort();
    predicates
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lite_agg_index_optimizer_cases() -> Result<()> {
    for test in agg_index_test_cases() {
        let ctx = LiteTableContext::create().await?;
        ctx.configure_for_optimizer_case(test.case.auto_stats)?;
        setup_tables(&ctx, &test.case).await?;

        let runner = AggIndexLiteRunner {
            ctx: ctx.clone(),
            agg_index_table: test.agg_index_table,
            agg_index_sqls: test.agg_index_sqls,
        };
        let plan = runner.bind_sql(&test.case.sql).await?;
        let optimized_plan = runner.optimize_plan(plan).await?;
        let agg_index = find_push_down_index_info_from_plan(&optimized_plan)?;

        assert_eq!(
            test.is_matched,
            agg_index.is_some(),
            "case: {}, sql: {}, indexes: {:?}",
            test.case.name,
            test.case.sql,
            test.agg_index_sqls
        );

        if let Some(agg_index) = agg_index {
            let mut expected_selection: Vec<_> = test
                .index_selection
                .iter()
                .map(|s| (*s).to_string())
                .collect();
            expected_selection.sort();
            assert_eq!(
                expected_selection,
                format_selection(agg_index),
                "case: {} selection mismatch",
                test.case.name
            );

            let mut expected_predicates: Vec<_> = test
                .rewritten_predicates
                .iter()
                .map(|s| (*s).to_string())
                .collect();
            expected_predicates.sort();
            assert_eq!(
                expected_predicates,
                format_filter(agg_index),
                "case: {} predicate mismatch",
                test.case.name
            );
        }
    }

    Ok(())
}
