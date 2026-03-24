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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql_test_support::TestCase;
use databend_common_sql_test_support::TestCaseRunner;
use databend_common_sql_test_support::TestSuite;
use databend_common_sql_test_support::TestSuiteMints;
use databend_common_sql_test_support::run_test_case_core;

mod fixture;
pub(crate) use self::fixture::LiteTableContext;

struct LiteRunner(Arc<LiteTableContext>);

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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_like_escape_rejects_non_single_character_literal() -> Result<()> {
    let ctx = LiteTableContext::create().await?;

    for sql in [
        "SELECT 'a' LIKE 'a' ESCAPE ''",
        "SELECT 'a' LIKE 'a' ESCAPE 'ab'",
        "SELECT 'a' LIKE ANY ('a', 'b') ESCAPE ''",
        "SELECT 'a' LIKE ANY (SELECT 'a') ESCAPE ''",
        "SELECT 'a' LIKE ANY (SELECT 'a') ESCAPE 'ab'",
    ] {
        let err = ctx.bind_sql(sql).await.unwrap_err();
        assert_eq!(err.code(), ErrorCode::SEMANTIC_ERROR);
        assert!(
            err.message()
                .contains("LIKE ESCAPE expression must be a single character"),
            "unexpected error for `{sql}`: {}",
            err.message()
        );
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
