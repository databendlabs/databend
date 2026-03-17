// Copyright 2025 Datafuse Labs.
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

#![allow(clippy::replace_box)]

use std::sync::Arc;

use databend_base::uniq_id::GlobalUniq;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::Metadata;
use databend_common_sql::optimize;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::Statistics;
use databend_common_sql_test_support::TestCase;
use databend_common_sql_test_support::TestCaseRunner;
use databend_common_sql_test_support::TestSuite;
use databend_common_sql_test_support::TestSuiteMints;
use databend_common_sql_test_support::configure_optimizer_settings;
use databend_common_sql_test_support::run_test_case_core;
use databend_common_statistics::Datum;
use databend_meta_client::types::NodeInfo;
use databend_meta_types::NodeInfo;
use databend_query::clusters::ClusterHelper;
use databend_query::physical_plans::PhysicalPlanBuilder;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;

use crate::sql::planner::optimizer::test_utils::execute_sql;
use crate::sql::planner::optimizer::test_utils::raw_plan;

struct ServiceRunner(Arc<QueryContext>);

impl TestCaseRunner for ServiceRunner {
    async fn bind_sql(&self, sql: &str) -> Result<Plan> {
        raw_plan(&self.0, sql).await
    }

    async fn optimize_plan(&self, plan: Plan) -> Result<Plan> {
        let metadata = match &plan {
            Plan::Query { metadata, .. } => metadata.clone(),
            _ => Arc::new(parking_lot::RwLock::new(Metadata::default())),
        };

        let settings = self.0.get_settings();
        let opt_ctx = OptimizerContext::new(self.0.clone(), metadata)
            .with_settings(&settings)?
            .set_enable_distributed_optimization(true)
            .clone();

        optimize(opt_ctx, plan).await
    }

    async fn build_physical(&self, optimized: &Plan) -> Result<Option<String>> {
        if let Plan::Query {
            metadata,
            bind_context,
            s_expr,
            ..
        } = optimized
        {
            let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.0.clone(), false);
            let physical = builder.build(s_expr, bind_context.column_set()).await?;
            let metadata = metadata.read();
            Ok(Some(
                physical
                    .format(&metadata, Default::default())?
                    .format_pretty()?,
            ))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_optimizer() -> anyhow::Result<()> {
    let base_path = TestSuite::optimizer_data_dir();
    let subdir = std::env::var("TEST_SUBDIR").ok();

    let suite = TestSuite::new(base_path.clone(), subdir);
    let fixture = TestFixture::setup().await?;

    let cases = suite.load_cases()?;
    if cases.is_empty() {
        return Ok(());
    }

    let mut mints = suite.create_mints();

    let local_id = GlobalUniq::unique();
    let standalone_cluster = Cluster::create(vec![create_node(&local_id)], local_id);

    for case in cases {
        println!("\n========== Testing: {} ==========", case.name);

        let ctx = fixture.new_query_ctx().await?;
        ctx.get_settings().set_enable_auto_materialize_cte(0)?;

        ctx.set_cluster(standalone_cluster.clone());

        if let Some(nodes) = case.node_num {
            let mut nodes_info = Vec::with_capacity(nodes as usize);
            for _ in 0..nodes - 1 {
                nodes_info.push(create_node(&GlobalUniq::unique()));
            }

            let local_id = GlobalUniq::unique();
            nodes_info.push(create_node(&local_id));
            ctx.set_cluster(Cluster::create(nodes_info, local_id))
        }

        setup_tables(&ctx, &case).await?;
        run_test_case(&ctx, &case, &mut mints).await?;

        clean_tables(&ctx, &case).await?;
        ctx.set_cluster(standalone_cluster.clone());
        println!("✅ {} test passed!", case.name);
    }

    Ok(())
}

fn create_node(local_id: &str) -> Arc<NodeInfo> {
    let mut node_info = NodeInfo::create(
        local_id.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
    );
    node_info.cluster_id = "cluster_id".to_string();
    node_info.warehouse_id = "warehouse_id".to_string();
    Arc::new(node_info)
}

async fn setup_tables(ctx: &Arc<QueryContext>, case: &TestCase) -> Result<()> {
    for sql in case.tables.values() {
        for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
            match execute_sql(ctx, statement).await {
                Ok(_) => {}
                Err(e) if e.code() == ErrorCode::TABLE_ALREADY_EXISTS => {
                    // Ignore table already exists errors
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(())
}

async fn clean_tables(ctx: &Arc<QueryContext>, case: &TestCase) -> Result<()> {
    for table_name in case.tables.keys() {
        execute_sql(ctx, &format!("DROP TABLE {}", table_name)).await?;
    }
    Ok(())
}

async fn run_test_case(
    ctx: &Arc<QueryContext>,
    case: &TestCase,
    mints: &mut TestSuiteMints,
) -> Result<()> {
    let settings = ctx.get_settings();
    configure_optimizer_settings(settings.as_ref(), case.auto_stats)?;

    let runner = ServiceRunner(ctx.clone());
    run_test_case_core(case, mints.mint_for(case), &runner).await?;

    Ok(())
}
