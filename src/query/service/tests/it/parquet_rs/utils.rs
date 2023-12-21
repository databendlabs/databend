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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::Plan;
use databend_common_sql::Planner;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;

pub async fn create_parquet2_test_fixture() -> TestFixture {
    let mut conf = ConfigBuilder::create().config();
    conf.storage.allow_insecure = true;
    let test_fixture = TestFixture::setup_with_config(&conf).await.unwrap();
    test_fixture
        .default_session()
        .get_settings()
        .set_use_parquet2(false)
        .unwrap();

    test_fixture
}

pub async fn get_data_source_plan(ctx: Arc<dyn TableContext>, sql: &str) -> Result<DataSourcePlan> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let plan = if let Plan::Query {
        s_expr,
        metadata,
        bind_context,
        ..
    } = plan
    {
        let mut builder = PhysicalPlanBuilder::new(metadata, ctx, false);
        let physcail_plan = builder.build(&s_expr, bind_context.column_set()).await?;
        physcail_plan.try_find_single_data_source().unwrap().clone()
    } else {
        unreachable!()
    };
    Ok(plan)
}
