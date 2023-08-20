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

use common_catalog::plan::DataSourcePlan;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_meta_app::storage::StorageFsConfig;
use common_meta_app::storage::StorageParams;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::plans::Plan;
use common_sql::Planner;
use databend_query::test_kits::create_query_context_with_config;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use tempfile::TempDir;

pub async fn create_test_fixture() -> TestFixture {
    let tmp_dir = TempDir::new().unwrap();
    let mut conf = ConfigBuilder::create().config();

    // make sure we are using `fs` storage
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        // use `TempDir` as root path (auto clean)
        root: tmp_dir.path().to_str().unwrap().to_string(),
    });
    conf.storage.allow_insecure = true;

    let (_guard, ctx) = create_query_context_with_config(conf, None).await.unwrap();

    ctx.get_settings().set_use_parquet2(false).unwrap();

    TestFixture::new_with_ctx(_guard, ctx).await
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
