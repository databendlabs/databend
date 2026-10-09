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

use databend_common_catalog::table_context::TableContext;
use databend_common_sql::plans::Plan;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_send_notification_planning_is_result_free_and_uncacheable() -> anyhow::Result<()> {
    // No Cloud Control configured: planning must not try to send an RPC.
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner
        .plan_sql("CALL system$send_notification('oncall', 'alert')")
        .await?;
    assert!(!plan.has_result_set());
    assert!(matches!(plan, Plan::Query {
        ignore_result: true,
        ..
    }));
    assert!(!ctx.result_cache_state().cacheable());

    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner
        .plan_sql("SELECT * FROM send_notification('oncall', 'alert')")
        .await?;
    assert!(plan.has_result_set());
    assert!(!ctx.result_cache_state().cacheable());
    Ok(())
}
