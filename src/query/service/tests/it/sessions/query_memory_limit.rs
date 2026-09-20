// Copyright 2022 Datafuse Labs.
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

use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_catalog::session_type::SessionType;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::Planner;
use databend_common_version::BUILD_INFO;
use databend_query::pipelines::memory_settings::MemorySettingsExt;
use databend_query::schedulers::QueryFragmentsActions;
use databend_query::servers::flight::v1::packets::QueryEnv;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_query_memory_hard_limit_statement_and_worker() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    let query_stat = MemStat::create_child(Some("coordinator".into()), 0, ParentMemStat::Root);
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.mem_stat = Some(query_stat.clone());
    let ctx = ThreadTracker::tracking_future_with_payload(
        async {
            let ctx = session.create_query_context(&BUILD_INFO).await?;
            Planner::new(ctx.clone())
                .plan_sql(
                    "SETTINGS (query_memory_hard_limit = 268435456, \
             query_out_of_memory_behavior = 'spilling', allow_query_exceeded_limit = 1, \
             max_query_memory_usage = 1048576, max_memory_usage = 0) SELECT 1",
                )
                .await?;
            databend_common_exception::Result::Ok(ctx)
        },
        Some(payload.into()),
    )
    .await?;

    // Installing the hard limit must not disable the lower automatic spill threshold.
    let spill = MemorySettings::from_aggregate_settings(&ctx)?;
    let spill_bytes = 2 * 1024 * 1024;
    query_stat
        .record_memory::<true>(spill_bytes, spill_bytes)
        .unwrap();
    assert!(spill.check_spill());
    query_stat.record_memory::<false>(-spill_bytes, 0).unwrap();

    let bytes = 300 * 1024 * 1024;
    let cause = query_stat.record_memory::<true>(bytes, bytes).unwrap_err();
    assert!(cause.is_hard_limit);
    assert_eq!(cause.limit, 268435456);
    assert_eq!(session.get_settings().get_query_memory_hard_limit()?, 0);

    // Workers receive the resolved statement settings without running the planner.
    let env = QueryFragmentsActions::create(ctx).get_query_env()?;
    let received: QueryEnv = serde_json::from_value(serde_json::to_value(env)?)?;
    let worker_stat = MemStat::create_child(Some("worker".into()), 0, ParentMemStat::Root);
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.mem_stat = Some(worker_stat.clone());
    let worker = ThreadTracker::tracking_future_with_payload(
        received.create_query_ctx(),
        Some(payload.into()),
    )
    .await?;
    let cause = worker_stat.record_memory::<true>(bytes, bytes).unwrap_err();
    assert!(cause.is_hard_limit);
    assert_eq!(cause.limit, 268435456);
    drop(worker);

    // A subsequent statement on the same session gets a fresh query budget.
    let next_stat = MemStat::create_child(Some("next".into()), 0, ParentMemStat::Root);
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.mem_stat = Some(next_stat.clone());
    ThreadTracker::tracking_future_with_payload(
        async {
            let next = session.create_query_context(&BUILD_INFO).await?;
            Planner::new(next).plan_sql("SELECT 1").await?;
            databend_common_exception::Result::Ok(())
        },
        Some(payload.into()),
    )
    .await?;
    next_stat.record_memory::<true>(bytes, bytes).unwrap();
    next_stat.record_memory::<false>(-bytes, 0).unwrap();
    Ok(())
}
