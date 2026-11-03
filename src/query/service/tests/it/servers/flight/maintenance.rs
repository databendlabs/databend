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

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_catalog::session_type::SessionType;
use databend_common_version::BUILD_INFO;
use databend_query::schedulers::QueryFragmentsActions;
use databend_query::servers::flight::v1::actions::fragments_tracking_payload;
use databend_query::servers::flight::v1::packets::QueryEnv;
use databend_query::sessions::TableContextQueryIdentity;
use databend_query::test_kits::TestFixture;

/// A maintenance statement runs as such on every node, so that its reads bypass the disk cache
/// there too; a query does not. Each step of the way is checked:
/// - the coordinator puts the flag of the statement in the `QueryEnv` it sends, and in its own
///   context, under which it runs its own fragments;
/// - the flag survives the wire;
/// - a node receiving the `QueryEnv` runs `init_query_env` under it, and the context it
///   creates for the query carries it;
/// - the fragments, prepared (`init_query_fragments`) and run (`start_prepared_query`) under
///   the payload of that context, carry it.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_maintenance_reaches_the_fragments_on_every_node() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    for maintenance in [false, true] {
        let ctx = session.create_query_context(&BUILD_INFO).await?;
        let payload = if maintenance {
            ThreadTracker::maintenance_payload()
        } else {
            ThreadTracker::new_tracking_payload()
        };
        let actions = QueryFragmentsActions::create(ctx.clone());
        let env = payload.tracking(async { actions.get_query_env() }).await?;
        assert_eq!(env.maintenance, maintenance);
        assert_eq!(
            ctx.is_maintenance(),
            maintenance,
            "the coordinator's context"
        );
        let local = fragments_tracking_payload(&ctx.get_id(), &ctx);
        assert_eq!(
            local.maintenance, maintenance,
            "the coordinator's fragments"
        );

        let received: QueryEnv = serde_json::from_value(serde_json::to_value(&env)?)?;
        assert_eq!(received.maintenance, maintenance, "over the wire");
        let mem_stat = MemStat::create_child(None, 0, ParentMemStat::StaticRef(&GLOBAL_MEM_STAT));
        let init = received.tracking_payload(mem_stat, None);
        assert_eq!(init.maintenance, maintenance, "init_query_env");
        assert_eq!(init.query_id.as_deref(), Some(env.query_id.as_str()));
        let remote = received.create_query_ctx().await?;
        assert_eq!(remote.is_maintenance(), maintenance, "the remote context");
        let fragments = fragments_tracking_payload(&env.query_id, &remote);
        assert_eq!(fragments.maintenance, maintenance, "the remote fragments");
        let flag = fragments
            .tracking(async { ThreadTracker::is_maintenance() })
            .await;
        assert_eq!(flag, maintenance, "as the readers see it");
    }
    Ok(())
}

/// A `QueryEnv` without the flag, as a node of an older version sends, is not maintenance.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_env_without_the_flag_is_not_maintenance() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    let ctx = session.create_query_context(&BUILD_INFO).await?;
    let actions = QueryFragmentsActions::create(ctx.clone());
    let env = ThreadTracker::maintenance_payload()
        .tracking(async { actions.get_query_env() })
        .await?;
    let mut value = serde_json::to_value(&env)?;
    assert!(
        value
            .as_object_mut()
            .unwrap()
            .remove("maintenance")
            .is_some()
    );
    let received: QueryEnv = serde_json::from_value(value)?;
    assert!(!received.maintenance);
    Ok(())
}
