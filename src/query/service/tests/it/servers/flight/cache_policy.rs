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

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::session_type::SessionType;
use databend_common_version::BUILD_INFO;
use databend_query::schedulers::QueryFragmentsActions;
use databend_query::servers::flight::v1::packets::QueryEnv;
use databend_query::sessions::TableContextCachePolicy;
use databend_query::test_kits::TestFixture;

/// The cache policy of a statement is that of its fragments on every node, so that the reads
/// of a maintenance statement bypass the disk cache there too, and those of a query do not.
/// Each step of the way is checked: the coordinator puts the policy of its context in the
/// `QueryEnv` it sends, the policy survives the wire, and the context a node creates from the
/// `QueryEnv` carries it.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cache_policy_reaches_every_node() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    for policy in [
        CachePolicy::DEFAULT,
        CachePolicy::MAINTENANCE,
        CachePolicy::READ_ONLY,
    ] {
        let ctx = session.create_query_context(&BUILD_INFO).await?;
        assert_eq!(ctx.cache_policy(), CachePolicy::DEFAULT, "a new context");
        ctx.set_cache_policy(policy);
        let env = QueryFragmentsActions::create(ctx.clone()).get_query_env()?;
        assert_eq!(env.cache_policy, policy, "the coordinator's QueryEnv");

        let received: QueryEnv = serde_json::from_value(serde_json::to_value(&env)?)?;
        assert_eq!(received.cache_policy, policy, "over the wire");
        let remote = received.create_query_ctx().await?;
        assert_eq!(remote.cache_policy(), policy, "the remote context");
    }
    Ok(())
}

/// A `QueryEnv` without the policy, as a node of an older version sends, reads as queries do.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_env_without_the_policy_is_the_default() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    let ctx = session.create_query_context(&BUILD_INFO).await?;
    ctx.set_cache_policy(CachePolicy::MAINTENANCE);
    let env = QueryFragmentsActions::create(ctx.clone()).get_query_env()?;
    let mut value = serde_json::to_value(&env)?;
    assert!(
        value
            .as_object_mut()
            .unwrap()
            .remove("cache_policy")
            .is_some()
    );
    let received: QueryEnv = serde_json::from_value(value)?;
    assert_eq!(received.cache_policy, CachePolicy::DEFAULT);
    Ok(())
}
