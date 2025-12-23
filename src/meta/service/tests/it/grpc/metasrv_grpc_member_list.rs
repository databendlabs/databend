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

use std::collections::HashSet;

use databend_common_base::base::tokio::time::Duration;
use databend_common_base::base::tokio::time::sleep;
use databend_common_meta_sled_store::openraft::ServerState;
use databend_common_version::BUILD_INFO;
use databend_meta::message::ForwardRequest;
use databend_meta::message::ForwardRequestBody;
use databend_meta::message::JoinRequest;
use databend_meta::meta_service::MetaNode;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_member_list() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Get member list

    let (tc, _addr) = crate::tests::start_metasrv().await?;

    let client = tc.grpc_client().await?;

    let resp = client.get_member_list().await?;
    println!("member list: {:?}", resp);

    // The member list should contain at least one member (the current node)
    assert!(!resp.data.is_empty(), "member list should not be empty");

    // Each member address should be in the expected format (host:port)
    for member in &resp.data {
        assert!(
            member.contains(':'),
            "member address should contain port: {}",
            member
        );
        // Check that the address has valid format like "127.0.0.1:port" or "host:port"
        let parts: Vec<&str> = member.split(':').collect();
        assert_eq!(
            parts.len(),
            2,
            "member address should have host:port format: {}",
            member
        );

        // Check that port is a valid number
        let port = parts[1].parse::<u16>();
        assert!(
            port.is_ok(),
            "member address should have valid port number: {}",
            member
        );
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_member_list_with_learner() -> anyhow::Result<()> {
    // This test verifies that member_list API returns both voters and learners
    // Start with 2 voters, then add 1 learner

    // Start initial cluster with 2 voters
    let tcs = crate::tests::start_metasrv_cluster(&[0, 1]).await?;

    // Create a learner node but don't start it as part of the initial cluster
    let learner_node_id = 2;
    let learner_tc = MetaSrvTestContext::new(learner_node_id);
    let learner_mn = MetaNode::open(&learner_tc.config.raft_config, &BUILD_INFO).await?;

    // Get the leader to send join request
    let leader_tc = &tcs[0];
    let leader_mn = leader_tc
        .grpc_srv
        .as_ref()
        .unwrap()
        .meta_handle
        .clone()
        .unwrap();

    // Join the learner to the cluster
    let endpoint = learner_tc.config.raft_config.raft_api_addr().await?;
    let grpc_api_advertise_address = learner_tc
        .config
        .grpc_api_advertise_address()
        .unwrap_or_else(|| "127.0.0.1:29191".to_string());

    let admin_req = ForwardRequest {
        forward_to_leader: 0,
        body: ForwardRequestBody::Join(
            JoinRequest::new(
                learner_node_id,
                endpoint,
                Some(grpc_api_advertise_address.clone()),
            )
            .with_role_learner(),
        ),
    };

    leader_mn
        .request(|mn| {
            let fu = async move { mn.handle_forwardable_request(admin_req).await };
            Box::pin(fu)
        })
        .await??;

    // Wait for the learner to be added and reach the correct state
    sleep(Duration::from_millis(1000)).await;

    // Verify learner state
    learner_mn
        .raft
        .wait(Some(Duration::from_secs(10)))
        .state(ServerState::Learner, "learner should be in learner state")
        .await?;

    // Get member list from leader
    let client = leader_tc.grpc_client().await?;
    let resp = client.get_member_list().await?;

    println!("member list with 2 voters + 1 learner: {:?}", resp);

    // Should have exactly 3 members (2 voters + 1 learner)
    assert_eq!(
        resp.data.len(),
        3,
        "member list should contain exactly 3 members (2 voters + 1 learner), got: {}",
        resp.data.len()
    );

    // Collect expected addresses
    let mut expected_addresses = HashSet::new();
    for tc in &tcs {
        if let Some(addr) = tc.config.grpc_api_advertise_address() {
            expected_addresses.insert(addr);
        }
    }
    expected_addresses.insert(grpc_api_advertise_address.clone());

    // Verify all expected addresses are in the member list
    let member_set: HashSet<String> = resp.data.into_iter().collect();

    assert_eq!(
        expected_addresses.len(),
        3,
        "should have 3 expected addresses, got: {}",
        expected_addresses.len()
    );

    for expected_addr in &expected_addresses {
        assert!(
            member_set.contains(expected_addr),
            "member list should contain expected address: {}, got: {:?}",
            expected_addr,
            member_set
        );
    }

    Ok(())
}
