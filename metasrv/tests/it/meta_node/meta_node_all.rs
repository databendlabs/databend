// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeSet;
use std::sync::Arc;

use common_base::tokio;
use common_base::tokio::time::Duration;
use common_meta_api::KVApi;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::LogIdOptionExt;
use common_meta_sled_store::openraft::RaftMetrics;
use common_meta_sled_store::openraft::State;
use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::DatabaseMeta;
use common_meta_types::ForwardToLeader;
use common_meta_types::LogEntry;
use common_meta_types::MatchSeq;
use common_meta_types::MetaError;
use common_meta_types::MetaRaftError;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::RetryableError;
use common_meta_types::SeqV;
use common_tracing::tracing;
use databend_meta::configs;
use databend_meta::meta_service::meta_leader::MetaLeader;
use databend_meta::meta_service::ForwardRequest;
use databend_meta::meta_service::ForwardRequestBody;
use databend_meta::meta_service::JoinRequest;
use databend_meta::meta_service::MetaNode;
use databend_meta::Opened;
use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::init_meta_ut;
use crate::tests::service::MetaSrvTestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_boot() -> anyhow::Result<()> {
    // - Start a single node meta service cluster.
    // - Test the single node is recorded by this cluster.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = MetaSrvTestContext::new(0);
    let addr = tc.config.raft_config.raft_api_addr().await?;

    let mn = MetaNode::boot(&tc.config.raft_config).await?;

    let got = mn.get_node(&0).await?;
    assert_eq!(addr, got.unwrap().address);
    mn.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_graceful_shutdown() -> anyhow::Result<()> {
    // - Start a leader then shutdown.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_nid0, tc) = start_meta_node_leader().await?;
    let mn0 = tc.meta_node();

    let mut rx0 = mn0.raft.metrics();

    let joined = mn0.stop().await?;
    assert_eq!(3, joined);

    // tx closed:
    loop {
        let r = rx0.changed().await;
        if r.is_err() {
            tracing::info!("done!!!");
            break;
        }

        tracing::info!("st: {:?}", rx0.borrow());
    }
    assert!(rx0.changed().await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_leader_and_non_voter() -> anyhow::Result<()> {
    // - Start a leader and a non-voter;
    // - Write to leader, check on non-voter.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    {
        let span = tracing::span!(tracing::Level::DEBUG, "test_meta_node_leader_and_non_voter");
        let _ent = span.enter();

        let (_nid0, tc0) = start_meta_node_leader().await?;
        let mn0 = tc0.meta_node();

        let (_nid1, tc1) = start_meta_node_non_voter(mn0.clone(), 1).await?;
        let mn1 = tc1.meta_node();

        assert_upsert_kv_synced(vec![mn0.clone(), mn1.clone()], "metakey2").await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_write_to_local_leader() -> anyhow::Result<()> {
    // - Start a leader, 2 followers and a non-voter;
    // - Write to the raft node on the leader, expect Ok.
    // - Write to the raft node on the non-leader, expect ForwardToLeader error.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    {
        let span = tracing::span!(tracing::Level::DEBUG, "test_meta_node_leader_and_non_voter");
        let _ent = span.enter();

        let (mut _nlog, tcs) = start_meta_node_cluster(btreeset![0, 1, 2], btreeset![3]).await?;
        let all = test_context_nodes(&tcs);

        let leader_id = all[0].raft.metrics().borrow().current_leader.unwrap();

        // test writing to leader and non-leader
        let key = "t-non-leader-write";
        for id in 0u64..4 {
            let mn = &all[id as usize];
            let maybe_leader = MetaLeader::new(mn);
            let rst = maybe_leader
                .write(LogEntry {
                    txid: None,
                    cmd: Cmd::UpsertKV {
                        key: key.to_string(),
                        seq: MatchSeq::Any,
                        value: Operation::Update(key.to_string().into_bytes()),
                        value_meta: None,
                    },
                })
                .await;

            if id == leader_id {
                assert!(rst.is_ok());
            } else {
                assert!(rst.is_err());
                let e = rst.unwrap_err();
                match e {
                    MetaError::MetaRaftError(MetaRaftError::ForwardToLeader(ForwardToLeader {
                        leader_id: forward_leader_id,
                    })) => {
                        assert_eq!(Some(leader_id), forward_leader_id);
                    }
                    _ => {
                        panic!("expect MetaRaftError::ForwardToLeader")
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_add_database() -> anyhow::Result<()> {
    // - Start a leader, 2 followers and a non-voter;
    // - Assert that every node handles AddDatabase request correctly.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    let tenant = "tenant1";

    {
        let (_nlog, all_tc) = start_meta_node_cluster(btreeset![0, 1, 2], btreeset![3]).await?;
        let all = all_tc.iter().map(|tc| tc.meta_node()).collect::<Vec<_>>();

        // ensure cluster works
        assert_upsert_kv_synced(all.clone(), "foo").await?;

        // - db name to create
        // - expected db id
        let cases: Vec<(&str, u64)> = vec![("foo", 1), ("bar", 2), ("foo", 1), ("bar", 2)];

        // Sending AddDatabase request to any node is ok.
        for (i, (name, want_id)) in cases.iter().enumerate() {
            let mn = &all[i as usize];

            let last_applied = mn.raft.metrics().borrow().last_applied;

            let rst = mn
                .write(LogEntry {
                    txid: None,
                    cmd: Cmd::CreateDatabase {
                        tenant: tenant.to_string(),
                        name: name.to_string(),
                        meta: DatabaseMeta {
                            engine: "default".to_string(),
                            ..Default::default()
                        },
                    },
                })
                .await;

            assert!(rst.is_ok());

            // No matter if a db is created, the log that tries to create db always applies.
            assert_applied_index(all.clone(), last_applied.next_index()).await?;

            for (i, mn) in all.iter().enumerate() {
                let got = mn
                    .get_state_machine()
                    .await
                    .get_database_id(tenant, name.as_ref())?;

                assert_eq!(*want_id, got, "n{} applied AddDatabase", i);
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_snapshot_replication() -> anyhow::Result<()> {
    // - Bring up a cluster of 3.
    // - Write just enough logs to trigger a snapshot.
    // - Add a non-voter, test the snapshot is sync-ed
    // - Write logs to trigger another snapshot.
    // - Add

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    // Create a snapshot every 10 logs
    let snap_logs = 10;

    let mut tc = MetaSrvTestContext::new(0);
    tc.config.raft_config.snapshot_logs_since_last = snap_logs;
    tc.config.raft_config.install_snapshot_timeout = 10_1000; // milli seconds. In a CI multi-threads test delays async task badly.
    tc.config.raft_config.max_applied_log_to_keep = 0;

    let mn = MetaNode::boot(&tc.config.raft_config).await?;

    tc.assert_raft_server_connection().await?;

    wait_for_state(&mn, State::Leader).await?;
    wait_for_current_leader(&mn, 0).await?;

    // initial membership, leader blank log, add node.
    let mut log_index = 2;

    mn.raft
        .wait(timeout())
        .log(Some(log_index), "leader init logs")
        .await?;

    let n_req = 12;

    for i in 0..n_req {
        let key = format!("test_meta_node_snapshot_replication-key-{}", i);
        mn.write(LogEntry {
            txid: None,
            cmd: Cmd::UpsertKV {
                key: key.clone(),
                seq: MatchSeq::Any,
                value: Some(b"v".to_vec()).into(),
                value_meta: None,
            },
        })
        .await?;
    }
    log_index += n_req;

    tracing::info!("--- check the log is locally applied");

    mn.raft
        .wait(timeout())
        .log(Some(log_index), "applied on leader")
        .await?;

    tracing::info!("--- check the snapshot is created");

    mn.raft
        .wait(timeout())
        .metrics(
            |x| x.snapshot.map(|x| x.term) == Some(1) && x.snapshot.next_index() >= snap_logs,
            "snapshot is created by leader",
        )
        .await?;

    tracing::info!("--- start a non_voter to receive snapshot replication");

    let (_, tc1) = start_meta_node_non_voter(mn.clone(), 1).await?;
    log_index += 1;

    let mn1 = tc1.meta_node();

    mn1.raft
        .wait(timeout())
        .log(Some(log_index), "non-voter replicated all logs")
        .await?;

    mn1.raft
        .wait(timeout())
        .metrics(
            |x| x.snapshot.map(|x| x.term) == Some(1) && x.snapshot.next_index() >= snap_logs,
            "snapshot is received by non-voter",
        )
        .await?;

    for i in 0..n_req {
        let key = format!("test_meta_node_snapshot_replication-key-{}", i);
        let got = mn1.get_kv(&key).await?;
        match got {
            None => {
                panic!("expect get some value for {}", key)
            }
            Some(SeqV { ref data, .. }) => {
                assert_eq!(data, b"v");
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_join() -> anyhow::Result<()> {
    // - Bring up a cluster
    // - Join a new node by sending a Join request to leader.
    // - Join a new node by sending a Join request to a non-voter.
    // - Restart all nodes and check if states are restored.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let span = tracing::span!(tracing::Level::INFO, "test_meta_node_join");
    let _ent = span.enter();

    let (mut _nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![1]).await?;
    let mut all = test_context_nodes(&tcs);
    let tc0 = tcs.remove(0);
    let tc1 = tcs.remove(0);

    tracing::info!("--- bring up non-voter 2");

    let node_id = 2;
    let tc2 = MetaSrvTestContext::new(node_id);
    let mn2 = MetaNode::open_create_boot(&tc2.config.raft_config, None, Some(()), None).await?;

    tracing::info!("--- join non-voter 2 to cluster by leader");

    let leader_id = all[0].get_leader().await;
    let leader = all[leader_id as usize].clone();

    let admin_req = join_req(node_id, tc2.config.raft_config.raft_api_addr().await?, 0);
    leader.handle_forwardable_request(admin_req).await?;

    all.push(mn2.clone());

    tracing::info!("--- check all nodes has node-3 joined");
    {
        for mn in all.iter() {
            mn.raft
                .wait(timeout())
                .members(btreeset! {0,2}, format!("node-2 is joined: {}", mn.sto.id))
                .await?;
        }
    }

    tracing::info!("--- bring up non-voter 3");

    let node_id = 3;
    let tc3 = MetaSrvTestContext::new(node_id);
    let mn3 = MetaNode::open_create_boot(&tc3.config.raft_config, None, Some(()), None).await?;

    tracing::info!("--- join node-3 by sending rpc `join` to a non-leader");
    {
        let to_addr = tc1.config.raft_config.raft_api_addr().await?;

        let mut client = RaftServiceClient::connect(format!("http://{}", to_addr)).await?;
        let admin_req = join_req(node_id, tc3.config.raft_config.raft_api_addr().await?, 1);
        client.forward(admin_req).await?;
    }

    tracing::info!("--- check all nodes has node-3 joined");

    all.push(mn3.clone());
    for mn in all.iter() {
        mn.raft
            .wait(timeout())
            .members(
                btreeset! {0,2,3},
                format!("node-3 is joined: {}", mn.sto.id),
            )
            .await?;
    }

    tracing::info!("--- stop all meta node");

    for mn in all.drain(..) {
        mn.stop().await?;
    }

    tracing::info!("--- re-open all meta node");

    let mn0 = MetaNode::open_create_boot(&tc0.config.raft_config, Some(()), None, None).await?;
    let mn1 = MetaNode::open_create_boot(&tc1.config.raft_config, Some(()), None, None).await?;
    let mn2 = MetaNode::open_create_boot(&tc2.config.raft_config, Some(()), None, None).await?;
    let mn3 = MetaNode::open_create_boot(&tc3.config.raft_config, Some(()), None, None).await?;

    let all = vec![mn0, mn1, mn2, mn3];

    tracing::info!("--- check reopened memberships");

    for mn in all.iter() {
        mn.raft
            .wait(timeout())
            .members(btreeset! {0,2,3}, format!("node-{} membership", mn.sto.id))
            .await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_join_rejoin() -> anyhow::Result<()> {
    // - Bring up a cluster
    // - Join a new node.
    // - Join another new node twice.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let span = tracing::span!(tracing::Level::INFO, "test_meta_node_join_rejoin");
    let _ent = span.enter();

    let (mut _nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![]).await?;
    let mut all = test_context_nodes(&tcs);
    let _tc0 = tcs.remove(0);

    tracing::info!("--- bring up non-voter 1");

    let node_id = 1;
    let tc1 = MetaSrvTestContext::new(node_id);
    let mn1 = MetaNode::open_create_boot(&tc1.config.raft_config, None, Some(()), None).await?;

    tracing::info!("--- join non-voter 1 to cluster");

    let leader_id = all[0].get_leader().await;
    let leader = all[leader_id as usize].clone();
    let req = join_req(node_id, tc1.config.raft_config.raft_api_addr().await?, 1);
    leader.handle_forwardable_request(req).await?;

    all.push(mn1.clone());

    tracing::info!("--- check all nodes has node-1 joined");
    {
        for mn in all.iter() {
            mn.raft
                .wait(timeout())
                .members(btreeset! {0,1}, format!("node-1 is joined: {}", mn.sto.id))
                .await?;
        }
    }

    tracing::info!("--- bring up non-voter 3");

    let node_id = 2;
    let tc2 = MetaSrvTestContext::new(node_id);
    let mn2 = MetaNode::open_create_boot(&tc2.config.raft_config, None, Some(()), None).await?;

    tracing::info!("--- join node-2 by sending rpc `join` to a non-leader");
    {
        let req = join_req(node_id, tc2.config.raft_config.raft_api_addr().await?, 1);
        leader.handle_forwardable_request(req).await?;
    }
    tracing::info!("--- join node-2 again");
    {
        let req = join_req(node_id, tc2.config.raft_config.raft_api_addr().await?, 1);
        mn1.handle_forwardable_request(req).await?;
    }

    all.push(mn2.clone());

    tracing::info!("--- check all nodes has node-3 joined");

    for mn in all.iter() {
        mn.raft
            .wait(timeout())
            .members(
                btreeset! {0,1,2},
                format!("node-2 is joined: {}", mn.sto.id),
            )
            .await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_restart() -> anyhow::Result<()> {
    // TODO check restarted follower.
    // - Start a leader and a non-voter;
    // - Restart them.
    // - Check old data an new written data.

    // TODO(xp): this only tests for in-memory storage.
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_nid0, tc0) = start_meta_node_leader().await?;
    let mn0 = tc0.meta_node();

    let (_nid1, tc1) = start_meta_node_non_voter(mn0.clone(), 1).await?;
    let mn1 = tc1.meta_node();

    let sto0 = mn0.sto.clone();
    let sto1 = mn1.sto.clone();

    let meta_nodes = vec![mn0.clone(), mn1.clone()];

    assert_upsert_kv_synced(meta_nodes.clone(), "key1").await?;

    // stop
    tracing::info!("shutting down all");

    let n = mn0.stop().await?;
    assert_eq!(3, n);
    let n = mn1.stop().await?;
    assert_eq!(3, n);

    tracing::info!("restart all");

    // restart
    let config = configs::Config::empty();
    let mn0 = MetaNode::builder(&config.raft_config)
        .node_id(0)
        .sto(sto0)
        .build()
        .await?;
    let mn1 = MetaNode::builder(&config.raft_config)
        .node_id(1)
        .sto(sto1)
        .build()
        .await?;

    let meta_nodes = vec![mn0.clone(), mn1.clone()];

    wait_for_state(&mn0, State::Leader).await?;
    wait_for_state(&mn1, State::Learner).await?;
    wait_for_current_leader(&mn1, 0).await?;

    assert_upsert_kv_synced(meta_nodes.clone(), "key2").await?;

    // check old data
    assert_get_kv(meta_nodes, "key1", "key1").await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_restart_single_node() -> anyhow::Result<()> {
    // TODO(xp): This function will replace `test_meta_node_restart` after disk backed state machine is ready.

    // Test disk backed meta node restart.
    // - Start a cluster of a solo leader;
    // - Write one log.
    // - Restart.
    // - Check node state:
    //   - raft hard state
    //   - raft logs.
    //   - state machine:
    //     - Nodes
    //   - TODO(xp): snapshot is empty, since snapshot is not persisted in this version see `MetaStore`.
    // - Check cluster:
    //   - Leader is elected.
    //   - TODO(xp): Leader starts replication to follower and non-voter.
    //   - TODO(xp): New log will be successfully written and sync
    //   - TODO(xp): A new snapshot will be created and transferred  on demand.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let mut log_index: u64 = 0;
    let (_id, tc) = start_meta_node_leader().await?;
    // initial membeship, leader blank, add node
    log_index += 2;

    let want_hs;
    {
        let leader = tc.meta_node();

        leader
            .as_leader()
            .await?
            .write(LogEntry {
                txid: None,
                cmd: Cmd::UpsertKV {
                    key: "foo".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"1".to_vec()),
                    value_meta: None,
                },
            })
            .await?;
        log_index += 1;

        want_hs = leader.sto.raft_state.read_hard_state()?;

        leader.stop().await?;
    }

    tracing::info!("--- reopen MetaNode");

    let leader = MetaNode::open_create_boot(&tc.config.raft_config, Some(()), None, None).await?;

    log_index += 1;

    wait_for_state(&leader, State::Leader).await?;
    wait_for_log(&leader, log_index as u64).await?;

    tracing::info!("--- check hard state");
    {
        let hs = leader.sto.raft_state.read_hard_state()?;
        assert_eq!(want_hs, hs);
    }

    tracing::info!("--- check logs");
    {
        let logs = leader.sto.log.range_values(..)?;
        tracing::info!("logs: {:?}", logs);
        assert_eq!(log_index as usize + 1, logs.len());
    }

    tracing::info!("--- check state machine: nodes");
    {
        let node = leader.sto.get_node(&0).await?.unwrap();
        assert_eq!(tc.config.raft_config.raft_api_addr().await?, node.address);
    }

    Ok(())
}

/// Setup a cluster with several voter and several non_voter
/// The node id 0 must be in `voters` and node 0 is elected as leader.
pub(crate) async fn start_meta_node_cluster(
    voters: BTreeSet<NodeId>,
    non_voters: BTreeSet<NodeId>,
) -> anyhow::Result<(u64, Vec<MetaSrvTestContext>)> {
    // TODO(xp): use setup_cluster if possible in tests. Get rid of boilerplate snippets.
    // leader is always node-0
    assert!(voters.contains(&0));
    assert!(!non_voters.contains(&0));

    let mut test_contexts = vec![];

    let (_id, tc0) = start_meta_node_leader().await?;
    let leader = tc0.meta_node();
    test_contexts.push(tc0);

    // membership log, blank log and add node
    let mut log_index = 2;
    wait_for_log(&leader, log_index).await?;

    for id in voters.iter() {
        // leader is already created.
        if *id == 0 {
            continue;
        }
        let (_id, tc) = start_meta_node_non_voter(leader.clone(), *id).await?;

        // Adding a node
        log_index += 1;
        tc.meta_node()
            .raft
            .wait(timeout())
            .log(Some(log_index), format!("add :{}", id))
            .await?;

        test_contexts.push(tc);
    }

    for id in non_voters.iter() {
        let (_id, tc) = start_meta_node_non_voter(leader.clone(), *id).await?;

        // Adding a node
        log_index += 1;

        tc.meta_node()
            .raft
            .wait(timeout())
            .log(Some(log_index), format!("add :{}", id))
            .await?;
        // wait_for_log(&tc.meta_nodes[0], log_index).await?;

        test_contexts.push(tc);
    }

    if voters != btreeset! {0} {
        leader.raft.change_membership(voters.clone(), true).await?;
        log_index += 2;
    }

    tracing::info!("--- check node roles");
    {
        wait_for_state(&leader, State::Leader).await?;

        for item in test_contexts.iter().take(voters.len()).skip(1) {
            wait_for_state(&item.meta_node(), State::Follower).await?;
        }
        for item in test_contexts
            .iter()
            .skip(voters.len())
            .take(non_voters.len())
        {
            wait_for_state(&item.meta_node(), State::Learner).await?;
        }
    }

    tracing::info!("--- check node logs");
    {
        for tc in &test_contexts {
            wait_for_log(&tc.meta_node(), log_index).await?;
        }
    }

    Ok((log_index, test_contexts))
}

pub(crate) async fn start_meta_node_leader() -> anyhow::Result<(NodeId, MetaSrvTestContext)> {
    // Setup a cluster in which there is a leader and a non-voter.
    // asserts states are consistent

    let nid = 0;
    let mut tc = MetaSrvTestContext::new(nid);
    let addr = tc.config.raft_config.raft_api_addr().await?;

    // boot up a single-node cluster
    let mn = MetaNode::boot(&tc.config.raft_config).await?;
    tc.meta_node = Some(mn.clone());

    {
        tc.assert_raft_server_connection().await?;

        // assert that boot() adds the node to meta.
        let got = mn.get_node(&nid).await?;
        assert_eq!(addr, got.unwrap().address, "nid0 is added");

        wait_for_state(&mn, State::Leader).await?;
        wait_for_current_leader(&mn, 0).await?;
    }
    Ok((nid, tc))
}

/// Start a NonVoter and setup replication from leader to it.
/// Assert the NonVoter is ready and upto date such as the known leader, state and grpc service.
async fn start_meta_node_non_voter(
    leader: Arc<MetaNode>,
    id: NodeId,
) -> anyhow::Result<(NodeId, MetaSrvTestContext)> {
    let mut tc = MetaSrvTestContext::new(id);
    let addr = tc.config.raft_config.raft_api_addr().await?;

    let raft_config = tc.config.raft_config.clone();

    let mn = MetaNode::open_create_boot(&raft_config, None, Some(()), None).await?;
    assert!(!mn.is_opened());

    tc.meta_node = Some(mn.clone());

    {
        // add node to cluster as a non-voter
        let resp = leader.add_node(id, addr.clone()).await?;
        match resp {
            AppliedState::Node { prev: _, result } => {
                assert_eq!(addr.clone(), result.unwrap().address);
            }
            _ => {
                panic!("expect node")
            }
        }
    }

    {
        tc.assert_raft_server_connection().await?;
        wait_for_state(&mn, State::Learner).await?;
        wait_for_current_leader(&mn, 0).await?;
    }

    Ok((id, tc))
}

fn join_req(node_id: NodeId, address: String, forward: u64) -> ForwardRequest {
    ForwardRequest {
        forward_to_leader: forward,
        body: ForwardRequestBody::Join(JoinRequest { node_id, address }),
    }
}

/// Write one log on leader, check all nodes replicated the log.
/// Returns the number log committed.
async fn assert_upsert_kv_synced(meta_nodes: Vec<Arc<MetaNode>>, key: &str) -> anyhow::Result<u64> {
    let leader_id = meta_nodes[0].get_leader().await;
    let leader = meta_nodes[leader_id as usize].clone();

    let last_applied = leader.raft.metrics().borrow().last_applied;
    tracing::info!("leader: last_applied={:?}", last_applied);
    {
        leader
            .as_leader()
            .await?
            .write(LogEntry {
                txid: None,
                cmd: Cmd::UpsertKV {
                    key: key.to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(key.to_string().into_bytes()),
                    value_meta: None,
                },
            })
            .await?;
    }

    assert_applied_index(meta_nodes.clone(), last_applied.next_index()).await?;
    assert_get_kv(meta_nodes.clone(), key, key).await?;

    Ok(1)
}

/// Wait nodes for applied index to be upto date: applied >= at_least.
async fn assert_applied_index(meta_nodes: Vec<Arc<MetaNode>>, at_least: u64) -> anyhow::Result<()> {
    for (_i, mn) in meta_nodes.iter().enumerate() {
        wait_for_log(mn, at_least).await?;
    }
    Ok(())
}

async fn assert_get_kv(
    meta_nodes: Vec<Arc<MetaNode>>,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    for (i, mn) in meta_nodes.iter().enumerate() {
        let got = mn.get_kv(key).await?;
        // let got = mn.get_file(key).await?;
        assert_eq!(
            value.to_string().into_bytes(),
            got.unwrap().data,
            "n{} applied value",
            i
        );
    }
    Ok(())
}

/// Wait for the known leader of a raft to become the expected `leader_id` until a default 2000 ms time out.
#[tracing::instrument(level = "info", skip(mn))]
pub async fn wait_for_current_leader(
    mn: &MetaNode,
    leader_id: NodeId,
) -> anyhow::Result<RaftMetrics> {
    let metrics = mn
        .raft
        .wait(timeout())
        .current_leader(leader_id, "")
        .await?;
    Ok(metrics)
}

/// Wait for raft log to become the expected `index` until a default 2000 ms time out.
#[tracing::instrument(level = "info", skip(mn))]
async fn wait_for_log(mn: &MetaNode, index: u64) -> anyhow::Result<RaftMetrics> {
    let metrics = mn.raft.wait(timeout()).log(Some(index), "").await?;
    Ok(metrics)
}

/// Wait for raft state to become the expected `state` until a default 2000 ms time out.
#[tracing::instrument(level = "debug", skip(mn))]
pub async fn wait_for_state(mn: &MetaNode, state: openraft::State) -> anyhow::Result<RaftMetrics> {
    let metrics = mn.raft.wait(timeout()).state(state, "").await?;
    Ok(metrics)
}

/// Wait for raft metrics to become a state that satisfies `func`.
#[tracing::instrument(level = "debug", skip(mn, func))]
async fn wait_for<T>(mn: &MetaNode, func: T) -> anyhow::Result<RaftMetrics>
where T: Fn(&RaftMetrics) -> bool + Send {
    let metrics = mn.raft.wait(timeout()).metrics(func, "").await?;
    Ok(metrics)
}

/// Make a default timeout for wait() for test.
fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(10000))
}

fn test_context_nodes(tcs: &[MetaSrvTestContext]) -> Vec<Arc<MetaNode>> {
    tcs.iter().map(|tc| tc.meta_node()).collect::<Vec<_>>()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_node_incr_seq() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = MetaSrvTestContext::new(0);
    let addr = tc.config.raft_config.raft_api_addr().await?;

    let _mn = MetaNode::boot(&tc.config.raft_config).await?;
    tc.assert_raft_server_connection().await?;

    let mut client = RaftServiceClient::connect(format!("http://{}", addr)).await?;

    let cases = common_meta_raft_store::state_machine::testing::cases_incr_seq();

    for (name, txid, k, want) in cases.iter() {
        let req = LogEntry {
            txid: txid.clone(),
            cmd: Cmd::IncrSeq { key: k.to_string() },
        };
        let raft_reply = client.write(req).await?.into_inner();

        let res: Result<AppliedState, RetryableError> = raft_reply.into();
        let resp: AppliedState = res?;
        match resp {
            AppliedState::Seq { seq } => {
                assert_eq!(*want, seq, "{}", name);
            }
            _ => {
                panic!("not Seq")
            }
        }
    }

    Ok(())
}
