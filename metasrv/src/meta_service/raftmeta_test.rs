// Copyright 2020 Datafuse Labs.
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

use async_raft::RaftMetrics;
use async_raft::State;
use common_base::tokio;
use common_base::tokio::time::Duration;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::MatchSeq;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_tracing::tracing;
use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::configs;
use crate::meta_service::MetaNode;
use crate::meta_service::RetryableError;
use crate::tests::assert_meta_connection;
use crate::tests::service::new_test_context;
use crate::tests::service::KVSrvTestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_boot() -> anyhow::Result<()> {
    // - Start a single node meta service cluster.
    // - Test the single node is recorded by this cluster.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let addr = tc.config.raft_config.raft_api_addr();

    let mn = MetaNode::boot(0, &tc.config.raft_config).await?;

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

    let (_nid0, tc) = setup_leader().await?;
    let mn0 = tc.meta_nodes[0].clone();

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

        let (_nid0, tc0) = setup_leader().await?;
        let mn0 = tc0.meta_nodes[0].clone();

        let (_nid1, tc1) = setup_non_voter(mn0.clone(), 1).await?;
        let mn1 = tc1.meta_nodes[0].clone();

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

        let (mut _nlog, tcs) = setup_cluster(btreeset![0, 1, 2], btreeset![3]).await?;
        let all = test_context_nodes(&tcs);

        let leader_id = all[0].raft.metrics().borrow().current_leader.unwrap();

        // test writing to leader and non-leader
        let key = "t-non-leader-write";
        for id in 0u64..4 {
            let mn = &all[id as usize];
            let rst = mn
                .write_to_local_leader(LogEntry {
                    txid: None,
                    cmd: Cmd::UpsertKV {
                        key: key.to_string(),
                        seq: MatchSeq::Any,
                        value: Operation::Update(key.to_string().into_bytes()),
                        value_meta: None,
                    },
                })
                .await;

            let rst = rst?;

            if id == leader_id {
                assert!(rst.is_ok());
            } else {
                assert!(rst.is_err());
                let e = rst.unwrap_err();
                match e {
                    RetryableError::ForwardToLeader { leader } => {
                        assert_eq!(leader_id, leader);
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_meta_node_set_file() -> anyhow::Result<()> {
    // - Start a leader, 2 followers and 2 non-voter;
    // - Write to the raft node on every node, expect Ok.

    // TODO: test MetaNode.write during leader changes.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    {
        let span = tracing::span!(tracing::Level::DEBUG, "test_meta_node_set_file");
        let _ent = span.enter();

        tracing::info!("start");

        let (mut _nlog, tcs) = setup_cluster(btreeset![0, 1, 2], btreeset![3, 4]).await?;

        let all = test_context_nodes(&tcs);

        // test writing on every node
        for id in 0u64..4 {
            let key = format!("test_meta_node_set_file-key-{}", id);
            let mn = &all[id as usize];

            assert_upsert_kv_on_specified_node_synced(all.clone(), mn.clone(), &key).await?;
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

    {
        let span = tracing::span!(tracing::Level::DEBUG, "test_meta_node_add_database");
        let _ent = span.enter();

        let (_nlog, all_tc) = setup_cluster(btreeset![0, 1, 2], btreeset![3]).await?;
        let all = all_tc
            .iter()
            .map(|tc| tc.meta_nodes[0].clone())
            .collect::<Vec<_>>();

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
                        name: name.to_string(),
                    },
                })
                .await;

            assert!(rst.is_ok());

            // No matter if a db is created, the log that tries to create db always applies.
            assert_applied_index(all.clone(), last_applied + 1).await?;

            for (i, mn) in all.iter().enumerate() {
                let got = mn.get_state_machine().await.get_database(name)?;

                assert_eq!(*want_id, got.unwrap().data, "n{} applied AddDatabase", i);
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

    let mut tc = new_test_context();
    tc.config.raft_config.snapshot_logs_since_last = snap_logs;
    tc.config.raft_config.install_snapshot_timeout = 10_1000; // milli seconds. In a CI multi-threads test delays async task badly.
    let addr = tc.config.raft_config.raft_api_addr();

    let mn = MetaNode::boot(0, &tc.config.raft_config).await?;

    assert_meta_connection(&addr).await?;

    wait_for_state(&mn, State::Leader).await?;
    wait_for_current_leader(&mn, 0).await?;

    let mut n_logs = 2;

    mn.raft
        .wait(timeout())
        .log(n_logs, "leader init logs")
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
    n_logs += n_req;

    tracing::info!("--- check the log is locally applied");

    mn.raft
        .wait(timeout())
        .log(n_logs, "applied on leader")
        .await?;

    tracing::info!("--- check the snapshot is created");

    mn.raft
        .wait(timeout())
        .metrics(
            |x| x.snapshot.term == 1 && x.snapshot.index >= 10,
            "snapshot is created by leader",
        )
        .await?;

    tracing::info!("--- start a non_voter to receive snapshot replication");

    let (_, tc1) = setup_non_voter(mn.clone(), 1).await?;
    n_logs += 1;

    let mn1 = tc1.meta_nodes[0].clone();

    mn1.raft
        .wait(timeout())
        .log(n_logs, "non-voter replicated all logs")
        .await?;

    mn1.raft
        .wait(timeout())
        .metrics(
            |x| x.snapshot.term == 1 && x.snapshot.index >= 10,
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
async fn test_meta_node_cluster_1_2_2() -> anyhow::Result<()> {
    // - Bring up a cluster with 1 leader, 2 followers and 2 non-voters.
    // - Write to leader, check data is replicated.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let span = tracing::span!(tracing::Level::INFO, "test_meta_node_cluster_1_2_2");
    let _ent = span.enter();

    let (mut _nlog, tcs) = setup_cluster(btreeset![0, 1, 2], btreeset![3, 4]).await?;
    let all = test_context_nodes(&tcs);

    _nlog += assert_upsert_kv_synced(all.clone(), "foo-1").await?;

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

    let (_nid0, tc0) = setup_leader().await?;
    let mn0 = tc0.meta_nodes[0].clone();

    let (_nid1, tc1) = setup_non_voter(mn0.clone(), 1).await?;
    let mn1 = tc1.meta_nodes[0].clone();

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
    wait_for_state(&mn1, State::NonVoter).await?;
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

    let mut log_cnt: u64 = 0;
    let (_id, mut tc) = setup_leader().await?;
    log_cnt += 2;

    let want_hs;
    {
        let leader = tc.meta_nodes.pop().unwrap();

        leader
            .write_to_local_leader(LogEntry {
                txid: None,
                cmd: Cmd::UpsertKV {
                    key: "foo".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"1".to_vec()),
                    value_meta: None,
                },
            })
            .await??;
        log_cnt += 1;

        want_hs = leader.sto.raft_state.read_hard_state()?;

        leader.stop().await?;
    }

    tracing::info!("--- reopen MetaNode");
    let leader = MetaNode::open(&tc.config.raft_config).await?;
    log_cnt += 1;

    wait_for_state(&leader, State::Leader).await?;
    wait_for_log(&leader, log_cnt as u64).await?;

    tracing::info!("--- check hard state");
    {
        let hs = leader.sto.raft_state.read_hard_state()?;
        assert_eq!(want_hs, hs);
    }

    tracing::info!("--- check logs");
    {
        let logs = leader.sto.log.range_values(..)?;
        tracing::info!("logs: {:?}", logs);
        assert_eq!(log_cnt as usize, logs.len());
    }

    tracing::info!("--- check state machine: nodes");
    {
        let node = leader.sto.get_node(&0).await?.unwrap();
        assert_eq!(tc.config.raft_config.raft_api_addr(), node.address);
    }

    Ok(())
}

/// Setup a cluster with several voter and several non_voter
/// The node id 0 must be in `voters` and node 0 is elected as leader.
async fn setup_cluster(
    voters: BTreeSet<NodeId>,
    non_voters: BTreeSet<NodeId>,
) -> anyhow::Result<(u64, Vec<KVSrvTestContext>)> {
    // TODO(xp): use setup_cluster if possible in tests. Get rid of boilerplate snippets.
    // leader is always node-0
    assert!(voters.contains(&0));
    assert!(!non_voters.contains(&0));

    let mut rst = vec![];

    let (_id, tc0) = setup_leader().await?;
    let leader = tc0.meta_nodes[0].clone();
    rst.push(tc0);

    // blank log and add node
    let mut nlog = 2;
    wait_for_log(&leader, nlog).await?;

    for id in voters.iter() {
        // leader is already created.
        if *id == 0 {
            continue;
        }
        let (_id, tc) = setup_non_voter(leader.clone(), *id).await?;

        // Adding a node
        nlog += 1;
        wait_for_log(&tc.meta_nodes[0], nlog).await?;

        rst.push(tc);
    }

    for id in non_voters.iter() {
        let (_id, tc) = setup_non_voter(leader.clone(), *id).await?;

        // Adding a node
        nlog += 1;
        wait_for_log(&tc.meta_nodes[0], nlog).await?;

        rst.push(tc);
    }

    leader.raft.change_membership(voters.clone()).await?;
    nlog += 2;

    tracing::info!("--- check node roles");
    {
        wait_for_state(&leader, State::Leader).await?;

        for item in rst.iter().take(voters.len()).skip(1) {
            wait_for_state(&item.meta_nodes[0], State::Follower).await?;
        }
        for item in rst.iter().skip(voters.len()).take(non_voters.len()) {
            wait_for_state(&item.meta_nodes[0], State::NonVoter).await?;
        }
    }

    tracing::info!("--- check node logs");
    {
        for item in &rst {
            wait_for_log(&item.meta_nodes[0], nlog).await?;
        }
    }

    Ok((nlog, rst))
}

async fn setup_leader() -> anyhow::Result<(NodeId, KVSrvTestContext)> {
    // Setup a cluster in which there is a leader and a non-voter.
    // asserts states are consistent

    let nid = 0;
    let mut tc = new_test_context();
    let addr = tc.config.raft_config.raft_api_addr();

    // boot up a single-node cluster
    let mn = MetaNode::boot(nid, &tc.config.raft_config).await?;
    tc.meta_nodes.push(mn.clone());

    {
        assert_meta_connection(&addr).await?;

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
async fn setup_non_voter(
    leader: Arc<MetaNode>,
    id: NodeId,
) -> anyhow::Result<(NodeId, KVSrvTestContext)> {
    let mut tc = new_test_context();
    let addr = tc.config.raft_config.raft_api_addr();

    let mn = MetaNode::boot_non_voter(id, &tc.config.raft_config).await?;
    tc.meta_nodes.push(mn.clone());

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
        assert_meta_connection(&addr).await?;
        wait_for_state(&mn, State::NonVoter).await?;
        wait_for_current_leader(&mn, 0).await?;
    }

    Ok((id, tc))
}

/// Write one log on leader, check all nodes replicated the log.
/// Returns the number log committed.
async fn assert_upsert_kv_synced(meta_nodes: Vec<Arc<MetaNode>>, key: &str) -> anyhow::Result<u64> {
    let leader_id = meta_nodes[0].get_leader().await;
    let leader = meta_nodes[leader_id as usize].clone();

    let last_applied = leader.raft.metrics().borrow().last_applied;
    tracing::info!("leader: last_applied={}", last_applied);
    {
        leader
            .write_to_local_leader(LogEntry {
                txid: None,
                cmd: Cmd::UpsertKV {
                    key: key.to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(key.to_string().into_bytes()),
                    value_meta: None,
                },
            })
            .await??;
    }

    assert_applied_index(meta_nodes.clone(), last_applied + 1).await?;
    assert_get_kv(meta_nodes.clone(), key, key).await?;

    Ok(1)
}

/// Write one log on every node, check all nodes replicated the log.
/// Returns the number log committed.
async fn assert_upsert_kv_on_specified_node_synced(
    meta_nodes: Vec<Arc<MetaNode>>,
    write_to: Arc<MetaNode>,
    key: &str,
) -> anyhow::Result<u64> {
    let leader_id = meta_nodes[0].get_leader().await;
    let leader = meta_nodes[leader_id as usize].clone();

    let last_applied = leader.raft.metrics().borrow().last_applied;
    tracing::info!("leader: last_applied={}", last_applied);

    {
        write_to
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

    assert_applied_index(meta_nodes.clone(), last_applied + 1).await?;
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
    let metrics = mn.raft.wait(timeout()).log(index, "").await?;
    Ok(metrics)
}

/// Wait for raft state to become the expected `state` until a default 2000 ms time out.
#[tracing::instrument(level = "debug", skip(mn))]
pub async fn wait_for_state(
    mn: &MetaNode,
    state: async_raft::State,
) -> anyhow::Result<RaftMetrics> {
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

fn test_context_nodes(tcs: &[KVSrvTestContext]) -> Vec<Arc<MetaNode>> {
    tcs.iter()
        .map(|tc| tc.meta_nodes[0].clone())
        .collect::<Vec<_>>()
}
