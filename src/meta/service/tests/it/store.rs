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

use std::io;

use databend_common_meta_raft_store::leveled_store::db_exporter::DBExporter;
use databend_common_meta_raft_store::state_machine::testing::snapshot_logs;
use databend_common_meta_sled_store::openraft::entry::RaftEntry;
use databend_common_meta_sled_store::openraft::storage::RaftLogReaderExt;
use databend_common_meta_sled_store::openraft::storage::RaftLogStorage;
use databend_common_meta_sled_store::openraft::storage::RaftLogStorageExt;
use databend_common_meta_sled_store::openraft::storage::RaftStateMachine;
use databend_common_meta_sled_store::openraft::testing::log::StoreBuilder;
use databend_common_meta_sled_store::openraft::testing::log_id;
use databend_common_meta_sled_store::openraft::RaftLogReader;
use databend_common_meta_sled_store::openraft::RaftSnapshotBuilder;
use databend_common_meta_types::raft_types::new_log_id;
use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::Membership;
use databend_common_meta_types::raft_types::StorageError;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::raft_types::TypeConfig;
use databend_common_meta_types::raft_types::Vote;
use databend_common_meta_types::snapshot_db::DB;
use databend_meta::meta_service::meta_node::LogStore;
use databend_meta::meta_service::meta_node::SMStore;
use databend_meta::store::RaftStore;
use futures::TryStreamExt;
use log::debug;
use log::info;
use maplit::btreeset;
use pretty_assertions::assert_eq;
use raft_log::DumpApi;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;

struct MetaStoreBuilder {}

impl StoreBuilder<TypeConfig, LogStore, SMStore, MetaSrvTestContext> for MetaStoreBuilder {
    async fn build(&self) -> Result<(MetaSrvTestContext, LogStore, SMStore), StorageError> {
        let tc = MetaSrvTestContext::new(555);
        let sto = RaftStore::open(&tc.config.raft_config)
            .await
            .expect("fail to create store");
        Ok((tc, sto.log.clone(), sto.state_machine.clone()))
    }
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_impl_raft_storage() -> anyhow::Result<()> {
    databend_common_meta_sled_store::openraft::testing::log::Suite::test_all(MetaStoreBuilder {})
        .await?;

    Ok(())
}

/// Ensure purged logs to be removed from the cache
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_store_purge_cache() -> anyhow::Result<()> {
    let id = 3;
    let mut tc = MetaSrvTestContext::new(id);
    tc.config.raft_config.log_cache_max_items = 100;
    // Build with small chunk, because all entries in the last open chunk will be cached.
    tc.config.raft_config.log_wal_chunk_max_records = 5;

    {
        let sto = RaftStore::open(&tc.config.raft_config).await?;

        sto.log.clone().save_vote(&Vote::new(10, 5)).await?;

        sto.log
            .clone()
            .blocking_append([
                Entry::new_blank(log_id(1, 2, 1)),
                Entry::new_blank(log_id(1, 2, 2)),
                Entry::new_blank(log_id(1, 2, 3)),
                Entry::new_blank(log_id(1, 2, 4)),
                Entry::new_blank(log_id(1, 2, 5)),
            ])
            .await?;

        let stat = sto.log.read().await.stat();
        assert_eq!(stat.payload_cache_item_count, 5);

        {
            let r = sto.log.read().await;
            let got = r.dump().write_to_string()?;
            println!("dump: {}", got);
            let want_dumped = r#"RaftLog:
ChunkId(00_000_000_000_000_000_000)
  R-00000: [000_000_000, 000_000_018) Size(18): State(RaftLogState { vote: None, last: None, committed: None, purged: None, user_data: None })
  R-00001: [000_000_018, 000_000_046) Size(28): State(RaftLogState { vote: None, last: None, committed: None, purged: None, user_data: Some(LogStoreMeta { node_id: Some(3) }) })
  R-00002: [000_000_046, 000_000_096) Size(50): SaveVote(Cw(Vote { leader_id: LeaderId { term: 10, node_id: 5 }, committed: false }))
  R-00003: [000_000_096, 000_000_148) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 1 }), Cw(blank))
  R-00004: [000_000_148, 000_000_200) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 2 }), Cw(blank))
ChunkId(00_000_000_000_000_000_200)
  R-00000: [000_000_000, 000_000_100) Size(100): State(RaftLogState { vote: Some(Cw(Vote { leader_id: LeaderId { term: 10, node_id: 5 }, committed: false })), last: Some(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 2 })), committed: None, purged: None, user_data: Some(LogStoreMeta { node_id: Some(3) }) })
  R-00001: [000_000_100, 000_000_152) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 3 }), Cw(blank))
  R-00002: [000_000_152, 000_000_204) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 4 }), Cw(blank))
  R-00003: [000_000_204, 000_000_256) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 5 }), Cw(blank))
"#;
            assert_eq!(want_dumped, got);
        }

        // When purging up to index=4, all entries in the last open chunk will still be cached.
        // All previous entries are purge, although the cache is not full.

        sto.log.clone().purge(log_id(1, 2, 4)).await?;

        let r = sto.log.read().await;
        let got = r.dump().write_to_string()?;
        println!("dump: {}", got);

        let stat = sto.log.read().await.stat();
        println!("stat: {:#}", stat);
        assert_eq!(stat.payload_cache_item_count, 3);
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_store_restart() -> anyhow::Result<()> {
    // - Create a meta store
    // - Update meta store
    // - Close and reopen it
    // - Test state is restored: hard state, log, state machine

    let id = 3;
    let tc = MetaSrvTestContext::new(id);

    info!("--- new meta store");
    {
        let sto = RaftStore::open(&tc.config.raft_config).await?;
        assert_eq!(id, sto.id);
        assert!(!sto.is_opened);
        assert_eq!(None, sto.log.clone().read_vote().await?);

        info!("--- update metasrv");

        sto.log.clone().save_vote(&Vote::new(10, 5)).await?;

        sto.log
            .clone()
            .blocking_append([Entry::new_blank(log_id(1, 2, 1))])
            .await?;

        sto.log
            .clone()
            .save_committed(Some(log_id(1, 2, 2)))
            .await?;

        sto.state_machine
            .clone()
            .apply([Entry::new_blank(log_id(1, 2, 2))])
            .await?;
    }

    info!("--- reopen meta store");
    {
        let sto = RaftStore::open(&tc.config.raft_config).await?;
        assert_eq!(id, sto.id);
        assert!(sto.is_opened);
        assert_eq!(Some(Vote::new(10, 5)), sto.log.clone().read_vote().await?);

        assert_eq!(log_id(1, 2, 1), sto.log.clone().get_log_id(1).await?);
        assert_eq!(
            Some(log_id(1, 2, 2)),
            sto.log.clone().read_committed().await?
        );
        assert_eq!(
            None,
            sto.state_machine.clone().applied_state().await?.0,
            "state machine is not persisted"
        );
    }
    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_store_build_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Apply logs
    // - Create a snapshot check snapshot state

    let id = 3;
    let tc = MetaSrvTestContext::new(id);

    let sto = RaftStore::open(&tc.config.raft_config).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log.clone().blocking_append(logs.clone()).await?;
    sto.state_machine().get_inner().apply_entries(logs).await?;

    let curr_snap = sto.state_machine.clone().build_snapshot().await?;
    assert_eq!(Some(new_log_id(1, 0, 9)), curr_snap.meta.last_log_id);

    info!("--- check snapshot");
    {
        let data = curr_snap.snapshot;
        let res = db_to_lines(&data).await?;

        debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    info!("--- rebuild other 4 times, keeps only last 3");
    {
        sto.state_machine.clone().build_snapshot().await?;
        sto.state_machine.clone().build_snapshot().await?;
        sto.state_machine.clone().build_snapshot().await?;
        sto.state_machine.clone().build_snapshot().await?;

        let snapshot_store = sto.state_machine.snapshot_store();
        let loader = snapshot_store.new_loader();
        let (snapshot_ids, _) = loader.load_snapshot_ids().await?;
        assert_eq!(3, snapshot_ids.len());
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_store_current_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Apply logs
    // - Create a snapshot check snapshot state

    let id = 3;
    let tc = MetaSrvTestContext::new(id);

    let sto = RaftStore::open(&tc.config.raft_config).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log.clone().blocking_append(logs.clone()).await?;
    {
        let sm = sto.state_machine();
        sm.get_inner().apply_entries(logs).await?;
    }

    sto.state_machine.clone().build_snapshot().await?;

    info!("--- check get_current_snapshot");

    let curr_snap = sto
        .state_machine
        .clone()
        .get_current_snapshot()
        .await?
        .unwrap();
    assert_eq!(Some(new_log_id(1, 0, 9)), curr_snap.meta.last_log_id);

    info!("--- check snapshot");
    {
        let data = curr_snap.snapshot;
        let res = db_to_lines(&data).await?;

        debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_store_install_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Feed logs
    // - Create a snapshot
    // - Create a new metasrv and restore it by install the snapshot

    let (logs, want) = snapshot_logs();

    let id = 3;
    let snap;
    {
        let tc = MetaSrvTestContext::new(id);

        let sto = RaftStore::open(&tc.config.raft_config).await?;

        info!("--- feed logs and state machine");

        sto.log.clone().blocking_append(logs.clone()).await?;
        sto.state_machine().get_inner().apply_entries(logs).await?;

        snap = sto.state_machine.clone().build_snapshot().await?;
    }

    let data = snap.snapshot;

    info!("--- reopen a new metasrv to install snapshot");
    {
        let tc = MetaSrvTestContext::new(id);

        let sto = RaftStore::open(&tc.config.raft_config).await?;

        info!("--- install snapshot");
        {
            sto.state_machine
                .clone()
                .do_install_snapshot(data.clone())
                .await?;
        }

        info!("--- check installed meta");
        {
            let mem = sto
                .state_machine()
                .get_inner()
                .sys_data()
                .last_membership_ref()
                .clone();

            assert_eq!(
                StoredMembership::new(
                    Some(log_id(1, 0, 5)),
                    Membership::new_with_defaults(vec![btreeset! {4,5,6}], [])
                ),
                mem
            );

            let last_applied = *sto
                .state_machine()
                .get_inner()
                .sys_data()
                .last_applied_ref();
            assert_eq!(Some(log_id(1, 0, 9)), last_applied);
        }

        info!("--- check snapshot");
        {
            let curr_snap = sto.state_machine.clone().build_snapshot().await?;
            let data = curr_snap.snapshot;
            let res = db_to_lines(&data).await?;

            debug!("res: {:?}", res);

            assert_eq!(want, res);
        }
    }

    Ok(())
}

async fn db_to_lines(db: &DB) -> Result<Vec<String>, io::Error> {
    let res = DBExporter::new(db)
        .export()
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    let res = res
        .into_iter()
        .map(|sm_ent| serde_json::to_string(&sm_ent).unwrap())
        .collect::<Vec<_>>();

    Ok(res)
}
