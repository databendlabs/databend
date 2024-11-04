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
use databend_common_meta_types::new_log_id;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::Entry;
use databend_common_meta_types::Membership;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::StoredMembership;
use databend_common_meta_types::TypeConfig;
use databend_common_meta_types::Vote;
use databend_meta::meta_service::meta_node::LogStore;
use databend_meta::meta_service::meta_node::SMStore;
use databend_meta::store::RaftStore;
use databend_meta::Opened;
use futures::TryStreamExt;
use log::debug;
use log::info;
use maplit::btreeset;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;

struct MetaStoreBuilder {}

impl StoreBuilder<TypeConfig, LogStore, SMStore, MetaSrvTestContext> for MetaStoreBuilder {
    async fn build(&self) -> Result<(MetaSrvTestContext, LogStore, SMStore), StorageError> {
        let tc = MetaSrvTestContext::new(555);
        let sto = RaftStore::open_create(&tc.config.raft_config, None, Some(()))
            .await
            .expect("fail to create store");
        Ok((tc, sto.clone(), sto))
    }
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_impl_raft_storage() -> anyhow::Result<()> {
    databend_common_meta_sled_store::openraft::testing::log::Suite::test_all(MetaStoreBuilder {})
        .await?;

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
        let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;
        assert_eq!(id, sto.id);
        assert!(!sto.is_opened());
        assert_eq!(None, sto.read_vote().await?);

        info!("--- update metasrv");

        sto.save_vote(&Vote::new(10, 5)).await?;

        sto.blocking_append([Entry::new_blank(log_id(1, 2, 1))])
            .await?;

        sto.save_committed(Some(log_id(1, 2, 2))).await?;

        sto.apply([Entry::new_blank(log_id(1, 2, 2))]).await?;
    }

    info!("--- reopen meta store");
    {
        let mut sto = RaftStore::open_create(&tc.config.raft_config, Some(()), None).await?;
        assert_eq!(id, sto.id);
        assert!(sto.is_opened());
        assert_eq!(Some(Vote::new(10, 5)), sto.read_vote().await?);

        assert_eq!(log_id(1, 2, 1), sto.get_log_id(1).await?);
        assert_eq!(Some(log_id(1, 2, 2)), sto.read_committed().await?);
        assert_eq!(
            None,
            sto.applied_state().await?.0,
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

    let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log.write().await.append(logs.clone()).await?;
    sto.state_machine.write().await.apply_entries(logs).await?;

    let curr_snap = sto.build_snapshot().await?;
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
        sto.build_snapshot().await?;
        sto.build_snapshot().await?;
        sto.build_snapshot().await?;
        sto.build_snapshot().await?;

        let snapshot_store = sto.snapshot_store();
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

    let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log.write().await.append(logs.clone()).await?;
    {
        let mut sm = sto.state_machine.write().await;
        sm.apply_entries(logs).await?;
    }

    sto.build_snapshot().await?;

    info!("--- check get_current_snapshot");

    let curr_snap = sto.get_current_snapshot().await?.unwrap();
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

        let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

        info!("--- feed logs and state machine");

        sto.log.write().await.append(logs.clone()).await?;
        sto.state_machine.write().await.apply_entries(logs).await?;

        snap = sto.build_snapshot().await?;
    }

    let data = snap.snapshot;

    info!("--- reopen a new metasrv to install snapshot");
    {
        let tc = MetaSrvTestContext::new(id);

        let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

        info!("--- install snapshot");
        {
            sto.do_install_snapshot(data.as_ref().clone()).await?;
        }

        info!("--- check installed meta");
        {
            let mem = sto
                .state_machine
                .write()
                .await
                .sys_data_ref()
                .last_membership_ref()
                .clone();

            assert_eq!(
                StoredMembership::new(
                    Some(log_id(1, 0, 5)),
                    Membership::new(vec![btreeset! {4,5,6}], ())
                ),
                mem
            );

            let last_applied = *sto
                .state_machine
                .write()
                .await
                .sys_data_ref()
                .last_applied_ref();
            assert_eq!(Some(log_id(1, 0, 9)), last_applied);
        }

        info!("--- check snapshot");
        {
            let curr_snap = sto.build_snapshot().await?;
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
