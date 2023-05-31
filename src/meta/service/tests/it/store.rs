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

use common_base::base::tokio;
use common_meta_raft_store::state_machine::testing::snapshot_logs;
use common_meta_sled_store::openraft::async_trait::async_trait;
use common_meta_sled_store::openraft::entry::RaftEntry;
use common_meta_sled_store::openraft::storage::Adaptor;
use common_meta_sled_store::openraft::storage::RaftLogReaderExt;
use common_meta_sled_store::openraft::testing::log_id;
use common_meta_sled_store::openraft::testing::StoreBuilder;
use common_meta_sled_store::openraft::RaftSnapshotBuilder;
use common_meta_sled_store::openraft::RaftStorage;
use common_meta_types::new_log_id;
use common_meta_types::Entry;
use common_meta_types::Membership;
use common_meta_types::StorageError;
use common_meta_types::StoredMembership;
use common_meta_types::TypeConfig;
use common_meta_types::Vote;
use databend_meta::init_meta_ut;
use databend_meta::meta_service::raftmeta::LogStore;
use databend_meta::meta_service::raftmeta::SMStore;
use databend_meta::store::RaftStore;
use databend_meta::Opened;
use maplit::btreeset;
use pretty_assertions::assert_eq;
use tracing::debug;
use tracing::info;

use crate::tests::service::MetaSrvTestContext;

struct MetaStoreBuilder {}

#[async_trait]
impl StoreBuilder<TypeConfig, LogStore, SMStore, MetaSrvTestContext> for MetaStoreBuilder {
    async fn build(&self) -> Result<(MetaSrvTestContext, LogStore, SMStore), StorageError> {
        let tc = MetaSrvTestContext::new(555);
        let sto = RaftStore::open_create(&tc.config.raft_config, None, Some(()))
            .await
            .expect("fail to create store");
        let (log_store, sm_store) = Adaptor::new(sto);
        Ok((tc, log_store, sm_store))
    }
}

#[test]
fn test_impl_raft_storage() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    common_meta_sled_store::openraft::testing::Suite::test_all(MetaStoreBuilder {})?;

    Ok(())
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
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

        sto.append_to_log([Entry::new_blank(log_id(1, 2, 1))])
            .await?;

        sto.save_committed(Some(log_id(1, 2, 2))).await?;

        sto.apply_to_state_machine(&[Entry::new_blank(log_id(1, 2, 2))])
            .await?;
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
            sto.last_applied_state().await?.0,
            "state machine is not persisted"
        );
    }
    Ok(())
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_store_build_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Apply logs
    // - Create a snapshot check snapshot state

    let id = 3;
    let tc = MetaSrvTestContext::new(id);

    let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log.append(logs.clone()).await?;
    sto.state_machine.write().await.apply_entries(&logs);

    let curr_snap = sto.build_snapshot().await?;
    assert_eq!(Some(new_log_id(1, 0, 9)), curr_snap.meta.last_log_id);

    info!("--- check snapshot");
    {
        let data = curr_snap.snapshot;
        let res = data.read_to_lines().await?;

        debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    Ok(())
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_store_current_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Apply logs
    // - Create a snapshot check snapshot state

    let id = 3;
    let tc = MetaSrvTestContext::new(id);

    let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log.append(logs.clone()).await?;
    {
        let mut sm = sto.state_machine.write().await;
        sm.apply_entries(&logs);
    }

    sto.build_snapshot().await?;

    info!("--- check get_current_snapshot");

    let curr_snap = sto.get_current_snapshot().await?.unwrap();
    assert_eq!(Some(new_log_id(1, 0, 9)), curr_snap.meta.last_log_id);

    info!("--- check snapshot");
    {
        let data = curr_snap.snapshot;
        let res = data.read_to_lines().await?;

        debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    Ok(())
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
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

        sto.log.append(logs.clone()).await?;
        sto.state_machine.write().await.apply_entries(&logs);

        snap = sto.build_snapshot().await?;
    }

    let data = snap.snapshot;

    info!("--- reopen a new metasrv to install snapshot");
    {
        let tc = MetaSrvTestContext::new(id);

        let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

        info!("--- install snapshot");
        {
            // TODO(1): remove write_state_machine_id
            // sto.raft_state.write_state_machine_id(&(0, 0)).await?;
            sto.do_install_snapshot(data).await?;
        }

        info!("--- check installed meta");
        {
            let mem = sto
                .state_machine
                .write()
                .await
                .last_membership_ref()
                .clone();

            assert_eq!(
                StoredMembership::new(
                    Some(log_id(1, 0, 5)),
                    Membership::new(vec![btreeset! {4,5,6}], ())
                ),
                mem
            );

            let last_applied = sto.state_machine.write().await.last_applied_ref().clone();
            assert_eq!(Some(log_id(1, 0, 9)), last_applied);
        }

        info!("--- check snapshot");
        {
            let curr_snap = sto.build_snapshot().await?;
            let data = curr_snap.snapshot;
            let res = data.read_to_lines().await?;

            debug!("res: {:?}", res);

            assert_eq!(want, res);
        }
    }

    Ok(())
}
