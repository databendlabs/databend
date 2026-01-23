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

use std::collections::BTreeMap;
use std::io;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::immutable_compactor::InMemoryCompactor;
use databend_common_meta_raft_store::sm_v003::SMV003;
use databend_common_meta_raft_store::sm_v003::SnapshotStoreV004;
use databend_common_meta_raft_store::sm_v003::WriteEntry;
use databend_common_meta_raft_store::state_machine::MetaSnapshotId;
use databend_common_meta_runtime_api::SpawnApi;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::Snapshot;
use databend_common_meta_types::raft_types::SnapshotMeta;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_metrics::count::Count;
use futures::FutureExt;
use futures::TryStreamExt;
use log::error;
use log::info;
use seq_marked::InternalSeq;
use tokio::sync::oneshot;

use crate::metrics::SnapshotBuilding;
use crate::metrics::raft_metrics;

mod raft_state_machine_impl;

/// A shared wrapper for use of SMV003 in this crate.
#[derive(Debug)]
pub struct MetaRaftStateMachine<SP> {
    pub(crate) id: NodeId,

    pub(crate) config: Arc<RaftConfig>,

    pub(crate) in_memory_compactor_cancel: Option<Arc<oneshot::Sender<()>>>,

    inner: Arc<Mutex<Arc<SMV003>>>,

    _phantom: PhantomData<SP>,
}

impl<SP> Clone for MetaRaftStateMachine<SP> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            config: self.config.clone(),
            in_memory_compactor_cancel: self.in_memory_compactor_cancel.clone(),
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<SP: SpawnApi> MetaRaftStateMachine<SP> {
    pub fn new(id: NodeId, config: Arc<RaftConfig>, inner: Arc<SMV003>) -> Self {
        Self {
            id,
            config,
            in_memory_compactor_cancel: None,
            inner: Arc::new(Mutex::new(inner)),
            _phantom: PhantomData,
        }
    }

    pub(crate) fn spawn_in_memory_compactor(&mut self, interval: Duration) {
        let (tx, rx) = oneshot::channel();

        self.in_memory_compactor_cancel = Some(Arc::new(tx));

        let weak = Arc::downgrade(&self.get_inner());
        let fu = Self::compact_loop(weak, interval, rx);

        #[allow(unused_must_use)]
        SP::spawn(fu, Some("in_memory_compactor".into()));
    }

    async fn compact_loop(
        weak_sm: Weak<SMV003>,
        interval: Duration,
        cancel: oneshot::Receiver<()>,
    ) {
        let mut c = std::pin::pin!(cancel);

        loop {
            let timeout_fu = tokio::time::sleep(interval);
            futures::select! {
                _ = c.as_mut().fuse() => {
                    info!("in_memory_compact canceled by user");
                    return;
                }

                _ = timeout_fu.fuse() => {
                    info!("in_memory_compact is woken up, try to compact");
                }

            }

            let Some(sm) = weak_sm.upgrade() else {
                info!("in_memory_compact canceled as state machine dropped");
                return;
            };

            Self::in_memory_compact_once(sm).await
        }
    }

    async fn in_memory_compact_once(sm: Arc<SMV003>) {
        let compactor = InMemoryCompactor::new(sm, "in_memory_compactor").await;

        // 1. freeze the writable
        let immutable_compactor = compactor.freeze();

        // 2. compact two adjacent levels if needed.
        // TODO: add snapshot seq when a mvcc is created.
        //       currently, use the max value to eliminate all older records.
        let min_snapshot_seq = InternalSeq::new(u64::MAX);
        immutable_compactor.compact(min_snapshot_seq);

        info!("in_memory_compact completed");
    }

    pub fn get_inner(&self) -> Arc<SMV003> {
        self.inner.lock().unwrap().deref().clone()
    }

    pub fn set_inner(&self, sm: Arc<SMV003>) {
        let mut guard = self.inner.lock().unwrap();
        *guard = sm;
    }

    #[fastrace::trace]
    pub(crate) async fn do_build_snapshot(&self) -> Result<Snapshot, io::Error> {
        // NOTE: building snapshot is guaranteed to be serialized called by RaftCore.

        info!(id = self.id; "do_build_snapshot start");

        let _guard = SnapshotBuilding::guard();

        let sm_v003 = self.get_inner();
        let mut compactor_permit = sm_v003
            .new_compactor_acquirer("build_snapshot")
            .acquire()
            .await;
        {
            let mut writer_permit = sm_v003.acquire_writer_permit().await;
            sm_v003
                .leveled_map()
                .freeze_writable(&mut writer_permit, &mut compactor_permit);
        }

        let mut compactor = sm_v003.new_compactor(compactor_permit);

        info!("do_build_snapshot compactor created");

        let (mut sys_data, mut strm) = compactor.compact_into_stream().await?;

        let last_applied = *sys_data.last_applied_ref();
        let last_membership = sys_data.last_membership_ref().clone();
        let snapshot_id = MetaSnapshotId::new_with_epoch(last_applied);
        let snapshot_meta = SnapshotMeta {
            snapshot_id: snapshot_id.to_string(),
            last_log_id: last_applied,
            last_membership,
        };
        let ss_store = self.snapshot_store();
        let writer = ss_store.new_writer()?;

        let context = format!("build snapshot: {:?}", last_applied);
        let (tx, th) = writer.spawn_writer_thread(context);

        info!("do_build_snapshot writing snapshot start");

        // Pipe entries to the writer.
        {
            // Count the user keys and expiration keys.
            let mut key_counts = BTreeMap::<String, u64>::new();

            while let Some(ent) = strm.try_next().await? {
                // The first 4 chars are key space, such as: "kv--/" or "exp-/"
                // Get the first 4 chars as key space.
                let prefix = &ent.0.as_str()[..4];
                if let Some(count) = key_counts.get_mut(prefix) {
                    *count += 1;
                } else {
                    key_counts.insert(prefix.to_string(), 1);
                }

                tx.send(WriteEntry::Data(ent))
                    .await
                    .map_err(io::Error::other)?;

                raft_metrics::storage::incr_snapshot_written_entries();
            }

            {
                let ks = sys_data.key_counts_mut();
                *ks = key_counts;
            }

            tx.send(WriteEntry::Finish((snapshot_id.clone(), sys_data)))
                .await
                .map_err(io::Error::other)?;
        }

        // Get snapshot write result
        let db = th.await.map_err(|e| {
            error!(error :% = e; "snapshot writer thread error");
            io::Error::other(e)
        })??;

        info!(
            snapshot_id :% = snapshot_id.to_string(),
            snapshot_file_size :% = db.file_size(),
            snapshot_stat :% = db.stat(); "do_build_snapshot complete");

        {
            sm_v003
                .leveled_map()
                .replace_with_compacted(&mut compactor, db.clone());
            info!("do_build_snapshot-replace_with_compacted returned");
        }

        // Consume and drop compactor, dropping it may involve blocking IO
        SP::spawn_blocking(move || {
            let drop_start = std::time::Instant::now();
            drop(compactor);
            info!(
                "do_build_snapshot-compactor-drop completed in {:?}",
                drop_start.elapsed()
            );
        })
        .await
        .expect("compactor drop task should not panic");

        // Clean old snapshot
        ss_store.new_loader().clean_old_snapshots().await?;
        info!("do_build_snapshot clean_old_snapshots complete");

        Ok(Snapshot {
            meta: snapshot_meta,
            snapshot: db,
        })
    }

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[fastrace::trace]
    pub async fn do_install_snapshot(&mut self, db: DB) -> Result<(), MetaStorageError> {
        let data_size = db.inner().file_size();
        let sys_data = db.sys_data().clone();

        info!(
            "SMV003::install_snapshot: data_size: {}; sys_data: {:?}",
            data_size, sys_data
        );

        let sm = self.get_inner();

        // Do not skip install if both self.last_applied and db.last_applied are None.
        //
        // The snapshot may contain data when its last_applied is None,
        // when importing data with metactl:
        // The snapshot is empty but contains Nodes data that are manually added.
        //
        // See: `databend_metactl::import`
        let my_last_applied = *sm.sys_data().last_applied_ref();
        #[allow(clippy::collapsible_if)]
        if my_last_applied.is_some() {
            if &my_last_applied >= sys_data.last_applied_ref() {
                info!(
                    "SMV003 try to install a smaller snapshot({:?}), ignored, my last applied: {:?}",
                    sys_data.last_applied_ref(),
                    sm.sys_data().last_applied_ref()
                );
                return Ok(());
            }
        }

        let new_sm = sm.install_snapshot_v003(db);

        self.set_inner(Arc::new(new_sm));

        Ok(())
    }

    /// Return a snapshot store of this instance.
    pub fn snapshot_store(&self) -> SnapshotStoreV004<SP> {
        SnapshotStoreV004::new(self.config.as_ref().clone())
    }
}
