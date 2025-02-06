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

use std::collections::Bound;
use std::fmt::Debug;
use std::io;

use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::StorageError;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::AppliedState;
use log::info;
use openraft::RaftLogId;
use tokio::sync::mpsc;
use tonic::Status;

use crate::applier::Applier;
use crate::leveled_store::leveled_map::compactor::Compactor;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::sm_v003::sm_v003_kv_api::SMV003KVApi;
use crate::state_machine::ExpireKey;
use crate::state_machine_api::SMEventSender;
use crate::state_machine_api::StateMachineApi;
use crate::state_machine_api_ext::StateMachineApiExt;

#[derive(Debug, Default)]
pub struct SMV003 {
    levels: LeveledMap,

    /// The expiration key since which for next clean.
    expire_cursor: ExpireKey,

    /// subscriber of state machine data
    pub(crate) subscriber: Option<Box<dyn SMEventSender>>,
}

impl StateMachineApi for SMV003 {
    type Map = LeveledMap;

    fn get_expire_cursor(&self) -> ExpireKey {
        self.expire_cursor
    }

    fn set_expire_cursor(&mut self, cursor: ExpireKey) {
        self.expire_cursor = cursor;
    }

    fn map_ref(&self) -> &Self::Map {
        &self.levels
    }

    fn map_mut(&mut self) -> &mut Self::Map {
        &mut self.levels
    }

    fn sys_data_mut(&mut self) -> &mut SysData {
        self.levels.sys_data_mut()
    }

    fn event_sender(&self) -> Option<&dyn SMEventSender> {
        self.subscriber.as_ref().map(|x| x.as_ref())
    }
}
impl SMV003 {
    /// Return a mutable reference to the map that stores app data.
    pub(in crate::sm_v003) fn map_mut(&mut self) -> &mut LeveledMap {
        &mut self.levels
    }
}

impl SMV003 {
    pub fn kv_api(&self) -> SMV003KVApi {
        SMV003KVApi { sm: self }
    }

    /// Install and replace state machine with the content of a snapshot.
    pub async fn install_snapshot_v003(&mut self, db: DB) -> Result<(), io::Error> {
        let data_size = db.inner().file_size();
        let sys_data = db.sys_data().clone();

        info!(
            "SMV003::install_snapshot: data_size: {}; sys_data: {:?}",
            data_size, sys_data
        );

        // Do not skip install if both self.last_applied and db.last_applied are None.
        //
        // The snapshot may contain data when its last_applied is None,
        // when importing data with metactl:
        // The snapshot is empty but contains Nodes data that are manually added.
        //
        // See: `databend_metactl::import`
        let my_last_applied = *self.sys_data_ref().last_applied_ref();
        #[allow(clippy::collapsible_if)]
        if my_last_applied.is_some() {
            if &my_last_applied >= sys_data.last_applied_ref() {
                info!(
                    "SMV003 try to install a smaller snapshot({:?}), ignored, my last applied: {:?}",
                    sys_data.last_applied_ref(),
                    self.sys_data_ref().last_applied_ref()
                );
                return Ok(());
            }
        }

        self.levels.clear();
        let levels = self.levels_mut();
        *levels.sys_data_mut() = sys_data;
        *levels.persisted_mut() = Some(db);
        Ok(())
    }

    pub fn get_snapshot(&self) -> Option<DB> {
        self.levels.persisted().cloned()
    }

    /// Atomically reads and forwards a range of key-value pairs to the provided `tx`.
    ///
    /// - Any data publishing must be queued by the singleton sender to maintain ordering.
    ///
    /// - Atomically reading the key-value range within the state machine
    ///   and sending it to the singleton event sender.
    ///   Ensuring that there is no event out of order.
    pub async fn send_range(
        &mut self,
        tx: mpsc::Sender<Result<WatchResponse, Status>>,
        rng: (Bound<String>, Bound<String>),
    ) -> Result<(), io::Error> {
        let Some(sender) = self.event_sender() else {
            return Ok(());
        };

        let strm = self.range_kv(rng).await?;

        sender.send_batch(tx, strm);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn new_applier(&mut self) -> Applier<'_, Self> {
        Applier::new(self)
    }

    pub async fn apply_entries(
        &mut self,
        entries: impl IntoIterator<Item = Entry>,
    ) -> Result<Vec<AppliedState>, StorageError> {
        let mut applier = Applier::new(self);

        let mut res = vec![];

        for ent in entries.into_iter() {
            let log_id = *ent.get_log_id();
            let r = applier
                .apply(&ent)
                .await
                .map_err(|e| StorageError::apply(log_id, &e))?;
            res.push(r);
        }
        Ok(res)
    }

    pub fn sys_data_ref(&self) -> &SysData {
        self.levels.writable_ref().sys_data_ref()
    }

    pub fn sys_data_mut(&mut self) -> &mut SysData {
        self.levels.writable_mut().sys_data_mut()
    }

    pub fn into_levels(self) -> LeveledMap {
        self.levels
    }

    pub fn levels(&self) -> &LeveledMap {
        &self.levels
    }

    pub fn levels_mut(&mut self) -> &mut LeveledMap {
        self.map_mut()
    }

    pub fn set_event_sender(&mut self, subscriber: Box<dyn SMEventSender>) {
        self.subscriber = Some(subscriber);
    }

    pub fn freeze_writable(&mut self) {
        self.levels.freeze_writable();
    }

    pub fn try_acquire_compactor(&mut self) -> Option<Compactor> {
        self.map_mut().try_acquire_compactor()
    }

    pub async fn acquire_compactor(&mut self) -> Compactor {
        self.levels.acquire_compactor().await
    }

    /// Replace all the state machine data with the given one.
    /// The input is a multi-level data.
    pub fn replace(&mut self, level: LeveledMap) {
        let applied = self.map_mut().writable_ref().last_applied_ref();
        let new_applied = level.writable_ref().last_applied_ref();

        assert!(
            new_applied >= applied,
            "the state machine({:?}) can not be replaced with an older one({:?})",
            applied,
            new_applied
        );

        self.levels = level;

        // The installed data may not clean up all expired keys, if it is built with an older state machine.
        // So we need to reset the cursor then the next time applying a log it will clean up all expired.
        self.expire_cursor = ExpireKey::new(0, 0);
    }
}
