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

use std::fmt;
use std::fmt::Formatter;
use std::io;
use std::io::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::StorageError;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::AppliedState;
use log::debug;
use map_api::mvcc;
use map_api::mvcc::ScopedViewReadonly;
use openraft::entry::RaftEntry;
use state_machine_api::ExpireKey;
use state_machine_api::SeqV;
use state_machine_api::StateMachineApi;
use state_machine_api::UserKey;
use tokio::sync::Semaphore;

use crate::applier::applier_data::ApplierData;
use crate::applier::Applier;
use crate::leveled_store::leveled_map::applier_acquirer::WriterAcquirer;
use crate::leveled_store::leveled_map::applier_acquirer::WriterPermit;
use crate::leveled_store::leveled_map::compactor::Compactor;
use crate::leveled_store::leveled_map::compactor_acquirer::CompactorAcquirer;
use crate::leveled_store::leveled_map::compactor_acquirer::CompactorPermit;
use crate::leveled_store::leveled_map::leveled_map_data::LeveledMapData;
use crate::leveled_store::leveled_map::leveled_map_data::LeveledMapDataInner;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::scoped::Scoped;
use crate::sm_v003::sm_v003_kv_api::SMV003KVApi;

pub type OnChange = Box<dyn Fn((String, Option<SeqV>, Option<SeqV>)) + Send + Sync>;

pub struct SMV003 {
    levels: LeveledMap,

    /// A semaphore that permits at most one compactor to run.
    pub(crate) compaction_semaphore: Arc<Semaphore>,

    /// Get a permit to write.
    ///
    /// Only one writer is allowed, to achieve serialization.
    /// For historical reason, inserting a tombstone does not increase the seq.
    /// Thus, mvcc isolation with the seq can not completely separate two concurrent writer.
    pub(crate) write_semaphore: Arc<Semaphore>,

    /// Since when to start cleaning expired keys.
    cleanup_start_time: Arc<Mutex<Duration>>,

    /// Callback when a change is applied to state machine
    pub(crate) on_change_applied: Arc<Mutex<Arc<Option<OnChange>>>>,
}

impl Default for SMV003 {
    fn default() -> Self {
        Self {
            levels: Default::default(),
            // Only one compactor is allowed a time.
            compaction_semaphore: Arc::new(Semaphore::new(1)),
            // Only one writer is allowed a time.
            write_semaphore: Arc::new(Semaphore::new(1)),
            cleanup_start_time: Arc::new(Mutex::new(Duration::ZERO)),
            on_change_applied: Arc::new(Mutex::new(Arc::new(None))),
        }
    }
}

impl fmt::Debug for SMV003 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SMV003")
            .field("levels", &self.levels)
            .field(
                "on_change_applied",
                &self
                    .get_on_change_applied()
                    .as_ref()
                    .as_ref()
                    .map(|_x| "is_set"),
            )
            .finish()
    }
}

#[async_trait::async_trait]
impl StateMachineApi<SysData> for ApplierData {
    type UserMap = ApplierData;

    fn user_map(&self) -> &Self::UserMap {
        self
    }

    fn user_map_mut(&mut self) -> &mut Self::UserMap {
        self
    }

    type ExpireMap = ApplierData;

    fn expire_map(&self) -> &Self::ExpireMap {
        self
    }

    fn expire_map_mut(&mut self) -> &mut Self::ExpireMap {
        self
    }

    fn on_change_applied(&mut self, change: (String, Option<SeqV>, Option<SeqV>)) {
        let Some(on_change_applied) = self.on_change_applied.as_ref().as_ref() else {
            // No subscribers, do nothing.
            return;
        };
        (*on_change_applied)(change);
    }

    fn with_cleanup_start_timestamp<T>(&self, f: impl FnOnce(&mut Duration) -> T) -> T {
        let mut ts = self.cleanup_start_time.lock().unwrap();
        f(&mut ts)
    }

    fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        self.view.base().with_sys_data(f)
    }

    async fn commit(self) -> Result<(), Error> {
        debug!("SMV003::commit: start");
        self.view.commit().await?;
        Ok(())
    }
}

impl SMV003 {
    /// Return a mutable reference to the map that stores app data.
    pub(in crate::sm_v003) fn map_mut(&mut self) -> &mut LeveledMap {
        &mut self.levels
    }

    pub fn expire_view_readonly(&self) -> impl mvcc::ScopedViewReadonly<ExpireKey, String> {
        Scoped::new(mvcc::View::new(self.levels.data.clone()))
    }
}

impl SMV003 {
    pub fn kv_api(&self) -> SMV003KVApi {
        SMV003KVApi { sm: self }
    }

    /// Install and replace state machine with the content of a snapshot.
    pub fn install_snapshot_v003(&self, db: DB) -> SMV003 {
        let sys_data = db.sys_data().clone();

        let new_sm = SMV003 {
            levels: LeveledMap {
                data: Arc::new(LeveledMapData {
                    inner: Mutex::new(LeveledMapDataInner {
                        writable: Default::default(),
                        immutable_levels: Default::default(),
                        persisted: Some(Arc::new(db)),
                    }),
                }),
            },
            compaction_semaphore: self.compaction_semaphore.clone(),
            write_semaphore: self.write_semaphore.clone(),
            cleanup_start_time: self.cleanup_start_time.clone(),
            on_change_applied: self.on_change_applied.clone(),
        };

        new_sm.levels.with_sys_data(|s| *s = sys_data);

        new_sm
    }

    pub fn get_snapshot(&self) -> Option<DB> {
        self.levels.persisted().map(|x| x.as_ref().clone())
    }

    pub fn data(&self) -> &Arc<LeveledMapData> {
        &self.levels.data
    }

    pub async fn get_maybe_expired_kv(&self, key: &str) -> Result<Option<SeqV>, io::Error> {
        let view = self.data().to_readonly_view();
        let got = view.get(UserKey::new(key.to_string())).await?;
        let seqv = Into::<Option<SeqV>>::into(got);
        Ok(seqv)
    }

    pub(crate) async fn new_applier(&self) -> Applier<ApplierData> {
        let permit = self.acquire_writer_permit().await;

        let view = mvcc::View::new(self.levels.data.clone());
        let applier_data = ApplierData {
            _permit: permit,
            view,
            cleanup_start_time: self.cleanup_start_time.clone(),
            on_change_applied: self.get_on_change_applied(),
        };

        Applier::new(applier_data)
    }

    pub fn get_on_change_applied(&self) -> Arc<Option<OnChange>> {
        self.on_change_applied.lock().unwrap().clone()
    }

    pub async fn acquire_writer_permit(&self) -> WriterPermit {
        let acquirer = self.new_writer_acquirer();
        let permit = acquirer.acquire().await;
        debug!("WriterPermit acquired");
        permit
    }

    pub fn new_writer_acquirer(&self) -> WriterAcquirer {
        WriterAcquirer::new(self.write_semaphore.clone())
    }

    pub async fn apply_entries(
        &self,
        entries: impl IntoIterator<Item = Entry>,
    ) -> Result<Vec<AppliedState>, StorageError> {
        let mut applier = self.new_applier().await;

        let mut res = vec![];

        for ent in entries.into_iter() {
            let log_id = ent.log_id();
            let r = applier
                .apply(&ent)
                .await
                .map_err(|e| StorageError::apply(log_id, &e))?;
            res.push(r);
        }

        applier
            .commit()
            .await
            .map_err(|e| StorageError::write(&e))?;

        Ok(res)
    }

    pub fn sys_data(&self) -> SysData {
        self.levels.with_sys_data(|x| x.clone())
    }

    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        self.levels.with_sys_data(f)
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

    pub fn set_on_change_applied(&self, on_change_applied: OnChange) {
        let mut g = self.on_change_applied.lock().unwrap();
        *g = Arc::new(Some(on_change_applied));
    }

    /// Get a singleton `Compactor` instance specific to `self`.
    pub async fn acquire_compactor(&self) -> Compactor {
        let permit = self.new_compactor_acquirer().acquire().await;
        self.new_compactor(permit)
    }

    pub fn new_compactor(&self, permit: CompactorPermit) -> Compactor {
        Compactor {
            _permit: permit,
            compacting_data: self.levels.new_compacting_data(),
        }
    }

    pub fn new_compactor_acquirer(&self) -> CompactorAcquirer {
        CompactorAcquirer::new(self.compaction_semaphore.clone())
    }
}
