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

use compactor::Compactor;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use log::info;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::level::Level;
use crate::leveled_store::level_index::LevelIndex;

pub mod compactor;
pub mod compactor_data;
#[cfg(test)]
mod leveled_map_test;
mod map_api_impl;

/// State machine data organized in multiple levels.
///
/// Similar to leveldb.
///
/// The top level is the newest and writable.
/// Others are immutable.
#[derive(Debug, Default)]
pub struct LeveledMap {
    /// Concurrency control: only one thread can set this field to Some and compact.
    ///
    /// The other should wait for the compaction to finish by blocking on the receiver.
    current_compactor: Option<tokio::sync::oneshot::Receiver<()>>,

    /// The top level is the newest and writable.
    writable: Level,

    /// The immutable levels.
    immutable_levels: ImmutableLevels,

    persisted: Option<DB>,
}

impl AsRef<SysData> for LeveledMap {
    fn as_ref(&self) -> &SysData {
        self.writable.sys_data_ref()
    }
}

impl LeveledMap {
    pub(crate) fn clear(&mut self) {
        self.writable = Default::default();
        self.immutable_levels = Default::default();
        self.persisted = None;
    }

    /// Return the [`LevelIndex`] of the newest **immutable** data
    pub(crate) fn immutable_level_index(&self) -> Option<LevelIndex> {
        let newest = self.immutable_levels.newest()?;
        Some(*newest.level_index())
    }

    /// Return an iterator of all levels in reverse order.
    pub(crate) fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        [&self.writable]
            .into_iter()
            .chain(self.immutable_levels.iter_levels())
    }

    /// Return the top level and an iterator of all immutable levels, in newest to oldest order.
    pub(crate) fn iter_shared_levels(&self) -> (Option<&Level>, impl Iterator<Item = &Immutable>) {
        (
            Some(&self.writable),
            self.immutable_levels.iter_immutable_levels(),
        )
    }

    /// Freeze the current writable level and create a new empty writable level.
    pub fn freeze_writable(&mut self) -> &ImmutableLevels {
        let new_writable = self.writable.new_level();

        let immutable = std::mem::replace(&mut self.writable, new_writable);
        self.immutable_levels
            .push(Immutable::new_from_level(immutable));

        &self.immutable_levels
    }

    /// Return an immutable reference to the top level i.e., the writable level.
    pub fn writable_ref(&self) -> &Level {
        &self.writable
    }

    /// Return a mutable reference to the top level i.e., the writable level.
    pub fn writable_mut(&mut self) -> &mut Level {
        &mut self.writable
    }

    pub fn sys_data_mut(&mut self) -> &mut SysData {
        self.writable.sys_data_mut()
    }

    pub fn persisted(&self) -> Option<&DB> {
        self.persisted.as_ref()
    }

    pub fn persisted_mut(&mut self) -> &mut Option<DB> {
        &mut self.persisted
    }

    /// Return a reference to the immutable levels.
    pub fn immutable_levels_ref(&self) -> &ImmutableLevels {
        &self.immutable_levels
    }

    /// Replace all immutable levels with the given one.
    #[allow(dead_code)]
    pub(crate) fn replace_immutable_levels(&mut self, b: ImmutableLevels) {
        self.immutable_levels = b;
    }

    /// Replace several bottom immutable levels and persisted level
    /// with the compacted data.
    pub fn replace_with_compacted(&mut self, mut compactor: Compactor, db: DB) {
        let len = compactor.immutable_levels.len();
        let corresponding_index = compactor
            .immutable_levels
            .levels()
            .get(len - 1)
            .map(|l| l.level_index())
            .copied();

        assert_eq!(
            compactor.since, corresponding_index,
            "unexpected change to sm during compaction"
        );

        let levels = self.immutable_levels.levels();

        let newly_added = levels.split_off(len);
        *levels = newly_added;

        self.persisted = Some(db);

        info!("compaction finished replacing the db");
    }

    /// Try to get a singleton `Compactor` instance specific to `self`
    /// if it is not currently in use by another thread.
    ///
    /// This method requires a mutable reference to prevent concurrent access to shared data,
    /// such as `self.immediate_levels` and `self.persisted`, during the construction of the compactor.
    pub(crate) fn try_acquire_compactor(&mut self) -> Option<Compactor> {
        if self.current_compactor.is_some() {
            // Other compactor is running.
            return None;
        }

        Some(self.new_compactor())
    }

    /// Get a singleton `Compactor` instance specific to `self`.
    ///
    /// This method requires a mutable reference to prevent concurrent access to shared data,
    /// such as `self.immediate_levels` and `self.persisted`, during the construction of the compactor.
    pub(crate) async fn acquire_compactor(&mut self) -> Compactor {
        if let Some(rx) = self.current_compactor.take() {
            let _ = rx.await;
        }

        self.new_compactor()
    }

    fn new_compactor(&mut self) -> Compactor {
        let (tx, rx) = tokio::sync::oneshot::channel();

        // current_compactor must be None, which is guaranteed by caller.
        self.current_compactor = Some(rx);

        Compactor {
            guard: tx,
            immutable_levels: self.immutable_levels.clone(),
            db: self.persisted.clone(),
            since: self.immutable_level_index(),
        }
    }
}
