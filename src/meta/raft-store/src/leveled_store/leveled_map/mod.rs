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
use std::ops::RangeBounds;
use std::sync::Arc;

use compactor::Compactor;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::Node;
use leveled_map_data::LeveledMapData;
use log::info;
use map_api::mvcc;
use map_api::IOResultStream;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::applier::applier_data::StateMachineView;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::leveled_map::applier_acquirer::WriterPermit;
use crate::leveled_store::leveled_map::compacting_data::CompactingData;
use crate::scoped::Scoped;

#[cfg(test)]
mod acquire_compactor_test;

pub mod applier_acquirer;
pub mod compacting_data;
pub mod compactor;
pub mod compactor_acquirer;
pub mod leveled_map_data;
#[cfg(test)]
mod leveled_map_test;
mod map_api_impl;

/// Similar to leveldb.
///
/// The top level is the newest and writable.
/// Others are immutable.
///
/// - A writer must acquire a permit to write_semaphore.
/// - A compactor must:
///   - acquire the compaction_semaphore first,
///   - then acquire `write_semaphore` to move `writeable` to `immutable_levels`,
#[derive(Debug)]
pub struct LeveledMap {
    pub data: Arc<LeveledMapData>,
}

impl Default for LeveledMap {
    fn default() -> Self {
        Self {
            data: Arc::new(LeveledMapData::default()),
        }
    }
}

impl LeveledMap {
    pub fn to_view(&self) -> StateMachineView {
        mvcc::View::new(self.data.clone())
    }

    pub fn to_scoped_view(&self) -> Scoped<StateMachineView> {
        Scoped::new(self.to_view())
    }

    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        self.data.with_sys_data(f)
    }

    /// Freeze the current writable level and create a new empty writable level.
    ///
    /// Need writer permit and compactor permit
    pub fn freeze_writable(&self, _writer_permit: &mut WriterPermit) -> Arc<ImmutableLevels> {
        self.do_freeze_writable()
    }

    /// For testing, requires no permit
    pub fn testing_freeze_writable(&self) -> Arc<ImmutableLevels> {
        self.do_freeze_writable()
    }

    pub fn do_freeze_writable(&self) -> Arc<ImmutableLevels> {
        let mut inner = self.data.inner();

        let new_writable = inner.writable.new_level();
        let new_immutable = std::mem::replace(&mut inner.writable, new_writable);

        let mut immutable_levels = inner.immutable_levels.as_ref().clone();
        immutable_levels.insert(Immutable::new_from_level(new_immutable));

        inner.immutable_levels = Arc::new(immutable_levels);

        info!(
            "do_freeze_writable: after writable: {:?}, immutables: {:?}",
            inner.writable,
            inner.immutable_levels.indexes()
        );

        inner.immutable_levels.clone()
    }

    pub fn persisted(&self) -> Option<Arc<DB>> {
        self.data.persisted()
    }

    pub fn with_persisted<T>(&self, f: impl FnOnce(&mut Option<Arc<DB>>) -> T) -> T {
        self.data.with_persisted(f)
    }

    /// Return a reference to the immutable levels.
    pub fn immutable_levels(&self) -> Arc<ImmutableLevels> {
        self.data.immutable_levels()
    }

    pub fn curr_seq(&self) -> u64 {
        self.data.with_sys_data(|s| s.curr_seq())
    }

    pub fn last_membership(&self) -> StoredMembership {
        self.data.with_sys_data(|s| s.last_membership_ref().clone())
    }

    pub fn last_applied(&self) -> Option<LogId> {
        self.data.with_sys_data(|s| *s.last_applied_mut())
    }

    pub fn nodes(&self) -> BTreeMap<NodeId, Node> {
        self.data.with_sys_data(|s| s.nodes_mut().clone())
    }

    /// Replace all immutable levels with the given one.
    #[allow(dead_code)]
    pub(crate) fn replace_immutable_levels(&mut self, b: ImmutableLevels) {
        self.data.with_immutable_levels(|x| *x = Arc::new(b));
    }

    /// Replace bottom immutable levels and persisted level with compacted data.
    ///
    /// **Important**: Do not drop the compactor within this function when called
    /// under a state machine lock, as dropping may take ~250ms.
    pub fn replace_with_compacted(&self, compactor: &mut Compactor, db: DB) {
        let upto = compactor.compacting_data.upto;
        let mut immutables = self.data.immutable_levels().as_ref().clone();

        info!(
            "replace_with_compacted: compacted upto {:?} immutable levels; my levels: {:?}; compacted levels: {:?}",
            upto,
            immutables.indexes(),
            compactor.compacting_data.immutable_levels.indexes(),
        );

        // If there is immutable levels compacted, remove them.
        if let Some(upto) = upto {
            immutables.remove_levels_upto(upto);
        }

        self.data.with_inner(|inner| {
            inner.immutable_levels = Arc::new(immutables);
            inner.persisted = Some(Arc::new(db));
        });

        info!("replace_with_compacted: finished replacing the db");
    }

    pub(crate) fn new_compacting_data(&self) -> CompactingData {
        CompactingData::new(self.immutable_levels(), self.persisted())
    }
}

#[derive(Clone, Debug)]
pub struct ReadonlyView {
    base_seq: InternalSeq,
    inner: Arc<LeveledMapData>,
}

impl ReadonlyView {
    pub fn new(inner: Arc<LeveledMapData>) -> Self {
        let base_seq = inner.with_sys_data(|s| s.curr_seq());
        Self {
            base_seq: InternalSeq::new(base_seq),
            inner,
        }
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedViewReadonly<UserKey, MetaValue> for ReadonlyView {
    fn base_seq(&self) -> InternalSeq {
        self.base_seq
    }

    async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        let got = self.inner.compacted_view_get(key, *self.base_seq).await?;
        Ok(got)
    }

    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(UserKey, SeqMarked<MetaValue>)>, io::Error>
    where
        R: RangeBounds<UserKey> + Send + Sync + Clone + 'static,
    {
        let strm = self
            .inner
            .compacted_view_range(range, *self.base_seq)
            .await?;
        Ok(strm)
    }
}
