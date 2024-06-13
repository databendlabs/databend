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

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use rotbl::v001::SeqMarked;

use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::level_index::LevelIndex;
use crate::leveled_store::leveled_map::compacting_data::CompactingData;
use crate::leveled_store::map_api::IOResultStream;

/// Compactor is responsible for compacting the immutable levels and db.
///
/// Only one Compactor can be running at a time.
pub struct Compactor {
    /// When dropped, drop the Sender so that the [`LeveledMap`] will be notified.
    #[allow(dead_code)]
    pub(super) guard: tokio::sync::oneshot::Sender<()>,

    /// In memory immutable levels.
    pub(super) immutable_levels: ImmutableLevels,

    /// Persisted data.
    pub(super) db: Option<DB>,

    /// Remember the newest level included in this compactor.
    pub(super) since: Option<LevelIndex>,
}

impl Compactor {
    pub fn immutable_levels(&self) -> &ImmutableLevels {
        &self.immutable_levels
    }

    pub fn db(&self) -> Option<&DB> {
        self.db.as_ref()
    }

    /// Compact in-memory immutable levels(excluding on disk db)
    /// into one level and keep tombstone record.
    ///
    /// When compact mem levels, do not remove tombstone,
    /// because tombstones are still required when compacting with the underlying db.
    pub async fn compact_immutable_in_place(&mut self) -> Result<(), io::Error> {
        let mut compacting_data = CompactingData::new(&mut self.immutable_levels, self.db.as_ref());
        compacting_data.compact_immutable_in_place().await
    }

    /// Compacted all data into a stream.
    ///
    /// Tombstones are removed because no more compact with lower levels.
    ///
    /// It returns a small chunk of sys data that is always copied across levels,
    /// and a stream contains `kv` and `expire` entries.
    ///
    /// The exported stream contains encoded `String` key and rotbl value [`SeqMarked`]
    pub async fn compact(
        &mut self,
    ) -> Result<(SysData, IOResultStream<(String, SeqMarked)>), io::Error> {
        let compacting_data = CompactingData::new(&mut self.immutable_levels, self.db.as_ref());
        compacting_data.compact().await
    }
}
