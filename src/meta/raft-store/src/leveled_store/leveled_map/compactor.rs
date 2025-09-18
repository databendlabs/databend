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
use std::sync::Arc;

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use map_api::IOResultStream;
use rotbl::v001::SeqMarked;

use crate::leveled_store::immutable_data::ImmutableData;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::sm_v003::compactor_acquirer::CompactorPermit;

/// Compactor is responsible for compacting the immutable levels and db.
///
/// Only one Compactor can be running at a time.
#[derive(Debug)]
pub struct Compactor {
    /// Acquired permit for this compactor.
    ///
    /// This is used to ensure that only one compactor can run at a time.
    pub(crate) _permit: CompactorPermit,
    pub(crate) immutable_data: Arc<ImmutableData>,
}

impl Compactor {
    pub fn immutable_data(&self) -> Arc<ImmutableData> {
        self.immutable_data.clone()
    }

    pub fn immutable_levels(&self) -> ImmutableLevels {
        self.immutable_data.levels().clone()
    }

    pub fn db(&self) -> Option<DB> {
        self.immutable_data.persisted().cloned()
    }

    /// Compact in-memory immutable levels(excluding on disk db)
    /// into one level and keep tombstone record.
    ///
    /// When compact mem levels, do not remove tombstone,
    /// because tombstones are still required when compacting with the underlying db.
    pub async fn compact_immutable_in_place(&mut self) -> Result<(), io::Error> {
        let immutable_levels = self.immutable_data.levels().clone();

        let levels = immutable_levels.compact_all().await;
        let immutable = ImmutableData::new(levels, self.immutable_data.persisted().cloned());
        self.immutable_data = Arc::new(immutable);

        Ok(())
    }

    /// Compacted all data into a stream.
    ///
    /// Tombstones are removed because no more compact with lower levels.
    ///
    /// It returns a small chunk of sys data that is always copied across levels,
    /// and a stream contains `kv` and `expire` entries.
    ///
    /// The exported stream contains encoded `String` key and rotbl value [`SeqMarked`]
    pub async fn compact_into_stream(
        &mut self,
    ) -> Result<(SysData, IOResultStream<(String, SeqMarked)>), io::Error> {
        self.immutable_data.compact_into_stream().await
    }
}
