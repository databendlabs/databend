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
use std::io;

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use futures_util::future;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use rotbl::v001::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::IOResultStream;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::rotbl_codec::RotblCodec;
use crate::leveled_store::util;
use crate::marked::Marked;
use crate::state_machine::ExpireKey;

/// The data to compact.
///
/// Including several in-memory immutable levels and an optional persisted db.
pub(crate) struct CompactingData<'a> {
    /// In memory immutable levels.
    pub(crate) immutable_levels: &'a mut ImmutableLevels,

    /// Persisted data.
    pub(crate) db: Option<&'a DB>,
}

impl<'a> CompactingData<'a> {
    pub fn new(immutable_levels: &'a mut ImmutableLevels, db: Option<&'a DB>) -> Self {
        Self {
            immutable_levels,
            db,
        }
    }

    /// Compact in-memory immutable levels(excluding on disk db)
    /// into one level and keep tombstone record.
    ///
    /// When compact mem levels, do not remove tombstone,
    /// because tombstones are still required when compacting with the underlying db.
    pub async fn compact_immutable_in_place(&mut self) -> Result<(), io::Error> {
        let immutable_levels = &mut *self.immutable_levels;

        let Some(newest) = immutable_levels.newest() else {
            return Ok(());
        };

        // Create an empty level with SysData cloned.
        let mut data = newest.new_level();

        // Copy all expire data and keep tombstone.
        let strm = (*immutable_levels).expire_map().range(..).await?;
        let bt = strm.try_collect().await?;
        data.replace_expire(bt);

        // Copy all kv data and keep tombstone.
        let strm = (*immutable_levels).str_map().range(..).await?;
        let bt = strm.try_collect().await?;
        data.replace_kv(bt);

        *immutable_levels = ImmutableLevels::new([Immutable::new_from_level(data)]);
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
    pub async fn compact(
        &self,
    ) -> Result<(SysData, IOResultStream<(String, SeqMarked)>), io::Error> {
        fn with_context(e: io::Error, key: &impl fmt::Debug) -> io::Error {
            io::Error::new(
                e.kind(),
                format!("{}, while encoding kv, key: {:?}", e, key),
            )
        }

        // TODO(rotbl): unwrap()???
        let d = self.immutable_levels.newest().unwrap();

        let sys_data = d.sys_data_ref().clone();

        // expire index: prefix `exp-/`.

        let strm = (*self.immutable_levels).expire_map().range(..).await?;
        let expire_strm = strm.map(|item: Result<(ExpireKey, Marked<String>), io::Error>| {
            let (k, v) = item?;
            RotblCodec::encode_key_seq_marked(&k, v).map_err(|e| with_context(e, &k))
        });

        // kv: prefix: `kv--/`

        let strm = (*self.immutable_levels).str_map().range(..).await?;
        let kv_strm = strm.map(|item: Result<(String, Marked), io::Error>| {
            let (k, v) = item?;
            RotblCodec::encode_key_seq_marked(&k, v).map_err(|e| with_context(e, &k))
        });

        // `exp-/*` < `kv--/`: expire keys should be output first to maintain the order.
        let strm = expire_strm.chain(kv_strm);

        let mut kmerge = KMerge::by(util::rotbl_by_key_seq);
        kmerge = kmerge.merge(strm);

        if let Some(db) = self.db {
            let db_strm = db.inner_range();
            kmerge = kmerge.merge(db_strm);
        }

        let coalesce = kmerge.coalesce(util::rotbl_choose_greater);

        // Filter out tombstone
        let normal_strm = coalesce.try_filter(|(_k, v)| future::ready(v.is_normal()));

        Ok((sys_data, normal_strm.boxed()))
    }
}
