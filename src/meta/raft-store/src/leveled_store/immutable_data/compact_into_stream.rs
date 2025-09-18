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

use databend_common_meta_types::sys_data::SysData;
use futures_util::future;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use map_api::mvcc::ScopedSeqBoundedRange;
use map_api::IOResultStream;
use map_api::MapKV;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::UserKey;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::immutable_data::ImmutableData;
use crate::leveled_store::rotbl_codec::RotblCodec;
use crate::leveled_store::util;
use crate::utils::add_cooperative_yielding;

impl ImmutableData {
    /// Compacted all data into a stream.
    ///
    /// Tombstones are removed because no more compact with lower levels.
    ///
    /// It returns a small chunk of sys data that is always copied across levels,
    /// and a stream contains `kv` and `expire` entries.
    /// The stream Item is 2 items tuple of key, and value with seq.
    ///
    /// The exported stream contains encoded `String` key and rotbl value [`SeqMarked`]
    // TODO: mvcc snapshot_seq
    pub async fn compact_into_stream(
        &self,
    ) -> Result<(SysData, IOResultStream<(String, SeqMarked)>), io::Error> {
        fn with_context(e: io::Error, key: &impl fmt::Debug) -> io::Error {
            io::Error::new(
                e.kind(),
                format!("{}, while encoding kv, key: {:?}", e, key),
            )
        }

        let immutable_levels = self.levels();
        let d = immutable_levels.newest().unwrap();

        let sys_data = d.sys_data().clone();

        // expire index: prefix `exp-/`.

        let strm = immutable_levels
            .range(ExpireKey::default().., u64::MAX)
            .await?;
        let expire_strm = strm.map(|item: Result<(ExpireKey, SeqMarked<String>), io::Error>| {
            let (expire_key, marked_string) = item?;

            RotblCodec::encode_key_seq_marked(&expire_key, marked_string)
                .map_err(|e| with_context(e, &expire_key))
        });

        // kv: prefix: `kv--/`

        let strm = immutable_levels
            .range(UserKey::default().., u64::MAX)
            .await?;
        let kv_strm = strm.map(|item: Result<MapKV<UserKey>, io::Error>| {
            let (k, v) = item?;

            RotblCodec::encode_key_seq_marked(&k, v).map_err(|e| with_context(e, &k))
        });

        // `exp-/*` < `kv--/`: expire keys should be output first to maintain the order.
        let strm = expire_strm.chain(kv_strm);

        let mut kmerge = KMerge::by(util::rotbl_by_key_seq);
        kmerge = kmerge.merge(strm);

        if let Some(db) = self.persisted() {
            let db_strm = db.inner_range();
            kmerge = kmerge.merge(db_strm);
        }

        let coalesce = kmerge.coalesce(util::rotbl_choose_greater);

        // Filter out tombstone
        let normal_strm = coalesce.try_filter(|(_k, v)| future::ready(v.is_normal()));

        let normal_strm = add_cooperative_yielding(normal_strm, "compact");

        Ok((sys_data, normal_strm.boxed()))
    }
}
