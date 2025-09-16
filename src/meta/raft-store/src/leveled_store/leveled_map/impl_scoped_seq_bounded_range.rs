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

use std::io::Error;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use log::warn;
use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use map_api::util;
use map_api::IOResultStream;
use map_api::MapKey;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::GetTable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::value_convert::ValueConvert;

// TODO: test it
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedRange<K, K::V> for LeveledMap
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: ValueConvert<SeqMarked>,
    Level: GetTable<K, K::V>,
    Immutable: mvcc::ScopedSeqBoundedRange<K, K::V>,
{
    async fn range<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let mut kmerge = KMerge::by(util::by_key_seq);

        // writable level

        let (vec, immutable) = {
            let inner = self.data.lock().unwrap();
            let it = inner
                .writable
                .get_table()
                .range(range.clone(), snapshot_seq);
            let vec = it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>();

            (vec, inner.immutable.clone())
        };

        if vec.len() > 1000 {
            warn!(
                "Level.writable::range(start={:?}, end={:?}) returns big range of len={}",
                range.start_bound(),
                range.end_bound(),
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        kmerge = kmerge.merge(strm);

        let strm = immutable.range(range, snapshot_seq).await?;
        kmerge = kmerge.merge(strm);

        // Merge entries with the same key, keep the one with larger internal-seq
        let coalesce = kmerge.coalesce(util::merge_kv_results);

        Ok(coalesce.boxed())
    }
}
