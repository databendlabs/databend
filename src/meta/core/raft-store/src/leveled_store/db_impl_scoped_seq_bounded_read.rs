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
use std::ops::RangeBounds;

use databend_common_meta_types::snapshot_db::DB;
use futures_util::StreamExt;
use map_api::IOResultStream;
use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use rotbl::v001::SeqMarked;

use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::persisted_codec::PersistedCodec;
use crate::leveled_store::rotbl_codec::RotblCodec;

/// A wrapper that implements the `ScopedSnapshot*` trait for the `DB`.
#[derive(Debug, Clone)]
pub struct ScopedSeqBoundedRead<'a>(pub &'a DB);

// TODO: test
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedGet<K, K::V> for ScopedSeqBoundedRead<'_>
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
{
    async fn get(&self, key: K, _snapshot_seq: u64) -> Result<SeqMarked<K::V>, io::Error> {
        // TODO: DB does not consider snapshot_seq
        let key = RotblCodec::encode_key(&key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let res = self.0.rotbl.get(&key).await?;

        let Some(seq_marked) = res else {
            return Ok(SeqMarked::new_not_found());
        };

        let marked = SeqMarked::<K::V>::decode_from(seq_marked)?;
        Ok(marked)
    }
}

// TODO: test
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedRange<K, K::V> for ScopedSeqBoundedRead<'_>
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
{
    async fn range<R>(
        &self,
        range: R,
        _snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        // TODO: DB does not consider snapshot_seq

        let rng = RotblCodec::encode_range(&range)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let strm = self.0.rotbl.range(rng);

        let strm = strm.map(|res_item: Result<(String, SeqMarked), io::Error>| {
            let (str_k, seq_marked) = res_item?;
            let key = RotblCodec::decode_key(&str_k)?;
            let marked = SeqMarked::decode_from(seq_marked)?;
            Ok((key, marked))
        });

        Ok(strm.boxed())
    }
}
