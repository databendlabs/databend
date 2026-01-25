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

use map_api::MapKey;
use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use seq_marked::SeqMarked;

use crate::leveled_store::get_sub_table::GetSubTable;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::persisted_codec::PersistedCodec;

// TODO: test it
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedGet<K, K::V> for LeveledMap
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: PersistedCodec<SeqMarked>,
    Level: GetSubTable<K, K::V>,
    Immutable: mvcc::ScopedSeqBoundedGet<K, K::V>,
{
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<K::V>, io::Error> {
        let immutable = {
            let inner = self.data.lock().unwrap();
            let got = inner
                .writable
                .get_sub_table()
                .get(key.clone(), snapshot_seq)
                .cloned();
            if !got.is_not_found() {
                return Ok(got);
            }

            inner.immutable.clone()
        };

        immutable.get(key, snapshot_seq).await
    }
}
