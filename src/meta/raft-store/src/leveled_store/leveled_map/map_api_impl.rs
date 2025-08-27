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
use std::ops::RangeBounds;

use map_api::map_api_ro::MapApiRO;
use seq_marked::SeqMarked;

use crate::leveled_store::db_map_api_ro_impl::MapView;
use crate::leveled_store::level::GetTable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::leveled_map::LeveledMapData;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::value_convert::ValueConvert;

#[async_trait::async_trait]
impl<K> MapApiRO<K> for LeveledMapData
where
    K: MapKey + fmt::Debug,
    K: MapKeyEncode,
    K: MapKeyDecode,
    SeqMarked<K::V>: ValueConvert<SeqMarked>,
    Level: GetTable<K, K::V>,
    <K as MapKey>::V: fmt::Debug,
    for<'a> MapView<'a>: MapApiRO<K>,
{
    async fn get(&self, key: &K) -> Result<SeqMarked<K::V>, io::Error> {
        self.compacted_view_get(key.clone(), u64::MAX).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Clone + Send + Sync + 'static {
        self.compacted_view_range(range, u64::MAX).await
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for LeveledMap
where
    K: MapKey + fmt::Debug,
    K: MapKeyEncode,
    K: MapKeyDecode,
    SeqMarked<K::V>: ValueConvert<SeqMarked>,
    Level: GetTable<K, K::V>,
    <K as MapKey>::V: fmt::Debug,
    for<'a> MapView<'a>: MapApiRO<K>,
{
    async fn get(&self, key: &K) -> Result<SeqMarked<K::V>, io::Error> {
        self.data.get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Clone + Send + Sync + 'static {
        self.data.range(range).await
    }
}

// #[async_trait::async_trait]
// impl<K> MapApi<K> for LeveledMap
// where
//     K: MapKey,
//     K: MapKeyEncode,
//     K: MapKeyDecode,
//     Level: MapApi<K>,
//     Immutable: MapApiRO<K>,
//     for<'a> MapView<'a>: MapApiRO<K>,
// {
//     async fn set(
//         &mut self,
//         key: K,
//         value: Option<K::V>,
//     ) -> Result<BeforeAfter<SeqMarkedOf<K>>, io::Error>
//     where
//         K: Ord,
//     {
//         // Get from the newest level where there is a tombstone or normal record of this key.
//         let prev = self.get(&key).await?.clone();
//
//         // No such entry at all, no need to create a tombstone for delete
//         if prev.is_not_found() && value.is_none() {
//             return Ok((prev, SeqMarked::new_tombstone(0)));
//         }
//
//         // `writeable` is a single level map and the returned `_prev` is only from that level.
//         // Therefore, it should be ignored and we use the `prev` from the multi-level map.
//         let (_prev, inserted) = self.writable.set(key, value).await?;
//         Ok((prev, inserted))
//     }
// }
