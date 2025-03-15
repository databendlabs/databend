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

use databend_common_meta_types::seq_value::KVMeta;
use map_api::compact::compacted_get;
use map_api::compact::compacted_range;
use map_api::map_api::MapApi;
use map_api::map_api_ro::MapApiRO;
use map_api::BeforeAfter;

use crate::leveled_store::db_map_api_ro_impl::MapView;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::map_api::MarkedOf;
use crate::marked::Marked;

#[async_trait::async_trait]
impl<K> MapApiRO<K, KVMeta> for LeveledMap
where
    K: MapKey<KVMeta> + fmt::Debug,
    K: MapKeyEncode,
    K: MapKeyDecode,
    Level: MapApiRO<K, KVMeta>,
    Immutable: MapApiRO<K, KVMeta>,
    for<'a> MapView<'a>: MapApiRO<K, KVMeta>,
{
    async fn get(&self, key: &K) -> Result<Marked<K::V>, io::Error> {
        let levels = self.iter_levels();
        let persisted = self.persisted.as_ref().map(MapView).into_iter();
        compacted_get(key, levels, persisted).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Clone + Send + Sync + 'static {
        let (top, levels) = self.iter_shared_levels();
        let persisted = self.persisted.as_ref().map(MapView).into_iter();
        compacted_range(range, top, levels, persisted).await
    }
}

#[async_trait::async_trait]
impl<K> MapApi<K, KVMeta> for LeveledMap
where
    K: MapKey<KVMeta>,
    K: MapKeyEncode,
    K: MapKeyDecode,
    Level: MapApi<K, KVMeta>,
    Immutable: MapApiRO<K, KVMeta>,
    for<'a> MapView<'a>: MapApiRO<K, KVMeta>,
{
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<KVMeta>)>,
    ) -> Result<BeforeAfter<MarkedOf<K>>, io::Error>
    where
        K: Ord,
    {
        // Get from the newest level where there is a tombstone or normal record of this key.
        let prev = self.get(&key).await?.clone();

        // No such entry at all, no need to create a tombstone for delete
        if prev.is_not_found() && value.is_none() {
            return Ok((prev, Marked::new_tombstone(0)));
        }

        // `writeable` is a single level map and the returned `_prev` is only from that level.
        // Therefore, it should be ignored and we use the `prev` from the multi-level map.
        let (_prev, inserted) = self.writable.set(key, value).await?;
        Ok((prev, inserted))
    }
}
