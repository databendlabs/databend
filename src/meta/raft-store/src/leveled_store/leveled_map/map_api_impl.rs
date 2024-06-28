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

use std::borrow::Borrow;
use std::fmt;
use std::io;
use std::ops::RangeBounds;

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::KVMeta;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::compacted_get;
use crate::leveled_store::map_api::compacted_range;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapApi;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::map_api::MarkedOf;
use crate::leveled_store::map_api::Transition;
use crate::marked::Marked;

#[async_trait::async_trait]
impl<K> MapApiRO<K> for LeveledMap
where
    K: MapKey + fmt::Debug,
    Level: MapApiRO<K>,
    Immutable: MapApiRO<K>,
    DB: MapApiRO<K>,
{
    async fn get<Q>(&self, key: &Q) -> Result<Marked<K::V>, io::Error>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: MapKeyEncode,
    {
        let levels = self.iter_levels();
        let persisted = self.persisted.as_ref().into_iter();
        compacted_get(key, levels, persisted).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Clone + Send + Sync + 'static {
        let (top, levels) = self.iter_shared_levels();
        let persisted = self.persisted.as_ref().into_iter();
        compacted_range(range, top, levels, persisted).await
    }
}

#[async_trait::async_trait]
impl<K> MapApi<K> for LeveledMap
where
    K: MapKey,
    Level: MapApi<K>,
    Immutable: MapApiRO<K>,
    DB: MapApiRO<K>,
{
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<KVMeta>)>,
    ) -> Result<Transition<MarkedOf<K>>, io::Error>
    where
        K: Ord,
    {
        // Get from the newest level where there is a tombstone or normal record of this key.
        let prev = self.get(&key).await?.clone();

        // No such entry at all, no need to create a tombstone for delete
        if prev.not_found() && value.is_none() {
            return Ok((prev, Marked::new_tombstone(0)));
        }

        // `writeable` is a single level map and the returned `_prev` is only from that level.
        // Therefore it should be ignored and we use the `prev` from the multi-level map.
        let (_prev, inserted) = self.writable.set(key, value).await?;
        Ok((prev, inserted))
    }
}
