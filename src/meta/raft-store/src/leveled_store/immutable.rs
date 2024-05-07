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
use std::io;
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::leveled_store::level::Level;
use crate::leveled_store::level_index::LevelIndex;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::map_api::MapKV;
use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::map_api::MarkedOf;
use crate::marked::Marked;
use crate::state_machine::ExpireKey;

/// A single **immutable** level of state machine data.
///
/// Immutable level implement only [`MapApiRO`], but not [`MapApi`].
///
/// [`MapApi`]: crate::sm_v003::leveled_store::map_api::MapApi
#[derive(Debug, Clone)]
pub struct Immutable {
    index: LevelIndex,
    level: Arc<Level>,
}

impl Immutable {
    pub fn new(level: Arc<Level>) -> Self {
        static UNIQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let uniq = UNIQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let internal_seq = level.sys_data_ref().curr_seq();

        let index = LevelIndex::new(internal_seq, uniq);

        Self { index, level }
    }

    pub fn new_from_level(level: Level) -> Self {
        Self::new(Arc::new(level))
    }

    pub fn inner(&self) -> &Arc<Level> {
        &self.level
    }

    pub fn level_index(&self) -> &LevelIndex {
        &self.index
    }
}

impl AsRef<Level> for Immutable {
    fn as_ref(&self) -> &Level {
        self.level.as_ref()
    }
}

impl Deref for Immutable {
    type Target = Level;

    fn deref(&self) -> &Self::Target {
        self.level.as_ref()
    }
}

impl Immutable {
    /// Build a static stream that yields key values for primary index
    #[futures_async_stream::try_stream(boxed, ok = MapKV<String>, error = io::Error)]
    async fn str_range<Q, R>(self: Immutable, range: R)
    where
        String: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync + 'static,
    {
        let it = self.as_ref().kv.range(range);

        for (k, v) in it {
            yield (k.clone(), v.clone());
        }
    }

    /// Build a static stream that yields expire key and key for the secondary expiration index
    #[futures_async_stream::try_stream(boxed, ok = MapKV<ExpireKey>, error = io::Error)]
    async fn expire_range<Q, R>(self: Immutable, range: R)
    where
        ExpireKey: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync + 'static,
    {
        let it = self.as_ref().expire.range(range);

        for (k, v) in it {
            yield (*k, v.clone());
        }
    }
}

#[async_trait::async_trait]
impl MapApiRO<String> for Immutable {
    async fn get<Q>(&self, key: &Q) -> Result<Marked<<String as MapKey>::V>, io::Error>
    where
        String: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: MapKeyEncode,
    {
        // get() is just delegated
        self.as_ref().str_map().get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<String>, io::Error>
    where R: RangeBounds<String> + Clone + Send + Sync + 'static {
        let strm = self.clone().str_range(range);
        Ok(strm)
    }
}

#[async_trait::async_trait]
impl MapApiRO<ExpireKey> for Immutable {
    async fn get<Q>(&self, key: &Q) -> Result<MarkedOf<ExpireKey>, io::Error>
    where
        ExpireKey: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: MapKeyEncode,
    {
        // get() is just delegated
        self.as_ref().expire_map().get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<ExpireKey>, io::Error>
    where R: RangeBounds<ExpireKey> + Clone + Send + Sync + 'static {
        let strm = self.clone().expire_range(range);
        Ok(strm)
    }
}
