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
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use map_api::map_api_ro::MapApiRO;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::level::Level;
use crate::leveled_store::level_index::LevelIndex;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapKV;
use crate::leveled_store::map_api::SeqMarkedOf;

/// A single **immutable** level of state machine data.
///
/// Immutable level implement only [`MapApiRO`], but not [`MapApi`].
///
/// [`MapApi`]: crate::sm_v003::leveled_store::map_api::MapApi
#[derive(Debug, Clone)]
pub struct Immutable {
    /// An in-process unique to identify this immutable level.
    ///
    /// It is used to assert a immutable level is not replaced after compaction.
    index: LevelIndex,
    level: Arc<Level>,
}

impl Immutable {
    pub fn new(level: Arc<Level>) -> Self {
        static UNIQ: AtomicU64 = AtomicU64::new(0);

        let uniq = UNIQ.fetch_add(1, Ordering::Relaxed);
        let internal_seq = level.with_sys_data(|s| s.curr_seq());

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
    #[futures_async_stream::try_stream(boxed, ok = MapKV<UserKey>, error = io::Error)]
    async fn user_range<R>(self: Immutable, range: R)
    where R: RangeBounds<UserKey> + Clone + Send + Sync + 'static {
        let it = self.as_ref().kv.range(range, u64::MAX);

        for (k, v) in it {
            yield (k.clone(), v.cloned());
        }
    }

    /// Build a static stream that yields expire key and key for the secondary expiration index
    #[futures_async_stream::try_stream(boxed, ok = MapKV<ExpireKey>, error = io::Error)]
    async fn expire_range<R>(self: Immutable, range: R)
    where R: RangeBounds<ExpireKey> + Clone + Send + Sync + 'static {
        let it = self.as_ref().expire.range(range, u64::MAX);

        for (k, v) in it {
            yield (*k, v.cloned());
        }
    }
}

#[async_trait::async_trait]
impl MapApiRO<UserKey> for Immutable {
    async fn get(&self, key: &UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        // get() is just delegated
        self.as_ref().as_user_map().get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<UserKey>, io::Error>
    where R: RangeBounds<UserKey> + Clone + Send + Sync + 'static {
        let strm = self.clone().user_range(range);
        Ok(strm)
    }
}

#[async_trait::async_trait]
impl MapApiRO<ExpireKey> for Immutable {
    async fn get(&self, key: &ExpireKey) -> Result<SeqMarkedOf<ExpireKey>, io::Error> {
        // get() is just delegated
        self.as_ref().as_expire_map().get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<ExpireKey>, io::Error>
    where R: RangeBounds<ExpireKey> + Clone + Send + Sync + 'static {
        let strm = self.clone().expire_range(range);
        Ok(strm)
    }
}
