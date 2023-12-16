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
use std::sync::Arc;

use databend_common_meta_types::KVMeta;

use crate::sm_v002::leveled_store::level::Level;
use crate::sm_v002::leveled_store::map_api::compacted_get;
use crate::sm_v002::leveled_store::map_api::compacted_range;
use crate::sm_v002::leveled_store::map_api::KVResultStream;
use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::leveled_store::map_api::MarkedOf;
use crate::sm_v002::leveled_store::map_api::Transition;
use crate::sm_v002::leveled_store::ref_::Ref;
use crate::sm_v002::leveled_store::ref_mut::RefMut;
use crate::sm_v002::leveled_store::static_levels::StaticLevels;
use crate::sm_v002::marked::Marked;

/// State machine data organized in multiple levels.
///
/// Similar to leveldb.
///
/// The top level is the newest and writable.
/// Others are immutable.
#[derive(Debug, Default)]
pub struct LeveledMap {
    /// The top level is the newest and writable.
    writable: Level,

    /// The immutable levels.
    frozen: StaticLevels,
}

impl LeveledMap {
    pub(crate) fn new(writable: Level) -> Self {
        Self {
            writable,
            frozen: Default::default(),
        }
    }

    /// Return an iterator of all levels in reverse order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        [&self.writable]
            .into_iter()
            .chain(self.frozen.iter_levels())
    }

    /// Return the top level and an iterator of all frozen levels, in newest to oldest order.
    pub(in crate::sm_v002) fn iter_shared_levels(
        &self,
    ) -> (Option<&Level>, impl Iterator<Item = &Arc<Level>>) {
        (Some(&self.writable), self.frozen.iter_arc_levels())
    }

    /// Freeze the current writable level and create a new empty writable level.
    pub fn freeze_writable(&mut self) -> &StaticLevels {
        let new_writable = self.writable.new_level();

        let frozen = std::mem::replace(&mut self.writable, new_writable);
        self.frozen.push(Arc::new(frozen));

        &self.frozen
    }

    /// Return an immutable reference to the top level i.e., the writable level.
    pub fn writable_ref(&self) -> &Level {
        &self.writable
    }

    /// Return a mutable reference to the top level i.e., the writable level.
    pub fn writable_mut(&mut self) -> &mut Level {
        &mut self.writable
    }

    /// Return a reference to the immutable levels.
    pub fn frozen_ref(&self) -> &StaticLevels {
        &self.frozen
    }

    /// Replace all immutable levels with the given one.
    pub(crate) fn replace_frozen(&mut self, b: StaticLevels) {
        self.frozen = b;
    }

    pub(crate) fn to_ref_mut(&mut self) -> RefMut {
        RefMut::new(&mut self.writable, &self.frozen)
    }

    #[allow(dead_code)]
    pub(crate) fn to_ref(&self) -> Ref {
        Ref::new(Some(&self.writable), &self.frozen)
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for LeveledMap
where
    K: MapKey + fmt::Debug,
    Level: MapApiRO<K>,
    Arc<Level>: MapApiRO<K>,
{
    async fn get<Q>(&self, key: &Q) -> Result<Marked<K::V>, io::Error>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        let levels = self.iter_levels();
        compacted_get(key, levels).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Clone + Send + Sync + 'static {
        let (top, levels) = self.iter_shared_levels();
        compacted_range(range, top, levels).await
    }
}

#[async_trait::async_trait]
impl<K> MapApi<K> for LeveledMap
where
    K: MapKey,
    Level: MapApi<K>,
    Arc<Level>: MapApiRO<K>,
{
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<KVMeta>)>,
    ) -> Result<Transition<MarkedOf<K>>, io::Error>
    where
        K: Ord,
    {
        let mut l = self.to_ref_mut();
        MapApi::set(&mut l, key, value).await
    }
}
