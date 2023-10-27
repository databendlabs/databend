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
use std::ops::RangeBounds;

use crate::sm_v002::leveled_store::level::Level;
use crate::sm_v002::leveled_store::map_api::compacted_get;
use crate::sm_v002::leveled_store::map_api::compacted_range;
use crate::sm_v002::leveled_store::map_api::EntryStream;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::leveled_store::static_levels::StaticLevels;
use crate::sm_v002::marked::Marked;

/// A readonly leveled map that does not not own the data.
#[derive(Debug)]
pub struct Ref<'d> {
    /// The top level is the newest and writable.
    writable: Option<&'d Level>,

    /// The immutable levels.
    frozen: &'d StaticLevels,
}

impl<'d> Ref<'d> {
    pub(in crate::sm_v002) fn new(
        writable: Option<&'d Level>,
        frozen: &'d StaticLevels,
    ) -> Ref<'d> {
        Self { writable, frozen }
    }

    /// Return an iterator of all levels in reverse order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &'d Level> + 'd {
        self.writable.into_iter().chain(self.frozen.iter_levels())
    }
}

#[async_trait::async_trait]
impl<'d, K> MapApiRO<K> for Ref<'d>
where
    K: MapKey + fmt::Debug,
    Level: MapApiRO<K>,
{
    async fn get<Q>(&self, key: &Q) -> Marked<K::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        let levels = self.iter_levels();
        compacted_get(key, levels).await
    }

    async fn range<Q, R>(&self, range: R) -> EntryStream<K>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q> + Clone + Send + Sync,
        Q: Ord + Send + Sync + ?Sized,
    {
        let levels = self.iter_levels();
        compacted_range(range, levels).await
    }
}
