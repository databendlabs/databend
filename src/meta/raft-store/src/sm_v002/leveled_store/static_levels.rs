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
use std::ops::RangeBounds;
use std::sync::Arc;

use futures_util::stream::BoxStream;

use crate::sm_v002::leveled_store::level::Level;
use crate::sm_v002::leveled_store::map_api::compacted_get;
use crate::sm_v002::leveled_store::map_api::compacted_range;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::leveled_store::ref_::Ref;
use crate::sm_v002::marked::Marked;

/// A readonly leveled map that owns the data.
#[derive(Debug, Default, Clone)]
pub struct StaticLevels {
    /// From oldest to newest, i.e., levels[0] is the oldest
    levels: Vec<Arc<Level>>,
}

impl StaticLevels {
    pub(in crate::sm_v002) fn new(levels: impl IntoIterator<Item = Arc<Level>>) -> Self {
        Self {
            levels: levels.into_iter().collect(),
        }
    }

    /// Return an iterator of all levels from newest to oldest.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        self.levels.iter().map(|x| x.as_ref()).rev()
    }

    pub(in crate::sm_v002) fn newest(&self) -> Option<&Arc<Level>> {
        self.levels.last()
    }

    pub(in crate::sm_v002) fn push(&mut self, level: Arc<Level>) {
        self.levels.push(level);
    }

    pub(in crate::sm_v002) fn len(&self) -> usize {
        self.levels.len()
    }

    #[allow(dead_code)]
    pub(in crate::sm_v002) fn to_ref(&self) -> Ref<'_> {
        Ref::new(None, self)
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for StaticLevels
where
    K: MapKey,
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

    async fn range<'f, Q, R>(&'f self, range: R) -> BoxStream<'f, (K, Marked<K::V>)>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync,
    {
        let levels = self.iter_levels();
        compacted_range(range, levels).await
    }
}
