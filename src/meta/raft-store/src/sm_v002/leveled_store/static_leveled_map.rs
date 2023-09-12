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
use std::sync::Arc;

use futures_util::stream::BoxStream;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::sm_v002::leveled_store::level_data::LevelData;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::util;
use crate::sm_v002::marked::Marked;

#[derive(Debug, Default, Clone)]
pub struct StaticLeveledMap {
    /// From oldest to newest, i.e., levels[0] is the oldest
    levels: Vec<Arc<LevelData>>,
}

impl StaticLeveledMap {
    pub(in crate::sm_v002) fn new(levels: impl IntoIterator<Item = Arc<LevelData>>) -> Self {
        Self {
            levels: levels.into_iter().collect(),
        }
    }

    /// Return an iterator of all levels from newest to oldest.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &LevelData> {
        self.levels.iter().map(|x| x.as_ref()).rev()
    }

    pub(in crate::sm_v002) fn newest(&self) -> Option<&Arc<LevelData>> {
        self.levels.last()
    }

    pub(in crate::sm_v002) fn push(&mut self, level: Arc<LevelData>) {
        self.levels.push(level);
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for StaticLeveledMap
where
    K: Ord + fmt::Debug + Send + Sync + Unpin + 'static,
    LevelData: MapApiRO<K>,
{
    type V = <LevelData as MapApiRO<K>>::V;

    async fn get<Q>(&self, key: &Q) -> Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        for level_data in self.iter_levels() {
            let got = level_data.get(key).await;
            if !got.is_not_found() {
                return got;
            }
        }
        return Marked::empty();
    }

    async fn range<'a, T: ?Sized, R>(&'a self, range: R) -> BoxStream<'a, (K, Marked<Self::V>)>
    where
        K: 'a,
        K: Borrow<T> + Clone,
        Self::V: Unpin,
        T: Ord,
        R: RangeBounds<T> + Clone + Send + Sync,
    {
        let mut km = KMerge::by(util::by_key_seq::<K, Self::V>);

        for api in self.iter_levels() {
            let a = api.range(range.clone()).await;
            km = km.merge(a);
        }

        // keep one of the entries with the same key, which has larger internal-seq
        let m = km.coalesce(util::choose_greater);

        Box::pin(m)
    }
}
