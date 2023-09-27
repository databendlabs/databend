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
use std::future::Future;
use std::ops::RangeBounds;
use std::sync::Arc;

use futures_util::stream::BoxStream;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::sm_v002::leveled_store::level_data::LevelData;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::map_api::MapKey;
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

    pub(in crate::sm_v002) fn len(&self) -> usize {
        self.levels.len()
    }

    pub(in crate::sm_v002) fn levels(&self) -> &[Arc<LevelData>] {
        &self.levels
    }
}

impl<'d, K> MapApiRO<K> for &'d StaticLeveledMap
where
    K: MapKey,
    for<'him> &'him LevelData: MapApiRO<K>,
{
    type GetFut<'f, Q> = impl Future<Output = Marked<K::V>> + 'f
        where
            Self: 'f,
            K: Borrow<Q>,
            Q: Ord + Send + Sync + ?Sized,
            Q: 'f;

    fn get<'f, Q>(self, key: &'f Q) -> Self::GetFut<'f, Q>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f,
    {
        // TODO: use LeveledRef
        async move {
            for level_data in self.iter_levels() {
                // let got = <&LevelData as MapApiRO<'_, '_, K>>::get(level_data, key).await;
                let got = level_data.get(key).await;
                if !got.is_not_found() {
                    return got;
                }
            }
            Marked::empty()
        }
    }

    type RangeFut<'f, Q, R> = impl Future<Output = BoxStream<'f, (K, Marked<K::V>)>> + 'f
        where
            Self: 'f,
            'd: 'f,
            K: Borrow<Q>,
            R: RangeBounds<Q> + Send + Sync + Clone,
        R:'f,
            Q: Ord + Send + Sync + ?Sized,
            Q: 'f;

    fn range<'f, Q, R>(self, range: R) -> Self::RangeFut<'f, Q, R>
    where
        'd: 'f,
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync,
        R: 'f,
    {
        // TODO: use LeveledRef
        async move {
            let mut km = KMerge::by(util::by_key_seq::<K, K::V>);

            for api in self.iter_levels() {
                let a = api.range(range.clone()).await;
                km = km.merge(a);
            }

            // keep one of the entries with the same key, which has larger internal-seq
            let m = km.coalesce(util::choose_greater);

            let x: BoxStream<'_, (K, Marked<K::V>)> = Box::pin(m);
            x
        }
    }
}
