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
use std::ops::RangeBounds;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::Level;
use crate::leveled_store::map_api::compacted_get;
use crate::leveled_store::map_api::compacted_range;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::marked::Marked;

/// A readonly leveled map that owns the data.
#[derive(Debug, Default, Clone)]
pub struct ImmutableLevels {
    /// From oldest to newest, i.e., levels[0] is the oldest
    levels: Vec<Immutable>,
}

impl ImmutableLevels {
    pub(crate) fn new(levels: impl IntoIterator<Item = Immutable>) -> Self {
        Self {
            levels: levels.into_iter().collect(),
        }
    }

    /// Return an iterator of all Arc of levels from newest to oldest.
    pub(crate) fn iter_immutable_levels(&self) -> impl Iterator<Item = &Immutable> {
        self.levels.iter().rev()
    }

    /// Return an iterator of all levels from newest to oldest.
    pub(crate) fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        self.levels.iter().map(|x| x.as_ref()).rev()
    }

    pub(crate) fn newest(&self) -> Option<&Immutable> {
        self.levels.last()
    }

    pub(crate) fn push(&mut self, level: Immutable) {
        self.levels.push(level);
    }

    pub(crate) fn len(&self) -> usize {
        self.levels.len()
    }
    pub(crate) fn levels(&mut self) -> &mut Vec<Immutable> {
        &mut self.levels
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for ImmutableLevels
where
    K: MapKey,
    K: MapKeyEncode,
    K: MapKeyDecode,
    Level: MapApiRO<K>,
    Immutable: MapApiRO<K>,
{
    async fn get(&self, key: &K) -> Result<Marked<K::V>, io::Error> {
        let levels = self.iter_immutable_levels();
        compacted_get::<_, _, Immutable>(key, levels, []).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Clone + Send + Sync + 'static {
        let levels = self.iter_immutable_levels();
        compacted_range::<_, _, _, Level, Level>(range, None, levels, []).await
    }
}
