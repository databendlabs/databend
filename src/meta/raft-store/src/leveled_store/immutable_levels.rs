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

use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;

use map_api::compact::compacted_get;
use map_api::compact::compacted_range;
use map_api::map_api_ro::MapApiRO;
use seq_marked::SeqMarked;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::Level;
use crate::leveled_store::level_index::LevelIndex;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapKey;

/// A readonly leveled map that owns the data.
#[derive(Debug, Default, Clone)]
pub struct ImmutableLevels {
    /// From oldest to newest, i.e., first is the oldest
    immutables: BTreeMap<LevelIndex, Immutable>,
}

impl ImmutableLevels {
    pub(crate) fn new_form_iter(immutables: impl IntoIterator<Item = Immutable>) -> Self {
        Self {
            immutables: immutables
                .into_iter()
                .map(|immu| (*immu.level_index(), immu))
                .collect(),
        }
    }

    pub(crate) fn indexes(&self) -> impl Iterator<Item = LevelIndex> + use<'_> {
        self.immutables.keys().cloned()
    }

    /// Return an iterator of all Arc of levels from newest to oldest.
    pub(crate) fn iter_immutable_levels(&self) -> impl Iterator<Item = &Immutable> {
        self.immutables.values().rev()
    }

    /// Return an iterator of all levels from newest to oldest.
    pub(crate) fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        self.immutables.values().map(|x| x.as_ref()).rev()
    }

    pub(crate) fn newest(&self) -> Option<&Immutable> {
        self.immutables.values().next_back()
    }

    pub(crate) fn newest_level_index(&self) -> Option<LevelIndex> {
        self.newest().map(|x| *x.level_index())
    }

    pub(crate) fn insert(&mut self, level: Immutable) {
        let key = *level.level_index();

        let last_key = self.newest_level_index().unwrap_or_default();

        assert!(
            key > last_key,
            "new level to insert {:?} must have greater index than the newest level {:?}",
            key,
            last_key
        );

        self.immutables.insert(key, level);
    }

    /// Remove all levels up to and including the given level index.
    pub(crate) fn remove_levels_upto(&mut self, level_index: LevelIndex) {
        assert!(
            self.immutables.contains_key(&level_index),
            "level_index to remove {:?} must exist",
            level_index
        );
        let left = self.immutables.split_off(&level_index);
        self.immutables = left;
    }

    #[allow(dead_code)]
    pub(crate) fn levels_mut(&mut self) -> &mut BTreeMap<LevelIndex, Immutable> {
        &mut self.immutables
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for ImmutableLevels
where
    K: MapKey,
    Level: MapApiRO<K>,
    Immutable: MapApiRO<K>,
{
    async fn get(&self, key: &K) -> Result<SeqMarked<K::V>, io::Error> {
        let levels = self.iter_immutable_levels();
        compacted_get::<_, _, Immutable>(key, levels, []).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Clone + Send + Sync + 'static {
        let levels = self.iter_immutable_levels();
        compacted_range::<_, _, Level, _, Level>(range, None, levels, []).await
    }
}
