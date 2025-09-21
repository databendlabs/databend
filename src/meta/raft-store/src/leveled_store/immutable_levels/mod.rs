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
use std::io::Error;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use map_api::mvcc;
use map_api::mvcc::ScopedSeqBoundedIntoRange;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use map_api::util;
use map_api::IOResultStream;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::LevelStat;
use crate::leveled_store::level_index::LevelIndex;
use crate::leveled_store::map_api::MapKey;

mod compact_all;
mod compact_conductor;
mod compact_min_adjacent;

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

    /// Get statistics of all levels from newest to oldest.
    pub fn stat(&self) -> Vec<LevelStat> {
        self.newest_to_oldest()
            .map(|imm| imm.inner().stat().with_index(*imm.level_index()))
            .collect()
    }

    pub(crate) fn indexes(&self) -> Vec<LevelIndex> {
        self.immutables.keys().cloned().collect()
    }

    pub(crate) fn newest_to_oldest(&self) -> impl Iterator<Item = &Immutable> {
        self.immutables.values().rev()
    }

    pub(crate) fn newest(&self) -> Option<&Immutable> {
        self.immutables.values().next_back()
    }

    pub(crate) fn newest_level_index(&self) -> Option<LevelIndex> {
        self.newest().map(|x| *x.level_index())
    }

    pub(crate) fn insert(&mut self, level: Immutable) {
        let new_index = *level.level_index();

        let last_used_index = self.newest_level_index().unwrap_or_default();

        // The newly added must have greater data index or there are no data added.
        assert!(
            new_index > last_used_index
                || (new_index == LevelIndex::default() && new_index == last_used_index),
            "new level to insert {:?} must have greater index than the newest level {:?}",
            new_index,
            last_used_index
        );

        self.immutables.insert(new_index, level);
    }

    /// Remove all levels up to and including the given level index.
    pub(crate) fn remove_levels_upto(&mut self, level_index: LevelIndex) {
        assert!(
            self.immutables.contains_key(&level_index),
            "level_index to remove {:?} must exist",
            level_index
        );

        let mut left = self.immutables.split_off(&level_index);
        // split_off() also returns the given key, remove it.
        left.pop_first();
        self.immutables = left;
    }

    #[allow(dead_code)]
    pub(crate) fn levels_mut(&mut self) -> &mut BTreeMap<LevelIndex, Immutable> {
        &mut self.immutables
    }

    pub(crate) fn last_seq(&self) -> Option<u64> {
        self.newest().map(|l| l.sys_data().curr_seq())
    }
}

// TODO: test
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedRange<K, K::V> for ImmutableLevels
where
    K: MapKey,
    K::V: ViewValue,
    Immutable: mvcc::ScopedSeqBoundedIntoRange<K, K::V>,
{
    async fn range<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let mut kmerge = KMerge::by(util::by_key_seq);

        for level in self.newest_to_oldest() {
            let strm = level
                .clone()
                .into_range(range.clone(), snapshot_seq)
                .await?;

            kmerge = kmerge.merge(strm);
        }

        // Merge entries with the same key, keep the one with larger internal-seq
        let coalesce = kmerge.coalesce(util::merge_kv_results);

        Ok(coalesce.boxed())
    }
}

// TODO: test
#[async_trait::async_trait]
impl<K, V> mvcc::ScopedSeqBoundedGet<K, V> for ImmutableLevels
where
    K: ViewKey,
    V: ViewValue,
    Immutable: mvcc::ScopedSeqBoundedGet<K, V>,
{
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<V>, Error> {
        for immutable in self.newest_to_oldest() {
            let value = immutable.get(key.clone(), snapshot_seq).await?;

            if !value.is_not_found() {
                return Ok(value);
            }
        }

        Ok(SeqMarked::new_not_found())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::StreamExt;
    use map_api::mvcc;
    use map_api::mvcc::ScopedSeqBoundedRange;
    use seq_marked::SeqMarked;
    use state_machine_api::ExpireKey;
    use state_machine_api::MetaValue;
    use state_machine_api::UserKey;

    use crate::leveled_store::immutable::Immutable;
    use crate::leveled_store::immutable_levels::ImmutableLevels;
    use crate::leveled_store::level::Level;

    fn assert_scoped_snapshot_range_traits<T>()
    where
        T: mvcc::ScopedSeqBoundedRange<UserKey, MetaValue>,
        T: mvcc::ScopedSeqBoundedRange<ExpireKey, String>,
    {
    }

    #[test]
    fn test_scoped_snapshot_range_iter() {
        assert_scoped_snapshot_range_traits::<ImmutableLevels>();
    }

    /// Helper function to convert string to bytes (similar to existing code pattern)
    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    /// Helper to create a Level with test data
    fn create_level_with_data(entries: Vec<(UserKey, SeqMarked<MetaValue>)>) -> Level {
        let mut level = Level::default();
        let mut max_seq = 0;

        for (key, seq_marked) in entries {
            let seq = *seq_marked.internal_seq();

            if seq_marked.is_tombstone() {
                level.kv.insert_tombstone(key, seq).unwrap();
            } else if let Some(value) = seq_marked.into_data() {
                level.kv.insert(key, seq, value).unwrap();
            }
            max_seq = max_seq.max(seq);
        }

        // Update sys_data to reflect the maximum sequence number
        if max_seq > 0 {
            level.with_sys_data(|s| s.update_seq(max_seq));
        }

        level
    }

    #[test]
    fn test_remove_levels_upto() {
        let mut immutables = ImmutableLevels::new_form_iter(vec![
            Immutable::new(Arc::new(Level::default())),
            Immutable::new(Arc::new(Level::default())),
            Immutable::new(Arc::new(Level::default())),
        ]);

        let index = immutables.indexes()[1];
        let left_index = immutables.indexes()[2];

        immutables.remove_levels_upto(index);

        assert_eq!(immutables.indexes(), vec![left_index]);
    }

    #[tokio::test]
    async fn test_into_range_with_multiple_levels_and_tombstones() {
        // Setup 3 levels with overlapping sequence ranges

        // Level 1 (oldest) - seq 10-30
        let level1_entries = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("old_value1"))),
            ),
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(15, (None, b("old_value2"))),
            ),
            (
                UserKey::new("key5"),
                SeqMarked::new_normal(25, (None, b("value5_old"))),
            ),
            (
                UserKey::new("key7"),
                SeqMarked::new_normal(30, (None, b("value7"))),
            ),
        ];
        let level1 = create_level_with_data(level1_entries);
        let immutable1 = Immutable::new(Arc::new(level1));

        // Level 2 (middle) - seq 20-40 (overlaps with level1 and level3)
        let level2_entries = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(22, (None, b("updated_value1"))),
            ), // overwrites key1 from level1
            (UserKey::new("key3"), SeqMarked::new_tombstone(27)), // tombstone for key3
            (
                UserKey::new("key4"),
                SeqMarked::new_normal(32, (None, b("value4"))),
            ),
            (
                UserKey::new("key5"),
                SeqMarked::new_normal(35, (None, b("value5_updated"))),
            ), // overwrites key5 from level1
        ];
        let level2 = create_level_with_data(level2_entries);
        let immutable2 = Immutable::new(Arc::new(level2));

        // Level 3 (newest) - seq 30-50 (overlaps with level2)
        let level3_entries = vec![
            (UserKey::new("key2"), SeqMarked::new_tombstone(38)), /* tombstone for key2 (overrides old value) */
            (
                UserKey::new("key4"),
                SeqMarked::new_normal(42, (None, b("value4_newest"))),
            ), // overwrites key4 from level2
            (
                UserKey::new("key6"),
                SeqMarked::new_normal(45, (None, b("value6"))),
            ),
            (
                UserKey::new("key8"),
                SeqMarked::new_normal(50, (None, b("value8"))),
            ),
        ];
        let level3 = create_level_with_data(level3_entries);
        let immutable3 = Immutable::new(Arc::new(level3));

        // Create ImmutableLevels with all 3 levels
        let mut levels = ImmutableLevels::default();
        levels.insert(immutable1);
        levels.insert(immutable2);
        levels.insert(immutable3);

        // Test 1: snapshot_seq = 20 (should see only data with seq <= 20)
        let stream = levels.range(UserKey::default().., 20).await.unwrap();
        let items: Vec<_> = stream.collect().await;
        let results: Vec<_> = items.into_iter().map(|r| r.unwrap()).collect();

        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("old_value1"))),
            ),
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(15, (None, b("old_value2"))),
            ),
        ];
        assert_eq!(results, expected);

        // Test 2: snapshot_seq = 30 (newer data from level2 overwrites level1)
        let stream = levels.range(UserKey::default().., 30).await.unwrap();
        let items: Vec<_> = stream.collect().await;
        let results: Vec<_> = items.into_iter().map(|r| r.unwrap()).collect();

        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(22, (None, b("updated_value1"))),
            ), // level2 overwrites level1
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(15, (None, b("old_value2"))),
            ),
            (UserKey::new("key3"), SeqMarked::new_tombstone(27)),
            (
                UserKey::new("key5"),
                SeqMarked::new_normal(25, (None, b("value5_old"))),
            ),
            (
                UserKey::new("key7"),
                SeqMarked::new_normal(30, (None, b("value7"))),
            ),
        ];
        assert_eq!(results, expected);

        // Test 3: snapshot_seq = 40 (includes data from all levels up to seq 40)
        let stream = levels.range(UserKey::default().., 40).await.unwrap();
        let items: Vec<_> = stream.collect().await;
        let results: Vec<_> = items.into_iter().map(|r| r.unwrap()).collect();

        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(22, (None, b("updated_value1"))),
            ),
            (UserKey::new("key2"), SeqMarked::new_tombstone(38)), /* level3 tombstone overwrites level1 */
            (UserKey::new("key3"), SeqMarked::new_tombstone(27)),
            (
                UserKey::new("key4"),
                SeqMarked::new_normal(32, (None, b("value4"))),
            ),
            (
                UserKey::new("key5"),
                SeqMarked::new_normal(35, (None, b("value5_updated"))),
            ), // level2 overwrites level1
            (
                UserKey::new("key7"),
                SeqMarked::new_normal(30, (None, b("value7"))),
            ),
        ];
        assert_eq!(results, expected);

        // Test 4: snapshot_seq = 50 (all data including newest overwrites)
        let stream = levels.range(UserKey::default().., 50).await.unwrap();
        let items: Vec<_> = stream.collect().await;
        let results: Vec<_> = items.into_iter().map(|r| r.unwrap()).collect();

        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(22, (None, b("updated_value1"))),
            ),
            (UserKey::new("key2"), SeqMarked::new_tombstone(38)),
            (UserKey::new("key3"), SeqMarked::new_tombstone(27)),
            (
                UserKey::new("key4"),
                SeqMarked::new_normal(42, (None, b("value4_newest"))),
            ), // level3 overwrites level2
            (
                UserKey::new("key5"),
                SeqMarked::new_normal(35, (None, b("value5_updated"))),
            ),
            (
                UserKey::new("key6"),
                SeqMarked::new_normal(45, (None, b("value6"))),
            ),
            (
                UserKey::new("key7"),
                SeqMarked::new_normal(30, (None, b("value7"))),
            ),
            (
                UserKey::new("key8"),
                SeqMarked::new_normal(50, (None, b("value8"))),
            ),
        ];
        assert_eq!(results, expected);

        // Test 5: Range query for specific keys with snapshot_seq = 40
        let stream = levels
            .range(UserKey::new("key2")..=UserKey::new("key5"), 40)
            .await
            .unwrap();
        let items: Vec<_> = stream.collect().await;
        let results: Vec<_> = items.into_iter().map(|r| r.unwrap()).collect();

        let expected = vec![
            (UserKey::new("key2"), SeqMarked::new_tombstone(38)),
            (UserKey::new("key3"), SeqMarked::new_tombstone(27)),
            (
                UserKey::new("key4"),
                SeqMarked::new_normal(32, (None, b("value4"))),
            ),
            (
                UserKey::new("key5"),
                SeqMarked::new_normal(35, (None, b("value5_updated"))),
            ),
        ];
        assert_eq!(results, expected);

        // Test 6: Early snapshot_seq = 12 (should only see key1)
        let stream = levels.range(UserKey::default().., 12).await.unwrap();
        let items: Vec<_> = stream.collect().await;
        let results: Vec<_> = items.into_iter().map(|r| r.unwrap()).collect();

        let expected = vec![(
            UserKey::new("key1"),
            SeqMarked::new_normal(10, (None, b("old_value1"))),
        )];
        assert_eq!(results, expected);
    }
}
