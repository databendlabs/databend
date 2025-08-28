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
use std::io::Error;
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use map_api::mvcc;
use map_api::mvcc::ScopedSnapshotIntoRange;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use map_api::IOResultStream;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::level::GetTable;
use crate::leveled_store::level::Level;
use crate::leveled_store::level_index::LevelIndex;

/// A single **immutable** level of state machine data.
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

impl AsRef<mvcc::Table<UserKey, MetaValue>> for Immutable {
    fn as_ref(&self) -> &mvcc::Table<UserKey, MetaValue> {
        self.level.as_ref().as_ref()
    }
}

impl AsRef<mvcc::Table<ExpireKey, String>> for Immutable {
    fn as_ref(&self) -> &mvcc::Table<ExpireKey, String> {
        self.level.as_ref().as_ref()
    }
}

impl Deref for Immutable {
    type Target = Level;

    fn deref(&self) -> &Self::Target {
        self.level.as_ref()
    }
}

// TODO: Test
#[async_trait::async_trait]
impl<K, V> mvcc::ScopedSeqBoundedGet<K, V> for Immutable
where
    K: ViewKey,
    V: ViewValue,
    Level: GetTable<K, V>,
{
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<V>, io::Error> {
        let seq_marked = self.level.get_table().get(key, snapshot_seq).cloned();
        Ok(seq_marked)
    }
}

// TODO: Test
#[async_trait::async_trait]
impl<K, V> mvcc::ScopedSeqBoundedRange<K, V> for Immutable
where
    K: ViewKey,
    V: ViewValue,
    Immutable: AsRef<mvcc::Table<K, V>>,
{
    async fn range<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        self.clone().into_range(range, snapshot_seq).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_scoped_snapshot_range_traits<T>()
    where
        T: mvcc::ScopedSeqBoundedRangeIter<UserKey, MetaValue>,
        T: mvcc::ScopedSeqBoundedRangeIter<ExpireKey, String>,
        T: mvcc::ScopedSnapshotIntoRange<UserKey, MetaValue>,
        T: mvcc::ScopedSnapshotIntoRange<ExpireKey, String>,
        T: mvcc::ScopedSeqBoundedRange<UserKey, MetaValue>,
        T: mvcc::ScopedSeqBoundedRange<ExpireKey, String>,
    {
    }

    #[test]
    fn test_scoped_snapshot_range_iter() {
        assert_scoped_snapshot_range_traits::<Immutable>();
    }
}
