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

use std::fmt;
use std::fmt::Formatter;
use std::io;
use std::io::Error;
use std::ops::RangeBounds;

use databend_common_meta_types::snapshot_db::DB;
use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;
use futures_util::StreamExt;
use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use map_api::util;
use map_api::IOResultStream;
use map_api::MapKey;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::level::LevelStat;
use crate::leveled_store::level_index::LevelIndex;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::value_convert::ValueConvert;
use crate::leveled_store::ScopedSeqBoundedRead;

mod compact_into_stream;

#[derive(Debug, Clone)]
pub struct ImmutableDataStat {
    pub last_seq: InternalSeq,
    pub last_level_index: Option<LevelIndex>,
    pub immutable_level_stats: Vec<LevelStat>,
    pub persisted: Option<String>,
}

impl fmt::Display for ImmutableDataStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ImmutableDataStat{{last_seq: {}, last_index: {}, immutables: {}, persisted: {}}}",
            *self.last_seq,
            self.last_level_index.display(),
            self.immutable_level_stats.display_n(256),
            self.persisted.display()
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct ImmutableData {
    /// The last sequence of the immutable data.
    #[allow(dead_code)]
    last_seq: InternalSeq,

    /// It is None if it is an empty levels.
    last_level_index: Option<LevelIndex>,

    /// The immutable levels.
    ///
    /// Protected by compactor_semaphore
    levels: ImmutableLevels,

    /// Protected by compactor_semaphore
    persisted: Option<DB>,
}

impl ImmutableData {
    pub(crate) fn new(levels: ImmutableLevels, persisted: Option<DB>) -> Self {
        let base_seq = if let Some(seq) = levels.last_seq() {
            seq
        } else if let Some(db) = &persisted {
            db.last_seq()
        } else {
            0
        };

        let newest_level_index = levels.newest_level_index();

        Self {
            last_seq: InternalSeq::new(base_seq),
            last_level_index: newest_level_index,
            levels,
            persisted,
        }
    }

    pub fn stat(&self) -> ImmutableDataStat {
        ImmutableDataStat {
            last_seq: self.last_seq,
            last_level_index: self.last_level_index,
            immutable_level_stats: self.levels.stat(),
            persisted: self.persisted.as_ref().map(|db| format!("{:?}", db)),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_levels(&self, levels: ImmutableLevels) -> Self {
        Self::new(levels, self.persisted.clone())
    }

    #[allow(dead_code)]
    pub(crate) fn with_persisted(&self, persisted: Option<DB>) -> Self {
        Self::new(self.levels.clone(), persisted)
    }

    #[allow(dead_code)]
    pub(crate) fn last_seq(&self) -> InternalSeq {
        self.last_seq
    }

    pub(crate) fn levels(&self) -> &ImmutableLevels {
        &self.levels
    }

    pub(crate) fn latest_level_index(&self) -> Option<LevelIndex> {
        self.last_level_index
    }

    pub(crate) fn persisted(&self) -> Option<&DB> {
        self.persisted.as_ref()
    }
}

// TODO: test
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedGet<K, K::V> for ImmutableData
where
    K: MapKey,
    K: ViewKey,
    K::V: ViewValue,
    K: MapKeyEncode + MapKeyDecode,
    SeqMarked<K::V>: ValueConvert<SeqMarked>,
    Immutable: mvcc::ScopedSeqBoundedGet<K, K::V>,
{
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<K::V>, Error> {
        let got = self.levels.get(key.clone(), snapshot_seq).await?;
        if !got.is_not_found() {
            return Ok(got);
        }

        let Some(db) = self.persisted() else {
            return Ok(SeqMarked::new_not_found());
        };

        mvcc::ScopedSeqBoundedGet::get(&ScopedSeqBoundedRead(db), key, snapshot_seq).await
    }
}

// TODO: test
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedRange<K, K::V> for ImmutableData
where
    K: MapKey,
    K: ViewKey,
    K::V: ViewValue,
    K: MapKeyEncode + MapKeyDecode,
    SeqMarked<K::V>: ValueConvert<SeqMarked>,
    Immutable: mvcc::ScopedSeqBoundedRange<K, K::V>,
{
    async fn range<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        R: RangeBounds<K> + Clone + Send + Sync + 'static,
    {
        let mut kmerge = KMerge::by(util::by_key_seq);

        for level in self.levels.newest_to_oldest() {
            let strm = level.range(range.clone(), snapshot_seq).await?;

            kmerge = kmerge.merge(strm);
        }

        // Bottom db level

        if let Some(db) = self.persisted() {
            let map_view = ScopedSeqBoundedRead(db);
            // NOTE: we assume a mvcc version won't use a version that is in a persisted db.
            //       Because we need to wait for a mvcc version to release in order to persist a db.
            //       Because when persisting, it may need to remove tombstone permanently.
            let strm =
                mvcc::ScopedSeqBoundedRange::range(&map_view, range.clone(), snapshot_seq).await?;
            kmerge = kmerge.merge(strm);
        };

        // Merge entries with the same key, keep the one with larger internal-seq
        let coalesce = kmerge.coalesce(util::merge_kv_results);

        Ok(coalesce.boxed())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use databend_common_meta_types::snapshot_db::DB;
    use futures_util::TryStreamExt;
    use map_api::mvcc::ScopedSeqBoundedGet;
    use map_api::mvcc::ScopedSeqBoundedRange;
    use rotbl::storage::impls::fs::FsStorage;
    use rotbl::v001::Config;
    use rotbl::v001::Rotbl;
    use rotbl::v001::RotblMeta;
    use seq_marked::SeqMarked;
    use state_machine_api::MetaValue;
    use state_machine_api::UserKey;
    use tempfile;

    use super::*;
    use crate::leveled_store::immutable::Immutable;
    use crate::leveled_store::level::Level;

    /// Helper function to convert to Vec<u8> - simplifies test code
    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().into_bytes()
    }

    /// Helper to create a DB with test data
    fn create_db_with_data(
        tmp_dir: &tempfile::TempDir,
        test_entries: Vec<(UserKey, SeqMarked<MetaValue>)>,
    ) -> Result<DB, Box<dyn std::error::Error>> {
        let storage = FsStorage::new(tmp_dir.path().to_path_buf());
        let config = Config::default();
        let path = "test_rotbl";

        // Convert test entries to proper rotbl format using RotblCodec
        let entries: Result<Vec<_>, _> = test_entries
            .iter()
            .map(|(key, seq_marked)| {
                use crate::leveled_store::rotbl_codec::RotblCodec;
                RotblCodec::encode_key_seq_marked(key, seq_marked.clone())
            })
            .collect();
        let entries = entries?;

        // Find the maximum sequence number from entries
        let max_seq = test_entries
            .iter()
            .map(|(_, seq_marked)| *seq_marked.internal_seq())
            .max()
            .unwrap_or(0);

        let rotbl = Rotbl::create_table(
            storage,
            config,
            path,
            RotblMeta::new(1, "test_data"),
            entries,
        )?;

        // Create sys_data with the correct sequence number
        let mut sys_data = databend_common_meta_types::sys_data::SysData::default();
        if max_seq > 0 {
            sys_data.update_seq(max_seq);
        }

        let db = DB {
            storage_path: tmp_dir.path().to_string_lossy().to_string(),
            rel_path: path.to_string(),
            meta: Default::default(),
            sys_data,
            rotbl: Arc::new(rotbl),
        };
        Ok(db)
    }

    /// Helper to create a Level with test data (handles both normal values and tombstones)
    fn create_level_with_data(entries: Vec<(UserKey, SeqMarked<MetaValue>)>) -> Level {
        let mut level = Level::default();
        let mut max_seq = 0;

        for (key, seq_marked) in entries {
            let seq = *seq_marked.internal_seq();

            if seq_marked.is_tombstone() {
                // Use the proper tombstone insertion method
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

    /// Helper to create ImmutableLevels with test data
    fn create_levels_with_data(entries: Vec<(UserKey, SeqMarked<MetaValue>)>) -> ImmutableLevels {
        if entries.is_empty() {
            return ImmutableLevels::default();
        }

        let level = create_level_with_data(entries);
        let immutable = Immutable::new_from_level(level);

        let mut levels = ImmutableLevels::default();
        levels.insert(immutable);
        levels
    }

    // ==============================================
    // Test Scenario 1: No levels, No DB
    // ==============================================

    #[tokio::test]
    async fn test_no_levels_no_db_snapshot_get() {
        let levels = ImmutableLevels::default();
        let data = ImmutableData::new(levels, None);
        let key = UserKey::new("test");

        // Test with different snapshot_seq values
        let res = data.get(key.clone(), 5).await.unwrap();
        assert!(res.is_not_found());

        let res = data.get(key, 100).await.unwrap();
        assert!(res.is_not_found());
    }

    #[tokio::test]
    async fn test_no_levels_no_db_snapshot_range() {
        let levels = ImmutableLevels::default();
        let data = ImmutableData::new(levels, None);

        // Test with different snapshot_seq values
        let stream = data.range(UserKey::default().., 5).await.unwrap();
        let items: Vec<_> = stream.collect().await;
        assert!(items.is_empty());

        let stream = data.range(UserKey::default().., 100).await.unwrap();
        let items: Vec<_> = stream.collect().await;
        assert!(items.is_empty());
    }

    #[test]
    fn test_no_levels_no_db_accessors() {
        let levels = ImmutableLevels::default();
        let data = ImmutableData::new(levels, None);

        assert_eq!(*data.last_seq, 0);
        assert!(data.last_level_index.is_none());
        assert!(data.persisted().is_none());
    }

    // ==============================================
    // Test Scenario 2: With levels, No DB
    // ==============================================

    #[tokio::test]
    async fn test_with_levels_no_db_snapshot_get() {
        let entries = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(20, (None, b("value2"))),
            ),
        ];
        let levels = create_levels_with_data(entries);
        let data = ImmutableData::new(levels, None);

        let key1 = UserKey::new("key1");
        let key2 = UserKey::new("key2");
        let missing_key = UserKey::new("missing");

        // Test snapshot_seq effects - seq too low (should not find)
        let res = data.get(key1.clone(), 5).await.unwrap();
        assert!(res.is_not_found());

        // Test snapshot_seq effects - seq adequate (should find)
        let res = data.get(key1.clone(), 15).await.unwrap();
        let expected = SeqMarked::new_normal(10, (None, b("value1")));
        assert_eq!(res, expected);

        // Test snapshot_seq effects - seq high (should find)
        let res = data.get(key2, 100).await.unwrap();
        let expected = SeqMarked::new_normal(20, (None, b("value2")));
        assert_eq!(res, expected);

        // Test missing key
        let res = data.get(missing_key, 100).await.unwrap();
        assert!(res.is_not_found());
    }

    #[tokio::test]
    async fn test_with_levels_no_db_snapshot_range() {
        let entries = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(20, (None, b("value2"))),
            ),
        ];
        let levels = create_levels_with_data(entries);
        let data = ImmutableData::new(levels, None);

        // Test snapshot_seq effects - seq too low (should find nothing)
        let stream = data.range(UserKey::default().., 5).await.unwrap();
        let items: Vec<_> = stream.collect().await;
        assert!(items.is_empty());

        // Test snapshot_seq effects - seq adequate (should find some)
        let stream = data.range(UserKey::default().., 15).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![(
            UserKey::new("key1"),
            SeqMarked::new_normal(10, (None, b("value1"))),
        )];
        assert_eq!(items, expected);

        // Test snapshot_seq effects - seq high (should find all)
        let stream = data.range(UserKey::default().., 100).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(20, (None, b("value2"))),
            ),
        ];
        assert_eq!(items, expected);
    }

    #[tokio::test]
    async fn test_with_levels_no_db_accessors() {
        let entries = vec![(
            UserKey::new("key1"),
            SeqMarked::new_normal(10, (None, b("value1"))),
        )];
        let levels = create_levels_with_data(entries);
        let data = ImmutableData::new(levels, None);

        assert_eq!(*data.last_seq, 10); // Should have seq from levels (max of 10)
        assert!(data.last_level_index.is_some());
        assert!(data.persisted().is_none());
    }

    // ==============================================
    // Test Scenario 2b: With levels containing tombstones, No DB
    // ==============================================

    #[tokio::test]
    async fn test_with_levels_tombstones_snapshot_get() {
        let entries = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (UserKey::new("key2"), SeqMarked::new_tombstone(15)),
            (
                UserKey::new("key3"),
                SeqMarked::new_normal(20, (None, b("value3"))),
            ),
        ];
        let levels = create_levels_with_data(entries);
        let data = ImmutableData::new(levels, None);

        let key1 = UserKey::new("key1");
        let key2 = UserKey::new("key2"); // tombstone
        let key3 = UserKey::new("key3");

        // Test snapshot_seq effects - seq adequate for normal value
        let res = data.get(key1, 25).await.unwrap();
        let expected = SeqMarked::new_normal(10, (None, b("value1")));
        assert_eq!(res, expected);

        // Test snapshot_seq effects - tombstone should return the tombstone
        let res = data.get(key2.clone(), 25).await.unwrap();
        let expected = SeqMarked::new_tombstone(15);
        assert_eq!(res, expected);

        // Test snapshot_seq effects - seq too low for tombstone (should not find)
        let res = data.get(key2, 10).await.unwrap();
        assert!(res.is_not_found());

        // Test snapshot_seq effects - normal value after tombstone
        let res = data.get(key3, 25).await.unwrap();
        let expected = SeqMarked::new_normal(20, (None, b("value3")));
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_with_levels_tombstones_snapshot_range() {
        let entries = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (UserKey::new("key2"), SeqMarked::new_tombstone(15)),
            (
                UserKey::new("key3"),
                SeqMarked::new_normal(20, (None, b("value3"))),
            ),
            (UserKey::new("key4"), SeqMarked::new_tombstone(25)),
        ];
        let levels = create_levels_with_data(entries);
        let data = ImmutableData::new(levels, None);

        // Test snapshot_seq = 12 (should find key1 but not key2 tombstone yet)
        let stream = data.range(UserKey::default().., 12).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![(
            UserKey::new("key1"),
            SeqMarked::new_normal(10, (None, b("value1"))),
        )];
        assert_eq!(items, expected);

        // Test snapshot_seq = 18 (should find key1 and key2 tombstone, but not key3 yet)
        let stream = data.range(UserKey::default().., 18).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (UserKey::new("key2"), SeqMarked::new_tombstone(15)),
        ];
        assert_eq!(items, expected);

        // Test snapshot_seq = 22 (should find key1, key2 tombstone, key3)
        let stream = data.range(UserKey::default().., 22).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (UserKey::new("key2"), SeqMarked::new_tombstone(15)),
            (
                UserKey::new("key3"),
                SeqMarked::new_normal(20, (None, b("value3"))),
            ),
        ];
        assert_eq!(items, expected);

        // Test snapshot_seq = 30 (should find all: key1, key2 tombstone, key3, key4 tombstone)
        let stream = data.range(UserKey::default().., 30).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (UserKey::new("key2"), SeqMarked::new_tombstone(15)),
            (
                UserKey::new("key3"),
                SeqMarked::new_normal(20, (None, b("value3"))),
            ),
            (UserKey::new("key4"), SeqMarked::new_tombstone(25)),
        ];
        assert_eq!(items, expected);
    }

    // ==============================================
    // Test Scenario 3: No levels, With DB
    // ==============================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_no_levels_with_db_snapshot_get() {
        let _tmp_dir = tempfile::tempdir().unwrap();
        let db_entries = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(20, (None, b("value2"))),
            ),
        ];
        let db = create_db_with_data(&_tmp_dir, db_entries).unwrap();

        let levels = ImmutableLevels::default();
        let data = ImmutableData::new(levels, Some(db));

        let key1 = UserKey::new("key1");
        let key_missing = UserKey::new("missing");

        // Test snapshot_seq effects - seq adequate (should find from DB)
        let res = data.get(key1, 25).await.unwrap();
        let expected = SeqMarked::new_normal(10, (None, b("value1")));
        assert_eq!(res, expected);

        // Test missing key
        let res = data.get(key_missing, 100).await.unwrap();
        assert!(res.is_not_found());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_no_levels_with_db_snapshot_range() {
        let _tmp_dir = tempfile::tempdir().unwrap();
        let db_entries = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(20, (None, b("value2"))),
            ),
        ];
        let db = create_db_with_data(&_tmp_dir, db_entries).unwrap();

        let levels = ImmutableLevels::default();
        let data = ImmutableData::new(levels, Some(db));

        // Test snapshot_seq effects - seq adequate (should find from DB)
        let stream = data.range(UserKey::default().., 100).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key1"),
                SeqMarked::new_normal(10, (None, b("value1"))),
            ),
            (
                UserKey::new("key2"),
                SeqMarked::new_normal(20, (None, b("value2"))),
            ),
        ];
        assert_eq!(items, expected);
    }

    #[test]
    fn test_no_levels_with_db_accessors() {
        let _tmp_dir = tempfile::tempdir().unwrap();
        let db_entries = vec![(
            UserKey::new("key1"),
            SeqMarked::new_normal(10, (None, b("value1"))),
        )];
        let db = create_db_with_data(&_tmp_dir, db_entries).unwrap();

        let levels = ImmutableLevels::default();
        let data = ImmutableData::new(levels, Some(db));

        assert_eq!(*data.last_seq, 10); // Should use DB sequence since levels are empty
        assert!(data.last_level_index.is_none()); // No levels
        assert!(data.persisted().is_some()); // Has DB
    }

    // ==============================================
    // Test Scenario 4: With levels, With DB
    // ==============================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_with_levels_with_db_snapshot_get() {
        let _tmp_dir = tempfile::tempdir().unwrap();

        // Create DB with data at seq 2, 4 (all compacted data has low seq numbers)
        let db_entries = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
        ];
        let db = create_db_with_data(&_tmp_dir, db_entries).unwrap();

        // Create levels with data at seq 10, 20
        let level_entries = vec![
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
            (
                UserKey::new("key_level2"),
                SeqMarked::new_normal(20, (None, b("level_value2"))),
            ),
        ];
        let levels = create_levels_with_data(level_entries);
        let data = ImmutableData::new(levels, Some(db));

        let key_db1 = UserKey::new("key_db1");
        let key_level1 = UserKey::new("key_level1");
        let key_missing = UserKey::new("missing");

        // Test snapshot_seq = 7 (should find DB keys at seq 2,4 but not level key at seq 10)
        let res = data.get(key_db1.clone(), 7).await.unwrap();
        let expected = SeqMarked::new_normal(2, (None, b("db_value1")));
        assert_eq!(res, expected);

        let res = data.get(key_level1.clone(), 7).await.unwrap();
        assert!(res.is_not_found());

        // Test snapshot_seq = 12 (should find both DB key at seq 2 and level key at seq 10)
        let res = data.get(key_db1, 12).await.unwrap();
        let expected = SeqMarked::new_normal(2, (None, b("db_value1")));
        assert_eq!(res, expected);

        let res = data.get(key_level1, 12).await.unwrap();
        let expected = SeqMarked::new_normal(10, (None, b("level_value1")));
        assert_eq!(res, expected);

        // Test missing key
        let res = data.get(key_missing, 100).await.unwrap();
        assert!(res.is_not_found());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_with_levels_with_db_snapshot_range() {
        let _tmp_dir = tempfile::tempdir().unwrap();

        // Create DB with data at seq 2, 4 (all compacted data has low seq numbers)
        let db_entries = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
        ];
        let db = create_db_with_data(&_tmp_dir, db_entries).unwrap();

        // Create levels with data at seq 10, 20
        let level_entries = vec![
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
            (
                UserKey::new("key_level2"),
                SeqMarked::new_normal(20, (None, b("level_value2"))),
            ),
        ];
        let levels = create_levels_with_data(level_entries);
        let data = ImmutableData::new(levels, Some(db));

        // Test snapshot_seq = 7 (should find 2 DB items at seq 2,4)
        let stream = data.range(UserKey::default().., 7).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
        ];
        assert_eq!(items, expected);

        // Test snapshot_seq = 12 (should find 2 DB + 1 level item)
        let stream = data.range(UserKey::default().., 12).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
        ];
        assert_eq!(items, expected);

        // Test snapshot_seq = 100 (should find all: 2 DB + 2 level items)
        let stream = data.range(UserKey::default().., 100).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
            (
                UserKey::new("key_level2"),
                SeqMarked::new_normal(20, (None, b("level_value2"))),
            ),
        ];
        assert_eq!(items, expected);
    }

    #[test]
    fn test_with_levels_with_db_accessors() {
        let _tmp_dir = tempfile::tempdir().unwrap();
        let db_entries = vec![(
            UserKey::new("key_db"),
            SeqMarked::new_normal(5, (None, b("db_value"))),
        )];
        let db = create_db_with_data(&_tmp_dir, db_entries).unwrap();

        let level_entries = vec![(
            UserKey::new("key_level"),
            SeqMarked::new_normal(10, (None, b("level_value"))),
        )];
        let levels = create_levels_with_data(level_entries);
        let data = ImmutableData::new(levels, Some(db));

        assert_eq!(*data.last_seq, 10); // Should have seq from levels (max of 10)
        assert!(data.last_level_index.is_some()); // Has levels
        assert!(data.persisted().is_some()); // Has DB
    }

    // ==============================================
    // Test Scenario 4b: With levels containing tombstones, With DB
    // ==============================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_with_levels_tombstones_with_db_snapshot_get() {
        let _tmp_dir = tempfile::tempdir().unwrap();

        // Create DB with data at seq 2, 4 (all compacted data has low seq numbers)
        let db_entries = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
        ];
        let db = create_db_with_data(&_tmp_dir, db_entries).unwrap();

        // Create levels with data and tombstones at seq 10, 15, 20
        let level_entries = vec![
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
            (UserKey::new("key_level2"), SeqMarked::new_tombstone(15)),
            (
                UserKey::new("key_level3"),
                SeqMarked::new_normal(20, (None, b("level_value3"))),
            ),
        ];
        let levels = create_levels_with_data(level_entries);
        let data = ImmutableData::new(levels, Some(db));

        let key_db1 = UserKey::new("key_db1");
        let key_level1 = UserKey::new("key_level1");
        let key_level2 = UserKey::new("key_level2"); // tombstone
        let key_level3 = UserKey::new("key_level3");

        // Test snapshot_seq = 7 (should find DB keys but not level keys)
        let res = data.get(key_db1, 7).await.unwrap();
        let expected = SeqMarked::new_normal(2, (None, b("db_value1")));
        assert_eq!(res, expected);

        let res = data.get(key_level1.clone(), 7).await.unwrap();
        assert!(res.is_not_found());

        // Test snapshot_seq = 12 (should find DB + level1, but not tombstone level2)
        let res = data.get(key_level1, 12).await.unwrap();
        let expected = SeqMarked::new_normal(10, (None, b("level_value1")));
        assert_eq!(res, expected);

        // Test tombstone - should return the tombstone with adequate snapshot_seq
        let res = data.get(key_level2, 25).await.unwrap();
        let expected = SeqMarked::new_tombstone(15);
        assert_eq!(res, expected);

        // Test snapshot_seq = 25 (should find DB + level1 + level3)
        let res = data.get(key_level3, 25).await.unwrap();
        let expected = SeqMarked::new_normal(20, (None, b("level_value3")));
        assert_eq!(res, expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_with_levels_tombstones_with_db_snapshot_range() {
        let _tmp_dir = tempfile::tempdir().unwrap();

        // Create DB with data at seq 2, 4
        let db_entries = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
        ];
        let db = create_db_with_data(&_tmp_dir, db_entries).unwrap();

        // Create levels with normal values and tombstones
        let level_entries = vec![
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
            (UserKey::new("key_level2"), SeqMarked::new_tombstone(15)),
            (
                UserKey::new("key_level3"),
                SeqMarked::new_normal(20, (None, b("level_value3"))),
            ),
        ];
        let levels = create_levels_with_data(level_entries);
        let data = ImmutableData::new(levels, Some(db));

        // Test snapshot_seq = 7 (should find 2 DB items, no level items)
        let stream = data.range(UserKey::default().., 7).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
        ];
        assert_eq!(items, expected);

        // Test snapshot_seq = 12 (should find 2 DB + 1 level item, no tombstone yet)
        let stream = data.range(UserKey::default().., 12).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
        ];
        assert_eq!(items, expected);

        // Test snapshot_seq = 18 (should find 2 DB + 1 level + 1 tombstone)
        let stream = data.range(UserKey::default().., 18).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
            (UserKey::new("key_level2"), SeqMarked::new_tombstone(15)),
        ];
        assert_eq!(items, expected);

        // Test snapshot_seq = 25 (should find all: 2 DB + 2 normal level items + 1 tombstone)
        let stream = data.range(UserKey::default().., 25).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        let expected = vec![
            (
                UserKey::new("key_db1"),
                SeqMarked::new_normal(2, (None, b("db_value1"))),
            ),
            (
                UserKey::new("key_db2"),
                SeqMarked::new_normal(4, (None, b("db_value2"))),
            ),
            (
                UserKey::new("key_level1"),
                SeqMarked::new_normal(10, (None, b("level_value1"))),
            ),
            (UserKey::new("key_level2"), SeqMarked::new_tombstone(15)),
            (
                UserKey::new("key_level3"),
                SeqMarked::new_normal(20, (None, b("level_value3"))),
            ),
        ];
        assert_eq!(items, expected);
    }

    // ==============================================
    // Additional utility tests
    // ==============================================

    #[test]
    fn test_immutable_data_debug() {
        let levels = ImmutableLevels::default();
        let data = ImmutableData::new(levels, None);

        let debug_str = format!("{:?}", data);
        assert!(debug_str.contains("ImmutableData"));
    }

    #[test]
    fn test_immutable_data_clone() {
        let levels = ImmutableLevels::default();
        let data = ImmutableData::new(levels, None);

        let cloned = data.clone();
        assert_eq!(*cloned.last_seq, *data.last_seq);
        assert_eq!(cloned.last_level_index, data.last_level_index);
    }

    #[test]
    fn test_immutable_data_default() {
        let data = ImmutableData::default();
        assert_eq!(*data.last_seq, 0);
        assert!(data.last_level_index.is_none());
        assert!(data.persisted().is_none());
    }
}
