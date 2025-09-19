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

use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::Node;
use futures_util::StreamExt;
use map_api::mvcc;
use map_api::mvcc::ScopedSeqBoundedRangeIter;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use map_api::IOResultStream;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::get_sub_table::GetSubTable;

/// A single level of state machine data.
///
/// State machine data is composed of multiple levels.
#[derive(Debug, Default)]
pub struct Level {
    /// System data(non-user data).
    pub(crate) sys_data: SysData,

    /// Generic Key-value store.
    pub(crate) kv: mvcc::Table<UserKey, MetaValue>,

    /// The expiration queue of generic kv.
    pub(crate) expire: mvcc::Table<ExpireKey, String>,
}

impl AsRef<mvcc::Table<UserKey, MetaValue>> for Level {
    fn as_ref(&self) -> &mvcc::Table<UserKey, MetaValue> {
        &self.kv
    }
}

impl AsRef<mvcc::Table<ExpireKey, String>> for Level {
    fn as_ref(&self) -> &mvcc::Table<ExpireKey, String> {
        &self.expire
    }
}

impl GetSubTable<UserKey, MetaValue> for Level {
    fn get_sub_table(&self) -> &mvcc::Table<UserKey, MetaValue> {
        &self.kv
    }
}

#[async_trait::async_trait]
impl GetSubTable<ExpireKey, String> for Level {
    fn get_sub_table(&self) -> &mvcc::Table<ExpireKey, String> {
        &self.expire
    }
}

#[async_trait::async_trait]
impl<K, V> mvcc::ScopedSeqBoundedRange<K, V> for Level
where
    K: ViewKey,
    V: ViewValue,
    Level: mvcc::ScopedSeqBoundedRangeIter<K, V>,
{
    async fn range<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let vec = self
            .range_iter(range, snapshot_seq)
            .map(|(k, v)| Ok((k.clone(), v.cloned())))
            .collect::<Vec<_>>();

        let strm = futures::stream::iter(vec);
        Ok(strm.boxed())
    }
}

impl Level {
    /// Create a new level that is based on this level.
    pub(crate) fn new_level(&self) -> Self {
        let mut level = Self {
            // sys data are cloned
            sys_data: self.sys_data.clone(),

            // Large data set is referenced.
            kv: Default::default(),
            expire: Default::default(),
        };

        level.with_sys_data(|s| s.incr_data_seq());

        level
    }

    pub(crate) fn with_sys_data<T>(&mut self, f: impl FnOnce(&mut SysData) -> T) -> T {
        f(&mut self.sys_data)
    }

    pub(crate) fn sys_data(&self) -> &SysData {
        &self.sys_data
    }

    /// Replace the kv store with a new one.
    pub(crate) fn replace_kv(&mut self, kv: mvcc::Table<UserKey, MetaValue>) {
        self.kv = kv;
    }

    /// Replace the expiry queue with a new one.
    pub(crate) fn replace_expire(&mut self, expire: mvcc::Table<ExpireKey, String>) {
        self.expire = expire;
    }

    pub fn last_membership(&self) -> StoredMembership {
        self.sys_data().last_membership_ref().clone()
    }

    pub fn last_applied(&self) -> Option<LogId> {
        *self.sys_data().last_applied()
    }

    pub fn nodes(&self) -> BTreeMap<NodeId, Node> {
        self.sys_data().nodes().clone()
    }

    /// Merge two levels into a new one.
    ///
    /// Given the seq of the known minimum snapshot, we can remove older versions if the newer version is smaller than
    /// this seq.
    pub fn compact(&self, other: &Self, min_snapshot_seq: InternalSeq) -> Self {
        let sys_data = if self.sys_data().curr_seq() > other.sys_data().curr_seq() {
            self.sys_data().clone()
        } else {
            other.sys_data().clone()
        };

        Level {
            sys_data,
            kv: mvcc::Table::compact(&self.kv, &other.kv, min_snapshot_seq),
            expire: mvcc::Table::compact(&self.expire, &other.expire, min_snapshot_seq),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_level_detach_sys_data() {
        let mut level1 = Level::default();
        let mut level2 = level1.new_level();
        level1.with_sys_data(|s: &mut SysData| {
            s.update_seq(1);
        });
        assert_eq!(level1.with_sys_data(|s| s.curr_seq()), 1);

        level2.with_sys_data(|s: &mut SysData| {
            s.update_seq(10);
        });
        assert_eq!(level1.with_sys_data(|s| s.curr_seq()), 1);
    }

    // Helper function to compare levels
    fn assert_level_eq(actual: &Level, expected: &Level, test_name: &str) {
        assert_eq!(
            actual.sys_data().curr_seq(),
            expected.sys_data().curr_seq(),
            "{}: sys_data curr_seq mismatch",
            test_name
        );

        // Compare key-value data by checking the internal data directly

        let got = actual.kv.inner.clone().into_iter().collect::<Vec<_>>();
        let want = expected.kv.inner.clone().into_iter().collect::<Vec<_>>();

        assert_eq!(got, want);

        let got = actual.expire.inner.clone().into_iter().collect::<Vec<_>>();
        let want = expected
            .expire
            .inner
            .clone()
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(got, want);
    }

    #[test]
    fn test_merge_coalesces_same_key_with_tombstones() {
        // Helper function to create Vec<u8> values
        fn b(x: impl ToString) -> Vec<u8> {
            x.to_string().as_bytes().to_vec()
        }

        // Create two levels for merging
        let mut level1 = Level::default();
        let mut level2 = Level::default();

        // Set up different sequence numbers for sys_data
        level1.with_sys_data(|s| s.update_seq(100));
        level2.with_sys_data(|s| s.update_seq(200));

        // Create test keys and values
        let k1 = || UserKey::new("k1");
        let k2 = || UserKey::new("k2");
        let v1 = || (None, b("v1"));
        let v2 = || (None, b("v2"));
        let v3 = || (None, b("v3"));

        // Populate level1 with initial data
        level1.kv.insert(k1(), 10, v1()).unwrap();
        level1.kv.insert(k2(), 15, v3()).unwrap();

        // Populate level2 with updates and tombstone
        level2.kv.insert(k1(), 20, v2()).unwrap();
        level2.kv.insert_tombstone(k1(), 30).unwrap(); // Tombstone at seq 30

        // Test 1: Merge with min_snapshot_seq = 0 (all versions preserved)
        {
            let got = level1.compact(&level2, InternalSeq::new(0));

            let mut want = Level::default();
            want.with_sys_data(|s| s.update_seq(200)); // Higher seq wins
                                                       // Insert in sequence order: 10, 15, 20, 30
            want.kv.insert(k1(), 10, v1()).unwrap();
            want.kv.insert(k2(), 15, v3()).unwrap();
            want.kv.insert(k1(), 20, v2()).unwrap();
            want.kv.insert_tombstone(k1(), 30).unwrap();

            assert_level_eq(&got, &want, "merge_min_seq_0");
        }

        // Test 2: Merge with min_snapshot_seq = 25 (older versions cleaned)
        {
            let got = level1.compact(&level2, InternalSeq::new(25));
            println!("Got level after merge with min_snapshot_seq=25: {:#?}", got);

            let mut want = Level::default();
            want.with_sys_data(|s| s.update_seq(200));
            // Insert in sequence order: key2 at 15, then tombstone at 30
            want.kv.insert(k2(), 15, v3()).unwrap(); // k2 preserved
            want.kv.insert(k1(), 20, v2()).unwrap();
            want.kv.insert_tombstone(k1(), 30).unwrap(); // Only seq 30 preserved

            assert_level_eq(&got, &want, "merge_min_seq_25");
        }

        // Test 3: Merge with min_snapshot_seq = 35 (very aggressive cleanup)
        {
            let got = level1.compact(&level2, InternalSeq::new(35));

            let mut want = Level::default();
            want.with_sys_data(|s| s.update_seq(200));
            // Insert in sequence order: k2 at 15, then tombstone at 30
            want.kv.insert(k2(), 15, v3()).unwrap(); // k2 preserved
            want.kv.insert_tombstone(k1(), 30).unwrap(); // Tombstone preserved

            assert_level_eq(&got, &want, "merge_min_seq_35");
        }

        // Test 4: Reverse merge should be deterministic for sys_data
        {
            let got = level2.compact(&level1, InternalSeq::new(0));

            let mut want = Level::default();
            want.with_sys_data(|s| s.update_seq(200)); // Same higher seq
                                                       // Insert in sequence order: 10, 15, 20, 30
            want.kv.insert(k1(), 10, v1()).unwrap();
            want.kv.insert(k2(), 15, v3()).unwrap();
            want.kv.insert(k1(), 20, v2()).unwrap();
            want.kv.insert_tombstone(k1(), 30).unwrap();

            assert_level_eq(&got, &want, "merge_reverse");
        }
    }

    #[test]
    fn test_merge_expire_table_with_tombstones() {
        // Create two levels for merging
        let mut level1 = Level::default();
        let mut level2 = Level::default();

        // Set up different sequence numbers for sys_data
        level1.with_sys_data(|s| s.update_seq(100));
        level2.with_sys_data(|s| s.update_seq(200));

        // Create test expire keys and values
        let ek1 = || ExpireKey {
            time_ms: 1000,
            seq: 1,
        };
        let ek2 = || ExpireKey {
            time_ms: 2000,
            seq: 2,
        };
        let ev1 = || "expire_v1".to_string();
        let ev2 = || "expire_v2".to_string();
        let ev3 = || "expire_v3".to_string();

        // Populate level1 with initial expire data
        level1.expire.insert(ek1(), 10, ev1()).unwrap();
        level1.expire.insert(ek2(), 15, ev3()).unwrap();

        // Populate level2 with updates and tombstone
        level2.expire.insert(ek1(), 20, ev2()).unwrap();
        level2.expire.insert_tombstone(ek1(), 30).unwrap(); // Tombstone at seq 30

        // Test 1: Merge with min_snapshot_seq = 0 (all versions preserved)
        {
            let got = level1.compact(&level2, InternalSeq::new(0));

            let mut want = Level::default();
            want.with_sys_data(|s| s.update_seq(200)); // Higher seq wins
                                                       // Insert in sequence order: 10, 15, 20, 30
            want.expire.insert(ek1(), 10, ev1()).unwrap();
            want.expire.insert(ek2(), 15, ev3()).unwrap();
            want.expire.insert(ek1(), 20, ev2()).unwrap();
            want.expire.insert_tombstone(ek1(), 30).unwrap();

            assert_level_eq(&got, &want, "expire_merge_min_seq_0");
        }

        // Test 2: Merge with min_snapshot_seq = 25 (older versions cleaned)
        {
            let got = level1.compact(&level2, InternalSeq::new(25));

            let mut want = Level::default();
            want.with_sys_data(|s| s.update_seq(200));
            // Insert in sequence order: ek2 at 15, then tombstone at 30
            want.expire.insert(ek2(), 15, ev3()).unwrap(); // ek2 preserved
            want.expire.insert(ek1(), 20, ev2()).unwrap();
            want.expire.insert_tombstone(ek1(), 30).unwrap(); // Only seq 30 preserved for ek1

            assert_level_eq(&got, &want, "expire_merge_min_seq_25");
        }

        // Test 3: Merge with min_snapshot_seq = 35 (very aggressive cleanup)
        {
            let got = level1.compact(&level2, InternalSeq::new(35));

            let mut want = Level::default();
            want.with_sys_data(|s| s.update_seq(200));
            // Insert in sequence order: ek2 at 15, then tombstone at 30
            want.expire.insert(ek2(), 15, ev3()).unwrap(); // ek2 preserved
            want.expire.insert_tombstone(ek1(), 30).unwrap(); // Tombstone preserved

            assert_level_eq(&got, &want, "expire_merge_min_seq_35");
        }

        // Test 4: Reverse merge should be deterministic for sys_data
        {
            let got = level2.compact(&level1, InternalSeq::new(0));

            let mut want = Level::default();
            want.with_sys_data(|s| s.update_seq(200)); // Same higher seq
                                                       // Insert in sequence order: 10, 15, 20, 30
            want.expire.insert(ek1(), 10, ev1()).unwrap();
            want.expire.insert(ek2(), 15, ev3()).unwrap();
            want.expire.insert(ek1(), 20, ev2()).unwrap();
            want.expire.insert_tombstone(ek1(), 30).unwrap();

            assert_level_eq(&got, &want, "expire_merge_reverse");
        }
    }
}
