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

use seq_marked::InternalSeq;

use crate::leveled_store::immutable_levels::ImmutableLevels;

impl ImmutableLevels {
    /// Compact the smallest two adjacent immutable levels into a new immutable level.
    ///
    /// Finds two adjacent levels `l[i]` and `l[i+1]` so that their combined size is minimal,
    /// then compacts them into a single new immutable level.
    ///
    /// # Returns
    /// A new `ImmutableLevels` with the two smallest adjacent levels compacted into one.
    /// If there are 0 or 1 levels, returns a clone of self.
    pub fn compact_min_adjacent(&self, min_snapshot_seq: InternalSeq) -> Self {
        let n = self.immutables.len();
        if n <= 1 {
            return self.clone();
        }

        let immutables = self.newest_to_oldest().collect::<Vec<_>>();

        let mut min_size = u64::MAX;
        let mut min_index = 0;
        for i in 0..n - 1 {
            let size = immutables[i].size() + immutables[i + 1].size();
            if size < min_size {
                min_size = size;
                min_index = i;
            }
        }

        let newer = immutables[min_index];
        let older = immutables[min_index + 1];

        let new_immutable = newer.compact(older, min_snapshot_seq);

        let mut bt = self.immutables.clone();
        bt.remove(older.level_index());
        bt.remove(newer.level_index());
        bt.insert(*new_immutable.level_index(), new_immutable);

        Self::new_form_iter(bt.into_values())
    }
}

#[cfg(test)]
mod tests {
    use map_api::mvcc::ScopedSeqBoundedGet;
    use seq_marked::InternalSeq;
    use seq_marked::SeqMarked;
    use state_machine_api::UserKey;

    use super::*;
    use crate::leveled_store::immutable::Immutable;
    use crate::leveled_store::level::Level;

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }

    fn uk(s: &str) -> UserKey {
        UserKey::new(s)
    }

    fn mv(s: &str) -> (Option<state_machine_api::KVMeta>, Vec<u8>) {
        (None, b(s))
    }

    #[tokio::test]
    async fn test_compact_min_adjacent_empty() {
        let empty_levels = ImmutableLevels::default();
        let result = empty_levels.compact_min_adjacent(InternalSeq::new(0));
        assert_eq!(result.immutables.len(), 0);
        assert!(result.immutables.is_empty());
    }

    #[tokio::test]
    async fn test_compact_min_adjacent_single_level() {
        let mut level = Level::default();
        level.with_sys_data(|s| s.update_seq(100));
        level.kv.insert(uk("k1"), 10, mv("v1")).unwrap();

        let immutable = Immutable::new_from_level(level);
        let original_index = *immutable.level_index();
        let levels = ImmutableLevels::new_form_iter([immutable]);

        let result = levels.compact_min_adjacent(InternalSeq::new(0));
        assert_eq!(result.immutables.len(), 1);

        let result_indexes: Vec<_> = result.immutables.keys().cloned().collect();
        let expected_indexes = vec![original_index];
        assert_eq!(result_indexes, expected_indexes);
    }

    #[tokio::test]
    async fn test_compact_min_adjacent_multiple_levels() {
        let mut level1 = Level::default();
        level1.with_sys_data(|s| s.update_seq(100));
        level1.kv.insert(uk("k1"), 10, mv("v1")).unwrap();
        level1.kv.insert(uk("k2"), 15, mv("v2")).unwrap();
        level1.kv.insert(uk("k3"), 20, mv("v3")).unwrap();

        let mut level2 = Level::default();
        level2.with_sys_data(|s| s.update_seq(200));
        level2.kv.insert(uk("k4"), 25, mv("v4")).unwrap();

        let mut level3 = Level::default();
        level3.with_sys_data(|s| s.update_seq(300));
        level3.kv.insert(uk("k5"), 30, mv("v5")).unwrap();

        let immutable1 = Immutable::new_from_level(level1);
        let immutable2 = Immutable::new_from_level(level2);
        let immutable3 = Immutable::new_from_level(level3);

        let index1 = *immutable1.level_index();
        let index3 = *immutable3.level_index();

        let levels = ImmutableLevels::new_form_iter([immutable1, immutable2, immutable3]);

        let result = levels.compact_min_adjacent(InternalSeq::new(0));

        assert_eq!(result.immutables.len(), 2);

        let result_indexes: Vec<_> = result.immutables.keys().cloned().collect();
        let expected_indexes = vec![index1, index3];
        assert_eq!(result_indexes, expected_indexes);

        assert_eq!(
            result.get(uk("k4"), 100).await.unwrap(),
            SeqMarked::new_normal(25u64, mv("v4"))
        );
        assert_eq!(
            result.get(uk("k5"), 100).await.unwrap(),
            SeqMarked::new_normal(30u64, mv("v5"))
        );
        assert_eq!(
            result.get(uk("k1"), 100).await.unwrap(),
            SeqMarked::new_normal(10u64, mv("v1"))
        );
    }

    #[tokio::test]
    async fn test_compact_min_adjacent_two_levels() {
        let mut level1 = Level::default();
        level1.with_sys_data(|s| s.update_seq(100));
        level1.kv.insert(uk("k1"), 10, mv("v1")).unwrap();

        let mut level2 = Level::default();
        level2.with_sys_data(|s| s.update_seq(200));
        level2.kv.insert(uk("k2"), 20, mv("v2")).unwrap();

        let immutable1 = Immutable::new_from_level(level1);
        let immutable2 = Immutable::new_from_level(level2);

        let level2_index = *immutable2.level_index();
        let levels = ImmutableLevels::new_form_iter([immutable1, immutable2]);

        let result = levels.compact_min_adjacent(InternalSeq::new(0));
        assert_eq!(result.immutables.len(), 1);

        let result_indexes: Vec<_> = result.immutables.keys().cloned().collect();
        let expected_indexes = vec![level2_index];
        assert_eq!(result_indexes, expected_indexes);

        assert_eq!(
            result.get(uk("k1"), 100).await.unwrap(),
            SeqMarked::new_normal(10u64, mv("v1"))
        );
        assert_eq!(
            result.get(uk("k2"), 100).await.unwrap(),
            SeqMarked::new_normal(20u64, mv("v2"))
        );
    }

    #[tokio::test]
    async fn test_compact_min_adjacent_with_filtering() {
        let mut level1 = Level::default();
        level1.with_sys_data(|s| s.update_seq(100));
        level1.kv.insert(uk("k1"), 5, mv("v1_very_old")).unwrap();
        level1.kv.insert(uk("k2"), 10, mv("v2_only")).unwrap();
        level1.kv.insert(uk("k1"), 15, mv("v1_old")).unwrap();

        let mut level2 = Level::default();
        level2.with_sys_data(|s| s.update_seq(200));
        level2.kv.insert(uk("k1"), 25, mv("v1_new")).unwrap();
        level2.kv.insert(uk("k3"), 30, mv("v3_new")).unwrap();

        let immutable1 = Immutable::new_from_level(level1);
        let immutable2 = Immutable::new_from_level(level2);
        let levels = ImmutableLevels::new_form_iter([immutable1, immutable2]);

        let result = levels.compact_min_adjacent(InternalSeq::new(18));
        assert_eq!(result.immutables.len(), 1);

        assert_eq!(
            result.get(uk("k1"), 100).await.unwrap(),
            SeqMarked::new_normal(25u64, mv("v1_new"))
        );

        assert_eq!(
            result.get(uk("k1"), 20).await.unwrap(),
            SeqMarked::new_normal(15u64, mv("v1_old"))
        );

        assert_eq!(
            result.get(uk("k1"), 12).await.unwrap(),
            SeqMarked::new_not_found()
        );

        assert_eq!(
            result.get(uk("k1"), 18).await.unwrap(),
            SeqMarked::new_normal(15u64, mv("v1_old"))
        );

        assert_eq!(
            result.get(uk("k2"), 100).await.unwrap(),
            SeqMarked::new_normal(10u64, mv("v2_only"))
        );
        assert_eq!(
            result.get(uk("k3"), 100).await.unwrap(),
            SeqMarked::new_normal(30u64, mv("v3_new"))
        );
    }
}
