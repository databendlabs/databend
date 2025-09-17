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

use map_api::mvcc;
use map_api::mvcc::ScopedSeqBoundedRange;
use state_machine_api::ExpireKey;
use state_machine_api::UserKey;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;

impl ImmutableLevels {
    /// Compact all immutable levels into a new immutable level.
    pub async fn compact_all(&self) -> Self {
        let immutable_levels = self.clone();

        let Some(newest) = self.newest() else {
            return self.clone();
        };

        // Create an empty level with SysData cloned.
        let mut data = newest.new_level();

        // Copy all expire data and keep tombstone.
        let strm = immutable_levels
            .range(ExpireKey::default().., u64::MAX)
            .await
            .unwrap();

        let table = mvcc::Table::from_stream(strm).await.unwrap();
        data.replace_expire(table);

        // Copy all kv data and keep tombstone.
        let strm = immutable_levels
            .range(UserKey::default().., u64::MAX)
            .await
            .unwrap();

        let table = mvcc::Table::from_stream(strm).await.unwrap();
        data.replace_kv(table);

        // Preserve the LevelIndex of the newest level after compaction
        let newest_index = *newest.level_index();
        let immutable = Immutable::new_with_index(data, newest_index);
        Self::new_form_iter([immutable])
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_types::raft_types::Membership;
    use databend_common_meta_types::raft_types::StoredMembership;
    use futures_util::TryStreamExt;
    use map_api::mvcc::ScopedSeqBoundedRange;
    use openraft::testing::log_id;
    use seq_marked::SeqMarked;
    use state_machine_api::ExpireKey;
    use state_machine_api::UserKey;

    use crate::sm_v003::compact_immutable_levels_test::build_3_levels;

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }

    fn user_key(s: impl ToString) -> UserKey {
        UserKey::new(s)
    }

    #[tokio::test]
    async fn test_compact_copied_value_and_kv() -> anyhow::Result<()> {
        let lm = build_3_levels().await?;

        lm.testing_freeze_writable();
        let immutable_levels = lm.immutable_levels();

        // Capture the original newest level's index before compaction
        let original_newest_index = *immutable_levels.newest().unwrap().level_index();

        let compacted = immutable_levels.compact_all().await;

        let d = compacted.newest().unwrap().clone();

        // Assert that compact_all preserves the LevelIndex of the newest level
        assert_eq!(
            *d.level_index(),
            original_newest_index,
            "LevelIndex should be preserved after compact_all"
        );

        assert_eq!(compacted.newest_to_oldest().count(), 1);
        assert_eq!(
            d.last_membership(),
            StoredMembership::new(
                Some(log_id(3, 3, 3)),
                Membership::new_with_defaults(vec![], [])
            )
        );
        assert_eq!(d.last_applied(), Some(log_id(3, 3, 3)));

        let got = d
            .range(UserKey::default().., u64::MAX)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(got, vec![
            //
            (user_key("a"), SeqMarked::new_normal(1, (None, b("a0")))),
            (user_key("b"), SeqMarked::new_tombstone(4)),
            (user_key("c"), SeqMarked::new_tombstone(6)),
            (user_key("d"), SeqMarked::new_normal(7, (None, b("d2")))),
            (user_key("e"), SeqMarked::new_normal(6, (None, b("e1")))),
        ]);

        let got = d
            .range(ExpireKey::default().., u64::MAX)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(got, vec![]);

        Ok(())
    }
}
