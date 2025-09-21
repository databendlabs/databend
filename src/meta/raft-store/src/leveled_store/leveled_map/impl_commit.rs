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

use log::info;
use map_api::mvcc;
use map_api::mvcc::Table;
use seq_marked::InternalSeq;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;

#[async_trait::async_trait]
impl mvcc::Commit<Namespace, Key, Value> for LeveledMap {
    async fn commit(
        &mut self,
        last_seq: InternalSeq,
        mut changes: BTreeMap<Namespace, Table<Key, Value>>,
    ) -> Result<(), Error> {
        info!(
            "Committing changes to leveled map data: last_seq={}",
            last_seq
        );
        let mut inner = self.data.lock().unwrap();

        // user map

        let user_updates = changes.remove(&Namespace::User);

        if let Some(updates) = user_updates {
            let it = updates.inner.into_iter().map(|((k, seq_marked), v)| {
                ((k.into_user(), seq_marked), v.map(|x| x.into_user()))
            });

            inner.writable.kv.apply_changes(updates.last_seq, it);
        }

        // expire map

        let expire_updates = changes.remove(&Namespace::Expire);

        if let Some(updates) = expire_updates {
            let it = updates.inner.into_iter().map(|((k, seq_marked), v)| {
                ((k.into_expire(), seq_marked), v.map(|x| x.into_expire()))
            });

            inner.writable.expire.apply_changes(updates.last_seq, it);
        }

        // seq

        inner.writable.with_sys_data(|sys_data| {
            sys_data.update_seq(*last_seq);
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;
    use std::collections::BTreeMap;

    use map_api::mvcc::Commit;
    use map_api::mvcc::Table;
    use map_api::SeqMarked;
    use seq_marked::InternalSeq;
    use state_machine_api::ExpireKey;
    use state_machine_api::KVMeta;
    use state_machine_api::UserKey;

    use crate::leveled_store::leveled_map::LeveledMap;
    use crate::leveled_store::types::Key;
    use crate::leveled_store::types::Namespace;
    use crate::leveled_store::types::Value;

    fn user_key(s: impl ToString) -> UserKey {
        UserKey::new(s)
    }

    fn expire_key(s: impl ToString, time_ms: u64) -> ExpireKey {
        ExpireKey::new(time_ms, s.to_string().parse().unwrap_or(1))
    }

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }

    #[tokio::test]
    async fn test_commit_user_namespace() -> anyhow::Result<()> {
        let mut lm = LeveledMap::default();

        // Create changes for User namespace
        let mut changes = BTreeMap::new();
        let mut user_table = Table::new();
        user_table.last_seq = SeqMarked::new_normal(5, ());
        user_table.inner.insert(
            (
                Key::User(user_key("a")),
                Reverse(SeqMarked::new_normal(1, ())),
            ),
            Some(Value::User((None, b("a1")))),
        );
        user_table.inner.insert(
            (
                Key::User(user_key("b")),
                Reverse(SeqMarked::new_normal(2, ())),
            ),
            Some(Value::User((Some(KVMeta::new_expires_at(10)), b("b1")))),
        );
        changes.insert(Namespace::User, user_table);

        // Commit changes
        lm.commit(InternalSeq::new(5), changes).await?;

        // Verify data was committed
        let data = lm.data.lock().unwrap();
        assert_eq!(data.writable.kv.last_seq, SeqMarked::new_normal(5, ()));

        // Verify exact data in User namespace
        assert_eq!(
            data.writable
                .kv
                .inner
                .get(&(user_key("a"), Reverse(SeqMarked::new_normal(1, ())))),
            Some(&Some((None, b("a1"))))
        );
        assert_eq!(
            data.writable
                .kv
                .inner
                .get(&(user_key("b"), Reverse(SeqMarked::new_normal(2, ())))),
            Some(&Some((Some(KVMeta::new_expires_at(10)), b("b1"))))
        );
        assert_eq!(data.writable.kv.inner.len(), 2);

        // Check that sys_data was updated
        assert_eq!(data.writable.sys_data().curr_seq(), *InternalSeq::new(5));

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_expire_namespace() -> anyhow::Result<()> {
        let mut lm = LeveledMap::default();

        // Create changes for Expire namespace
        let mut changes = BTreeMap::new();
        let mut expire_table = Table::new();
        expire_table.last_seq = SeqMarked::new_normal(3, ());
        expire_table.inner.insert(
            (
                Key::Expire(expire_key("exp1", 1000)),
                Reverse(SeqMarked::new_normal(1, ())),
            ),
            Some(Value::Expire("key1".to_string())),
        );
        changes.insert(Namespace::Expire, expire_table);

        // Commit changes
        lm.commit(InternalSeq::new(3), changes).await?;

        // Verify data was committed
        let data = lm.data.lock().unwrap();
        assert_eq!(data.writable.expire.last_seq, SeqMarked::new_normal(3, ()));

        // Verify exact data in Expire namespace
        assert_eq!(
            data.writable.expire.inner.get(&(
                expire_key("exp1", 1000),
                Reverse(SeqMarked::new_normal(1, ()))
            )),
            Some(&Some("key1".to_string()))
        );
        assert_eq!(data.writable.expire.inner.len(), 1);

        // Check that sys_data was updated
        assert_eq!(data.writable.sys_data().curr_seq(), *InternalSeq::new(3));

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_both_namespaces() -> anyhow::Result<()> {
        let mut lm = LeveledMap::default();

        // Create changes for both namespaces
        let mut changes = BTreeMap::new();

        // User namespace
        let mut user_table = Table::new();
        user_table.last_seq = SeqMarked::new_normal(7, ());
        user_table.inner.insert(
            (
                Key::User(user_key("user1")),
                Reverse(SeqMarked::new_normal(5, ())),
            ),
            Some(Value::User((None, b("data1")))),
        );
        user_table.inner.insert(
            (
                Key::User(user_key("user2")),
                Reverse(SeqMarked::new_tombstone(6)),
            ),
            None,
        );
        changes.insert(Namespace::User, user_table);

        // Expire namespace
        let mut expire_table = Table::new();
        expire_table.last_seq = SeqMarked::new_normal(7, ());
        expire_table.inner.insert(
            (
                Key::Expire(expire_key("exp2", 2000)),
                Reverse(SeqMarked::new_normal(7, ())),
            ),
            Some(Value::Expire("user1".to_string())),
        );
        changes.insert(Namespace::Expire, expire_table);

        // Commit changes
        lm.commit(InternalSeq::new(7), changes).await?;

        // Verify both namespaces were updated
        let data = lm.data.lock().unwrap();
        assert_eq!(data.writable.kv.last_seq, SeqMarked::new_normal(7, ()));
        assert_eq!(data.writable.expire.last_seq, SeqMarked::new_normal(7, ()));

        // Verify exact User namespace data
        assert_eq!(
            data.writable
                .kv
                .inner
                .get(&(user_key("user1"), Reverse(SeqMarked::new_normal(5, ())))),
            Some(&Some((None, b("data1"))))
        );
        assert_eq!(
            data.writable
                .kv
                .inner
                .get(&(user_key("user2"), Reverse(SeqMarked::new_tombstone(6)))),
            Some(&None)
        );
        assert_eq!(data.writable.kv.inner.len(), 2);

        // Verify exact Expire namespace data
        assert_eq!(
            data.writable.expire.inner.get(&(
                expire_key("exp2", 2000),
                Reverse(SeqMarked::new_normal(7, ()))
            )),
            Some(&Some("user1".to_string()))
        );
        assert_eq!(data.writable.expire.inner.len(), 1);

        // Check that sys_data was updated to the last_seq
        assert_eq!(data.writable.sys_data().curr_seq(), *InternalSeq::new(7));

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_empty_changes() -> anyhow::Result<()> {
        let mut lm = LeveledMap::default();

        // Commit with no namespace changes
        let changes = BTreeMap::new();
        lm.commit(InternalSeq::new(10), changes).await?;

        // Only sys_data should be updated
        let data = lm.data.lock().unwrap();
        assert_eq!(data.writable.sys_data().curr_seq(), *InternalSeq::new(10));

        // Tables should remain at their default seq
        assert_eq!(data.writable.kv.last_seq, SeqMarked::zero());
        assert_eq!(data.writable.expire.last_seq, SeqMarked::zero());

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_with_tombstones() -> anyhow::Result<()> {
        let mut lm = LeveledMap::default();

        // Create changes with tombstones (deletions)
        let mut changes = BTreeMap::new();
        let mut user_table = Table::new();
        user_table.last_seq = SeqMarked::new_normal(4, ());

        // Add a normal entry
        user_table.inner.insert(
            (
                Key::User(user_key("keep")),
                Reverse(SeqMarked::new_normal(3, ())),
            ),
            Some(Value::User((None, b("keepme")))),
        );

        // Add a tombstone (deletion)
        user_table.inner.insert(
            (
                Key::User(user_key("delete")),
                Reverse(SeqMarked::new_tombstone(4)),
            ),
            None,
        );

        changes.insert(Namespace::User, user_table);

        // Commit changes
        lm.commit(InternalSeq::new(4), changes).await?;

        // Verify data was committed
        let data = lm.data.lock().unwrap();
        assert_eq!(data.writable.kv.last_seq, SeqMarked::new_normal(4, ()));

        // Verify exact data: normal entry and tombstone
        assert_eq!(
            data.writable
                .kv
                .inner
                .get(&(user_key("keep"), Reverse(SeqMarked::new_normal(3, ())))),
            Some(&Some((None, b("keepme"))))
        );
        assert_eq!(
            data.writable
                .kv
                .inner
                .get(&(user_key("delete"), Reverse(SeqMarked::new_tombstone(4)))),
            Some(&None)
        );
        assert_eq!(data.writable.kv.inner.len(), 2);

        assert_eq!(data.writable.sys_data().curr_seq(), *InternalSeq::new(4));

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_merge_with_existing_data() -> anyhow::Result<()> {
        let mut lm = LeveledMap::default();

        // First, add some initial data to both namespaces
        {
            let mut initial_changes = BTreeMap::new();

            // Initial User namespace data
            let mut user_table = Table::new();
            user_table.last_seq = SeqMarked::new_normal(3, ());
            user_table.inner.insert(
                (
                    Key::User(user_key("key1")),
                    Reverse(SeqMarked::new_normal(1, ())),
                ),
                Some(Value::User((None, b("value1")))),
            );
            user_table.inner.insert(
                (
                    Key::User(user_key("key2")),
                    Reverse(SeqMarked::new_normal(2, ())),
                ),
                Some(Value::User((
                    Some(KVMeta::new_expires_at(100)),
                    b("value2"),
                ))),
            );
            initial_changes.insert(Namespace::User, user_table);

            // Initial Expire namespace data
            let mut expire_table = Table::new();
            expire_table.last_seq = SeqMarked::new_normal(3, ());
            expire_table.inner.insert(
                (
                    Key::Expire(expire_key("exp_key1", 100)),
                    Reverse(SeqMarked::new_normal(3, ())),
                ),
                Some(Value::Expire("key2".to_string())),
            );
            initial_changes.insert(Namespace::Expire, expire_table);

            lm.commit(InternalSeq::new(3), initial_changes).await?;
        }

        // Verify initial data was committed
        {
            let data = lm.data.lock().unwrap();
            assert_eq!(data.writable.kv.last_seq, SeqMarked::new_normal(3, ()));
            assert_eq!(data.writable.expire.last_seq, SeqMarked::new_normal(3, ()));

            // Verify exact User namespace data
            assert_eq!(
                data.writable
                    .kv
                    .inner
                    .get(&(user_key("key1"), Reverse(SeqMarked::new_normal(1, ())))),
                Some(&Some((None, b("value1"))))
            );
            assert_eq!(
                data.writable
                    .kv
                    .inner
                    .get(&(user_key("key2"), Reverse(SeqMarked::new_normal(2, ())))),
                Some(&Some((Some(KVMeta::new_expires_at(100)), b("value2"))))
            );
            assert_eq!(data.writable.kv.inner.len(), 2);

            // Verify exact Expire namespace data
            assert_eq!(
                data.writable.expire.inner.get(&(
                    expire_key("exp_key1", 100),
                    Reverse(SeqMarked::new_normal(3, ()))
                )),
                Some(&Some("key2".to_string()))
            );
            assert_eq!(data.writable.expire.inner.len(), 1);
        }

        // Now commit new changes that should be merged with existing data
        {
            let mut new_changes = BTreeMap::new();

            // New User namespace data (updates key2, adds key3, deletes key1)
            let mut user_table = Table::new();
            user_table.last_seq = SeqMarked::new_normal(7, ());
            // Update existing key2 with new value
            user_table.inner.insert(
                (
                    Key::User(user_key("key2")),
                    Reverse(SeqMarked::new_normal(4, ())),
                ),
                Some(Value::User((
                    Some(KVMeta::new_expires_at(200)),
                    b("value2_updated"),
                ))),
            );
            // Add new key3
            user_table.inner.insert(
                (
                    Key::User(user_key("key3")),
                    Reverse(SeqMarked::new_normal(5, ())),
                ),
                Some(Value::User((None, b("value3")))),
            );
            // Delete key1 with tombstone
            user_table.inner.insert(
                (
                    Key::User(user_key("key1")),
                    Reverse(SeqMarked::new_tombstone(6)),
                ),
                None,
            );
            new_changes.insert(Namespace::User, user_table);

            // New Expire namespace data
            let mut expire_table = Table::new();
            expire_table.last_seq = SeqMarked::new_normal(7, ());
            // Remove old expiry
            expire_table.inner.insert(
                (
                    Key::Expire(expire_key("exp_key1", 100)),
                    Reverse(SeqMarked::new_tombstone(7)),
                ),
                None,
            );
            // Add new expiry
            expire_table.inner.insert(
                (
                    Key::Expire(expire_key("exp_key2", 200)),
                    Reverse(SeqMarked::new_normal(7, ())),
                ),
                Some(Value::Expire("key2".to_string())),
            );
            new_changes.insert(Namespace::Expire, expire_table);

            lm.commit(InternalSeq::new(7), new_changes).await?;
        }

        // Verify merged data
        {
            let data = lm.data.lock().unwrap();

            // Check sequences were updated
            assert_eq!(data.writable.kv.last_seq, SeqMarked::new_normal(7, ()));
            assert_eq!(data.writable.expire.last_seq, SeqMarked::new_normal(7, ()));

            // Check that sys_data was updated
            assert_eq!(data.writable.sys_data().curr_seq(), *InternalSeq::new(7));

            // Verify User namespace has all entries (old and new)
            // Should have: old key1 (seq 1), new key1 tombstone (seq 6),
            //              old key2 (seq 2), new key2 (seq 4),
            //              new key3 (seq 5)
            assert_eq!(data.writable.kv.inner.len(), 5);

            // Verify key1: should have original value and tombstone
            assert_eq!(
                data.writable
                    .kv
                    .inner
                    .get(&(user_key("key1"), Reverse(SeqMarked::new_normal(1, ())))),
                Some(&Some((None, b("value1"))))
            );
            assert_eq!(
                data.writable
                    .kv
                    .inner
                    .get(&(user_key("key1"), Reverse(SeqMarked::new_tombstone(6)))),
                Some(&None)
            );

            // Verify key2: should have both old and new versions
            assert_eq!(
                data.writable
                    .kv
                    .inner
                    .get(&(user_key("key2"), Reverse(SeqMarked::new_normal(2, ())))),
                Some(&Some((Some(KVMeta::new_expires_at(100)), b("value2"))))
            );
            assert_eq!(
                data.writable
                    .kv
                    .inner
                    .get(&(user_key("key2"), Reverse(SeqMarked::new_normal(4, ())))),
                Some(&Some((
                    Some(KVMeta::new_expires_at(200)),
                    b("value2_updated")
                )))
            );

            // Verify key3: should have the new entry
            assert_eq!(
                data.writable
                    .kv
                    .inner
                    .get(&(user_key("key3"), Reverse(SeqMarked::new_normal(5, ())))),
                Some(&Some((None, b("value3"))))
            );

            // Verify Expire namespace has all entries
            assert_eq!(data.writable.expire.inner.len(), 3);

            // Verify exp_key1: should have original value and tombstone
            assert_eq!(
                data.writable.expire.inner.get(&(
                    expire_key("exp_key1", 100),
                    Reverse(SeqMarked::new_normal(3, ()))
                )),
                Some(&Some("key2".to_string()))
            );
            assert_eq!(
                data.writable.expire.inner.get(&(
                    expire_key("exp_key1", 100),
                    Reverse(SeqMarked::new_tombstone(7))
                )),
                Some(&None)
            );

            // Verify exp_key2: should have the new entry
            assert_eq!(
                data.writable.expire.inner.get(&(
                    expire_key("exp_key2", 200),
                    Reverse(SeqMarked::new_normal(7, ()))
                )),
                Some(&Some("key2".to_string()))
            );
        }

        Ok(())
    }
}
