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
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::Node;
use futures_util::StreamExt;
use log::warn;
use map_api::map_api_ro::MapApiRO;
use map_api::mvcc;
use map_api::BeforeAfter;
use map_api::MapApi;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::SeqMarkedOf;

/// A single level of state machine data.
///
/// State machine data is composed of multiple levels.
#[derive(Debug, Default, Clone)]
pub struct Level {
    /// System data(non-user data).
    pub(crate) sys_data: Arc<Mutex<SysData>>,

    /// Generic Key-value store.
    pub(crate) kv: mvcc::Table<UserKey, MetaValue>,

    /// The expiration queue of generic kv.
    pub(crate) expire: mvcc::Table<ExpireKey, String>,
}

pub(crate) trait GetTable<K, V> {
    fn get_table(&self) -> &mvcc::Table<K, V>;
}

impl GetTable<UserKey, MetaValue> for Level {
    fn get_table(&self) -> &mvcc::Table<UserKey, MetaValue> {
        &self.kv
    }
}

impl GetTable<ExpireKey, String> for Level {
    fn get_table(&self) -> &mvcc::Table<ExpireKey, String> {
        &self.expire
    }
}

impl Level {
    /// Create a new level that is based on this level.
    pub(crate) fn new_level(&self) -> Self {
        Self {
            // sys data are cloned
            sys_data: self.sys_data.clone(),

            // Large data set is referenced.
            kv: Default::default(),
            expire: Default::default(),
        }
    }

    pub(crate) fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        let mut sys_data = self.sys_data.lock().unwrap();
        f(&mut sys_data)
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
        self.with_sys_data(|s| s.last_membership_ref().clone())
    }

    pub fn last_applied(&self) -> Option<LogId> {
        self.with_sys_data(|s| *s.last_applied_mut())
    }

    pub fn nodes(&self) -> BTreeMap<NodeId, Node> {
        self.with_sys_data(|s| s.nodes_mut().clone())
    }
}

#[async_trait::async_trait]
impl MapApiRO<UserKey> for Level {
    async fn get(&self, key: &UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        let got = self.kv.get(key.clone(), u64::MAX).cloned();
        Ok(got)
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<UserKey>, io::Error>
    where R: RangeBounds<UserKey> + Clone + Send + Sync + 'static {
        // Level is borrowed. It has to copy the result to make the returning stream static.
        let vec = self
            .kv
            .range(range, u64::MAX)
            .map(|(k, v)| (k.clone(), v.cloned()))
            .collect::<Vec<_>>();

        if vec.len() > 1000 {
            warn!(
                "Level::<UserKey>::range() returns big range of len={}",
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        Ok(strm)
    }
}

#[async_trait::async_trait]
impl MapApiRO<ExpireKey> for Level {
    async fn get(&self, key: &ExpireKey) -> Result<SeqMarkedOf<ExpireKey>, io::Error> {
        let got = self.expire.get(*key, u64::MAX).cloned();
        Ok(got)
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<ExpireKey>, io::Error>
    where R: RangeBounds<ExpireKey> + Clone + Send + Sync + 'static {
        // Level is borrowed. It has to copy the result to make the returning stream static.
        let vec = self
            .expire
            .range(range, u64::MAX)
            .map(|(k, v)| (*k, v.cloned()))
            .collect::<Vec<_>>();

        if vec.len() > 1000 {
            warn!(
                "Level::<ExpireKey>::range() returns big range of len={}",
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        Ok(strm)
    }
}

// Only used for tests
#[async_trait::async_trait]
impl MapApi<UserKey> for Level {
    async fn set(
        &mut self,
        key: UserKey,
        value: Option<MetaValue>,
    ) -> Result<BeforeAfter<SeqMarked<MetaValue>>, io::Error> {
        // The chance it is the bottom level is very low in a loaded system.
        // Thus, we always tombstone the key if it is None.

        let prev = MapApiRO::get(self, &key).await?;

        let marked = if let Some(meta_v) = value {
            let seq = self.with_sys_data(|s| s.next_seq());
            self.kv.insert(key, seq, meta_v.clone()).unwrap();
            SeqMarked::new_normal(seq, meta_v)
        } else {
            // Do not increase the sequence number, just use the max seq for all tombstone.
            let seq = self.with_sys_data(|s| s.curr_seq());
            self.kv.insert_tombstone(key, seq).unwrap();
            SeqMarked::new_tombstone(seq)
        };

        Ok((prev, marked))
    }
}
