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
use std::sync::Arc;
use std::sync::Mutex;

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
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

/// A single level of state machine data.
///
/// State machine data is composed of multiple levels.
#[derive(Debug, Default)]
pub struct Level {
    /// System data(non-user data).
    pub(crate) sys_data: Arc<Mutex<SysData>>,

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

pub(crate) trait GetTable<K, V> {
    fn get_table(&self) -> &mvcc::Table<K, V>;
}

impl GetTable<UserKey, MetaValue> for Level {
    fn get_table(&self) -> &mvcc::Table<UserKey, MetaValue> {
        &self.kv
    }
}

#[async_trait::async_trait]
impl GetTable<ExpireKey, String> for Level {
    fn get_table(&self) -> &mvcc::Table<ExpireKey, String> {
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
        let level = Self {
            // sys data are cloned
            sys_data: self.sys_data.clone(),

            // Large data set is referenced.
            kv: Default::default(),
            expire: Default::default(),
        };

        level.with_sys_data(|s| s.incr_data_seq());

        level
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
