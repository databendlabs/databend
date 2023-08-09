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

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::RangeBounds;

use common_meta_types::KVMeta;
use common_meta_types::LogId;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::StoredMembership;
use log::debug;

use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::marked::Marked;
use crate::state_machine::ExpireKey;

static KV_EMPTY: Marked<Vec<u8>> = Marked::empty();
static EXPIRE_EMPTY: Marked<String> = Marked::empty();

/// A single level of state machine data.
///
/// State machine data is composed of multiple levels.
#[derive(Debug, Default)]
pub struct LevelData {
    /// The last applied log id.
    last_applied: Option<LogId>,

    /// The last applied membership log.
    last_membership: StoredMembership,

    /// All of the nodes in this cluster, including the voters and learners.
    nodes: BTreeMap<NodeId, Node>,

    // ---
    // ---
    // ---
    /// The sequence number for every [`SeqV`] value that is stored in this state machine.
    ///
    /// A seq is globally unique and monotonically increasing.
    sequence: u64,

    /// Generic Key-value store.
    kv: BTreeMap<String, Marked<Vec<u8>>>,

    /// The expiration queue of generic kv.
    expire: BTreeMap<ExpireKey, Marked<String>>,
}

impl LevelData {
    /// Create a new level that is based on this level.
    pub(crate) fn new_level(&self) -> Self {
        Self {
            // Small data are cloned
            sequence: self.sequence,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            nodes: self.nodes.clone(),

            // Large data set is referenced.
            kv: Default::default(),
            expire: Default::default(),
        }
    }

    pub(in crate::sm_v002) fn replace_kv(&mut self, kv: BTreeMap<String, Marked<Vec<u8>>>) {
        self.kv = kv;
    }

    pub(in crate::sm_v002) fn replace_expire(
        &mut self,
        expire: BTreeMap<ExpireKey, Marked<String>>,
    ) {
        self.expire = expire;
    }

    pub(crate) fn curr_seq(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn update_seq(&mut self, seq: u64) {
        self.sequence = seq;
    }

    // Cloned data is accessed directly
    pub(crate) fn next_seq(&mut self) -> u64 {
        self.sequence += 1;
        debug!("next_seq: {}", self.sequence);
        // dbg!("next_seq", self.sequence);

        self.sequence
    }

    // Cloned data is accessed directly
    pub fn last_applied_ref(&self) -> &Option<LogId> {
        &self.last_applied
    }

    // Cloned data is accessed directly
    pub fn last_membership_ref(&self) -> &StoredMembership {
        &self.last_membership
    }

    // Cloned data is accessed directly
    pub(crate) fn nodes_ref(&self) -> &BTreeMap<NodeId, Node> {
        &self.nodes
    }

    // Cloned data is accessed directly
    pub(crate) fn last_applied_mut(&mut self) -> &mut Option<LogId> {
        &mut self.last_applied
    }

    // Cloned data is accessed directly
    pub(crate) fn last_membership_mut(&mut self) -> &mut StoredMembership {
        &mut self.last_membership
    }

    // Cloned data is accessed directly
    pub(crate) fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        &mut self.nodes
    }
}

impl MapApi<String> for LevelData {
    type V = Vec<u8>;

    fn get<Q>(&self, key: &Q) -> &Marked<Self::V>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.kv.get(key).unwrap_or(&KV_EMPTY)
    }

    fn set(
        &mut self,
        key: String,
        value: Option<(Self::V, Option<KVMeta>)>,
    ) -> (Marked<Self::V>, Marked<Self::V>) {
        // The chance it is the bottom level is very low in a loaded system.
        // Thus we always tombstone the key if it is None.

        // dbg!("set kv", &key, &value);

        let marked = if let Some((v, meta)) = value {
            let seq = self.next_seq();
            Marked::new_normal(seq, v, meta)
        } else {
            // Do not increase the sequence number, just use the max seq for all tombstone.
            let seq = self.curr_seq();
            Marked::new_tomb_stone(seq)
        };

        let prev = MapApi::<String>::get(self, key.as_str()).clone();
        self.kv.insert(key, marked.clone());
        (prev, marked)
    }

    fn range<'a, T: ?Sized, R>(
        &'a self,
        range: R,
    ) -> Box<dyn Iterator<Item = (&'a String, &'a Marked)> + 'a>
    where
        String: 'a,
        String: Borrow<T>,
        T: Ord,
        R: RangeBounds<T>,
    {
        Box::new(self.kv.range(range))
    }
}

impl MapApi<ExpireKey> for LevelData {
    type V = String;

    fn get<Q>(&self, key: &Q) -> &Marked<Self::V>
    where
        ExpireKey: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.expire.get(key).unwrap_or(&EXPIRE_EMPTY)
    }

    fn set(
        &mut self,
        key: ExpireKey,
        value: Option<(Self::V, Option<KVMeta>)>,
    ) -> (Marked<Self::V>, Marked<Self::V>) {
        // dbg!("set expire", &key, &value);

        let seq = self.curr_seq();

        let marked = if let Some((v, meta)) = value {
            Marked::from((seq, v, meta))
        } else {
            Marked::TombStone { internal_seq: seq }
        };

        let prev = MapApi::<ExpireKey>::get(self, &key).clone();
        self.expire.insert(key, marked.clone());
        (prev, marked)
    }

    fn range<'a, T: ?Sized, R>(
        &'a self,
        range: R,
    ) -> Box<dyn Iterator<Item = (&'a ExpireKey, &'a Marked<String>)> + 'a>
    where
        ExpireKey: 'a,
        ExpireKey: Borrow<T>,
        T: Ord,
        R: RangeBounds<T>,
    {
        Box::new(self.expire.range(range))
    }
}
