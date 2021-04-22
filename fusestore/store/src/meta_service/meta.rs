// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;

use async_trait::async_trait;

#[async_trait]
pub trait IMeta {
    async fn add(&mut self, key: String, value: String) -> anyhow::Result<()>;
    // async fn remove(&self, key: String) -> anyhow::Result<()>;
}

/// Distributed FS meta data manager
#[derive(Default)]
pub struct Meta {
    /// The file names stored in this cluster
    pub keys: BTreeMap<String, String>,

    // cluster nodes, key distribution etc.
    pub slots: Vec<Slot>,
    pub nodes: HashMap<NodeId, Node>
}

/// A slot is a virtual and intermediate allocation unit in a distributed storage.
/// The key of an object is mapped to a slot by some hashing algo.
/// A slot is assigned to several physical servers(normally 3 for durability).
pub struct Slot {
    node_ids: Vec<i64>
}

pub type NodeId = i64;

#[derive(Debug, Clone)]
pub struct Node {
    pub name: String,
    pub address: String
}

#[async_trait]
impl IMeta for Meta {
    async fn add(&mut self, key: String, value: String) -> anyhow::Result<()> {
        self.keys.insert(key, value);
        Ok(())
    }
}

impl Meta {
    pub fn empty() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Returns the slot index to store a file.
    pub fn slot_index_for_key(&self, key: &str) -> u64 {
        // TODO use consistent hash if need to extend cluster.
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hsh = hasher.finish();
        hsh % self.slots.len() as u64
    }

    /// Returns the Node-s that keeps a copy of a file.
    pub fn nodes_to_store_key(&self, key: &str) -> Vec<Node> {
        let slot_idx = self.slot_index_for_key(key);
        let slot = &self.slots[slot_idx as usize];

        slot.node_ids
            .iter()
            .map(|nid| (*self.nodes.get(nid).unwrap()).clone())
            .collect()
    }
}
