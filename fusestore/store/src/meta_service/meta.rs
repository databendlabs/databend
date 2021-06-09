// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use common_tracing::tracing;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::placement::rand_n_from_m;
use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;
use crate::meta_service::IPlacement;
use crate::meta_service::NodeId;

/// Replication defines the replication strategy.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Replication {
    /// n-copies mode.
    Mirror(u64),
}

impl Default for Replication {
    fn default() -> Self {
        Replication::Mirror(1)
    }
}

/// Meta data of a Dfs.
/// Includes:
/// - what files are stored in this Dfs.
/// - the algo about how to distribute files to nodes.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Meta {
    /// The file names stored in this cluster
    pub keys: BTreeMap<String, String>,

    /// storage of auto-incremental number.
    pub sequences: BTreeMap<String, u64>,

    // cluster nodes, key distribution etc.
    pub slots: Vec<Slot>,
    pub nodes: HashMap<NodeId, Node>,

    pub replication: Replication,
}

#[derive(Debug, Default, Clone)]
pub struct MetaBuilder {
    /// The number of slots to allocated.
    initial_slots: Option<u64>,
    /// The replication strategy.
    replication: Option<Replication>,
}

impl MetaBuilder {
    /// Set the number of slots to boot up a cluster.
    pub fn slots(mut self, n: u64) -> Self {
        self.initial_slots = Some(n);
        self
    }

    /// Specifies the cluster to replicate by mirror `n` copies of every file.
    pub fn mirror_replication(mut self, n: u64) -> Self {
        self.replication = Some(Replication::Mirror(n));
        self
    }

    pub fn build(self) -> anyhow::Result<Meta> {
        let initial_slots = self.initial_slots.unwrap_or(3);
        let replication = self.replication.unwrap_or(Replication::Mirror(1));

        let mut m = Meta {
            keys: BTreeMap::new(),
            sequences: BTreeMap::new(),
            slots: Vec::with_capacity(initial_slots as usize),
            nodes: HashMap::new(),
            replication,
        };
        for _i in 0..initial_slots {
            m.slots.push(Slot::default());
        }
        Ok(m)
    }
}

impl Meta {
    pub fn builder() -> MetaBuilder {
        MetaBuilder {
            ..Default::default()
        }
    }

    // Apply an op from client.
    #[tracing::instrument(level = "info", skip(self))]
    pub fn apply(&mut self, data: &ClientRequest) -> anyhow::Result<ClientResponse> {
        match data.cmd {
            Cmd::AddFile { ref key, ref value } => {
                if self.keys.contains_key(key) {
                    let prev = self.keys.get(key);
                    Ok((prev.cloned(), None).into())
                } else {
                    let prev = self.keys.insert(key.clone(), value.clone());
                    tracing::info!("applied AddFile: {}={}", key, value);
                    Ok((prev, Some(value.clone())).into())
                }
            }

            Cmd::SetFile { ref key, ref value } => {
                let prev = self.keys.insert(key.clone(), value.clone());
                tracing::info!("applied SetFile: {}={}", key, value);
                Ok((prev, Some(value.clone())).into())
            }

            Cmd::IncrSeq { ref key } => {
                let prev = self.sequences.get(key);
                let curr = match prev {
                    Some(v) => v + 1,
                    None => 1,
                };
                self.sequences.insert(key.clone(), curr);
                tracing::info!("applied IncrSeq: {}={}", key, curr);
                Ok(curr.into())
            }

            Cmd::AddNode {
                ref node_id,
                ref node,
            } => {
                if self.nodes.contains_key(node_id) {
                    let prev = self.nodes.get(node_id);
                    Ok((prev.cloned(), None).into())
                } else {
                    let prev = self.nodes.insert(*node_id, node.clone());
                    tracing::info!("applied AddNode: {}={:?}", node_id, node);
                    Ok((prev, Some(node.clone())).into())
                }
            }
        }
    }

    /// Initialize slots by assign nodes to everyone of them randomly, according to replicationn config.
    pub fn init_slots(&mut self) -> anyhow::Result<()> {
        for i in 0..self.slots.len() {
            self.assign_rand_nodes_to_slot(i)?;
        }

        Ok(())
    }

    /// Assign `n` random nodes to a slot thus the files associated to this slot are replicated to the corresponding nodes.
    /// This func does not cnosider nodes load and should only be used when a Dfs cluster is initiated.
    /// TODO(xp): add another func for load based assignment
    pub fn assign_rand_nodes_to_slot(&mut self, slot_index: usize) -> anyhow::Result<()> {
        let n = match self.replication {
            Replication::Mirror(x) => x,
        } as usize;

        let mut node_ids = self.nodes.keys().collect::<Vec<&NodeId>>();
        node_ids.sort();
        let total = node_ids.len();
        let node_indexes = rand_n_from_m(total, n)?;

        let mut slot = self
            .slots
            .get_mut(slot_index)
            .ok_or_else(|| anyhow::anyhow!("slot not found: {}", slot_index))?;

        slot.node_ids = node_indexes.iter().map(|i| *node_ids[*i]).collect();

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub fn get_file(&self, key: &str) -> Option<String> {
        tracing::info!("meta::get_file: {}", key);
        let x = self.keys.get(key);
        tracing::info!("meta::get_file: {}={:?}", key, x);
        x.cloned()
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let x = self.nodes.get(node_id);
        x.cloned()
    }
}

/// A slot is a virtual and intermediate allocation unit in a distributed storage.
/// The key of an object is mapped to a slot by some hashing algo.
/// A slot is assigned to several physical servers(normally 3 for durability).
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Slot {
    pub node_ids: Vec<NodeId>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub address: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.name, self.address)
    }
}

impl IPlacement for Meta {
    fn get_slots(&self) -> &[Slot] {
        &self.slots
    }

    fn get_node(&self, node_id: &u64) -> Option<&Node> {
        self.nodes.get(node_id)
    }
}
