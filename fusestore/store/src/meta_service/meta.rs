// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use common_metatypes::Database;
use common_metatypes::Table;
use common_tracing::tracing;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::placement::rand_n_from_m;
use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;
use crate::meta_service::NodeId;
use crate::meta_service::Placement;

/// seq number key to generate database id
const SEQ_DATABASE_ID: &str = "database_id";
/// seq number key to generate table id
// const SEQ_TABLE_ID: &str = "table_id";

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

    /// db name to database mapping
    pub databases: BTreeMap<String, Database>,

    /// table id to table mapping
    pub tables: BTreeMap<u64, Table>,
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
            databases: BTreeMap::new(),
            tables: BTreeMap::new(),
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

    /// Internal func to get an auto-incr seq number.
    /// It is just what Cmd::IncrSeq does and is also used by Cmd that requires
    /// a unique id such as Cmd::AddDatabase which needs make a new database id.
    fn incr_seq(&mut self, key: &str) -> u64 {
        let prev = self.sequences.get(key);
        let curr = match prev {
            Some(v) => v + 1,
            None => 1,
        };
        self.sequences.insert(key.to_string(), curr);
        tracing::debug!("applied IncrSeq: {}={}", key, curr);

        curr
    }

    /// Apply an op sent from client.
    /// This is the only entry to modify meta data.
    /// The `data` is always committed by raft before applying.
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

            Cmd::IncrSeq { ref key } => Ok(self.incr_seq(key).into()),

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

            Cmd::AddDatabase { ref name } => {
                // - If the db present, return it.
                // - Otherwise, create a new one with next seq number as database id, and add it in to store.
                if self.databases.contains_key(name) {
                    let prev = self.databases.get(name);
                    Ok((prev.cloned(), None).into())
                } else {
                    let db = Database {
                        database_id: self.incr_seq(SEQ_DATABASE_ID),
                        tables: Default::default(),
                    };

                    let prev = self.databases.insert(name.clone(), db.clone());
                    tracing::debug!("applied AddDatabase: {}={:?}", name, db);

                    Ok((prev, Some(db)).into())
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

    pub fn get_database(&self, name: &str) -> Option<Database> {
        let x = self.databases.get(name);
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

impl Placement for Meta {
    fn get_slots(&self) -> &[Slot] {
        &self.slots
    }

    fn get_node(&self, node_id: &u64) -> Option<&Node> {
        self.nodes.get(node_id)
    }
}
