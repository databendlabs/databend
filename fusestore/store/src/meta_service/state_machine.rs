// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use async_raft::LogId;
use common_exception::prelude::ErrorCode;
use common_flights::storage_api_impl::AppendResult;
use common_flights::storage_api_impl::DataPartInfo;
use common_metatypes::Database;
use common_metatypes::MatchSeqExt;
use common_metatypes::SeqValue;
use common_metatypes::Table;
use common_planners::Part;
use common_planners::Statistics;
use common_tracing::tracing;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::placement::rand_n_from_m;
use crate::meta_service::AppliedState;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::meta_service::NodeId;
use crate::meta_service::Placement;

/// seq number key to generate seq for the value of a `generic_kv` record.
const SEQ_GENERIC_KV: &str = "generic_kv";
/// seq number key to generate database id
const SEQ_DATABASE_ID: &str = "database_id";
/// seq number key to generate table id
const SEQ_TABLE_ID: &str = "table_id";

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

/// The state machine of the `MemStore`.
/// It includes user data and two raft-related informations:
/// `last_applied_logs` and `client_serial_responses` to achieve idempotence.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachine {
    /// raft state: last applied log.
    pub last_applied_log: LogId,

    /// raft state: A mapping of client IDs to their state info:
    /// (serial, RaftResponse)
    /// This is used to de-dup client request, to impl idempotent operations.
    pub client_last_resp: HashMap<String, (u64, AppliedState)>,

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

    /// table parts， db -> (table -> data parts)
    pub tbl_parts: HashMap<String, HashMap<String, Vec<DataPartInfo>>>,

    /// A kv store of all other general purpose information.
    /// The value is tuple of a monotonic sequence number and userdata value in string.
    /// The sequence number is guaranteed to increment(by some value greater than 0) everytime the record changes.
    pub kv: BTreeMap<String, (u64, Vec<u8>)>,
}

#[derive(Debug, Default, Clone)]
pub struct StateMachineBuilder {
    /// The number of slots to allocated.
    initial_slots: Option<u64>,
    /// The replication strategy.
    replication: Option<Replication>,
}

impl StateMachineBuilder {
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

    pub fn build(self) -> common_exception::Result<StateMachine> {
        let initial_slots = self.initial_slots.unwrap_or(3);
        let replication = self.replication.unwrap_or(Replication::Mirror(1));

        let mut m = StateMachine {
            last_applied_log: LogId { term: 0, index: 0 },
            client_last_resp: Default::default(),
            keys: BTreeMap::new(),
            sequences: BTreeMap::new(),
            slots: Vec::with_capacity(initial_slots as usize),
            nodes: HashMap::new(),
            replication,
            databases: BTreeMap::new(),
            tables: BTreeMap::new(),
            tbl_parts: HashMap::new(),
            kv: BTreeMap::new(),
        };
        for _i in 0..initial_slots {
            m.slots.push(Slot::default());
        }
        Ok(m)
    }
}

impl StateMachine {
    pub fn builder() -> StateMachineBuilder {
        StateMachineBuilder {
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

    /// Apply an log entry to state machine.
    ///
    /// If a duplicated log entry is detected by checking data.txid, no update
    /// will be made and the previous resp is returned. In this way a client is able to re-send a
    /// command safely in case of network failure etc.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn apply(&mut self, log_id: &LogId, data: &LogEntry) -> anyhow::Result<AppliedState> {
        self.last_applied_log = *log_id;
        if let Some(ref txid) = data.txid {
            if let Some((serial, resp)) = self.client_last_resp.get(&txid.client) {
                if serial == &txid.serial {
                    return Ok(resp.clone());
                }
            }
        }

        let resp = self.apply_non_dup(data)?;

        if let Some(ref txid) = data.txid {
            self.client_last_resp
                .insert(txid.client.clone(), (txid.serial, resp.clone()));
        }
        Ok(resp)
    }

    /// Apply an op into state machine.
    /// Already applied log should be filtered out before passing into this function.
    /// This is the only entry to modify state machine.
    /// The `data` is always committed by raft before applying.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn apply_non_dup(&mut self, data: &LogEntry) -> common_exception::Result<AppliedState> {
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

            Cmd::CreateDatabase { ref name, .. } => {
                // - If the db present, return it.
                // - Otherwise, create a new one with next seq number as database id, and add it in to store.
                if self.databases.contains_key(name) {
                    let prev = self.databases.get(name);
                    Ok((prev.cloned(), prev.cloned()).into())
                } else {
                    let db = Database {
                        database_id: self.incr_seq(SEQ_DATABASE_ID),
                        tables: Default::default(),
                    };

                    self.databases.insert(name.clone(), db.clone());
                    tracing::debug!("applied CreateDatabase: {}={:?}", name, db);

                    Ok((None, Some(db)).into())
                }
            }

            Cmd::DropDatabase { ref name } => {
                let prev = self.databases.get(name).cloned();
                self.databases.remove(name);
                tracing::debug!("applied DropDatabase: {}", name);
                Ok((prev, None).into())
            }

            Cmd::CreateTable {
                ref db_name,
                ref table_name,
                if_not_exists: _,
                ref table,
            } => {
                let db = self.databases.get(db_name);
                let mut db = db.unwrap().to_owned();

                if db.tables.contains_key(table_name) {
                    let table_id = db.tables.get(table_name).unwrap();
                    let prev = self.tables.get(table_id);
                    Ok((prev.cloned(), prev.cloned()).into())
                } else {
                    let table = Table {
                        table_id: self.incr_seq(SEQ_TABLE_ID),
                        schema: table.schema.clone(),
                        parts: table.parts.clone(),
                    };
                    db.tables.insert(table_name.clone(), table.table_id);
                    self.databases.insert(db_name.clone(), db);
                    self.tables.insert(table.table_id, table.clone());
                    tracing::debug!("applied CreateTable: {}={:?}", table_name, table);

                    Ok((None, Some(table)).into())
                }
            }

            Cmd::DropTable {
                ref db_name,
                ref table_name,
                if_exists: _,
            } => {
                let db = self.databases.get_mut(db_name).unwrap();
                let tbl_id = db.tables.get(table_name);
                if let Some(tbl_id) = tbl_id {
                    let tbl_id = tbl_id.to_owned();
                    db.tables.remove(table_name);
                    let prev = self.tables.remove(&tbl_id);

                    Ok((prev, None).into())
                } else {
                    Ok((None::<Table>, None::<Table>).into())
                }
            }

            Cmd::UpsertKV {
                ref key,
                ref seq,
                ref value,
            } => {
                let prev = self.kv.get(key).cloned();
                if seq.match_seq(&prev).is_err() {
                    return Ok((prev, None).into());
                }

                let new_seq = self.incr_seq(SEQ_GENERIC_KV);
                let record_value = (new_seq, value.clone());
                self.kv.insert(key.clone(), record_value.clone());
                tracing::debug!("applied UpsertKV: {} {:?}", key, record_value);

                Ok((prev, Some(record_value)).into())
            }

            Cmd::DeleteKVByKey { ref key, ref seq } => {
                let prev = self.kv.get(key).cloned();

                if seq.match_seq(&prev).is_err() {
                    return Ok((prev.clone(), prev).into());
                }

                self.kv.remove(key);
                tracing::debug!("applied DeleteByKeyKV: {} {}", key, seq);
                Ok((prev, None).into())
            }
        }
    }

    /// Initialize slots by assign nodes to everyone of them randomly, according to replicationn config.
    pub fn init_slots(&mut self) -> common_exception::Result<()> {
        for i in 0..self.slots.len() {
            self.assign_rand_nodes_to_slot(i)?;
        }

        Ok(())
    }

    /// Assign `n` random nodes to a slot thus the files associated to this slot are replicated to the corresponding nodes.
    /// This func does not cnosider nodes load and should only be used when a Dfs cluster is initiated.
    /// TODO(xp): add another func for load based assignment
    pub fn assign_rand_nodes_to_slot(&mut self, slot_index: usize) -> common_exception::Result<()> {
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
            .ok_or_else(|| ErrorCode::InvalidConfig(format!("slot not found: {}", slot_index)))?;

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

    pub fn get_table(&self, tid: &u64) -> Option<Table> {
        let x = self.tables.get(tid);
        x.cloned()
    }

    pub fn get_kv(&self, key: &str) -> Option<SeqValue> {
        let x = self.kv.get(key);
        x.cloned()
    }

    pub fn get_data_parts(&self, db_name: &str, table_name: &str) -> Option<Vec<DataPartInfo>> {
        let parts = self.tbl_parts.get(db_name);
        parts.and_then(|m| m.get(table_name)).map(Clone::clone)
    }

    pub fn append_data_parts(
        &mut self,
        db_name: &str,
        table_name: &str,
        append_res: &AppendResult,
    ) {
        let part_info = || {
            append_res
                .parts
                .iter()
                .map(|p| {
                    let loc = &p.location;
                    DataPartInfo {
                        part: Part {
                            name: loc.clone(),
                            version: 0,
                        },
                        stats: Statistics::new_exact(p.disk_bytes, p.rows),
                    }
                })
                .collect::<Vec<_>>()
        };
        self.tbl_parts
            .entry(db_name.to_string())
            .and_modify(move |e| {
                e.entry(table_name.to_string())
                    .and_modify(|v| v.append(&mut part_info()))
                    .or_insert_with(part_info);
            })
            .or_insert_with(|| {
                [(table_name.to_string(), part_info())]
                    .iter()
                    .cloned()
                    .collect()
            });
    }

    pub fn remove_table_data_parts(&mut self, db_name: &str, table_name: &str) {
        self.tbl_parts
            .remove(db_name)
            .and_then(|mut t| t.remove(table_name));
    }

    pub fn remove_db_data_parts(&mut self, db_name: &str) {
        self.tbl_parts.remove(db_name);
    }

    pub fn mget_kv(&self, keys: &[impl AsRef<str>]) -> Vec<Option<SeqValue>> {
        keys.iter()
            .map(|key| self.kv.get(key.as_ref()).cloned())
            .collect()
    }

    pub fn prefix_list_kv(&self, prefix: &str) -> Vec<(String, SeqValue)> {
        self.kv
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|v| (v.0.clone(), v.1.clone()))
            .collect()
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

impl Placement for StateMachine {
    fn get_slots(&self) -> &[Slot] {
        &self.slots
    }

    fn get_node(&self, node_id: &u64) -> Option<&Node> {
        self.nodes.get(node_id)
    }
}
