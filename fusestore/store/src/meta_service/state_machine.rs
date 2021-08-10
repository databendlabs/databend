// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs::remove_dir_all;
use std::sync::Arc;
use std::sync::Mutex;

use async_raft::LogId;
use common_exception::prelude::ErrorCode;
use common_exception::ToErrorCode;
use common_flights::storage_api_impl::AppendResult;
use common_flights::storage_api_impl::DataPartInfo;
use common_metatypes::Database;
use common_metatypes::MatchSeq;
use common_metatypes::MatchSeqExt;
use common_metatypes::SeqValue;
use common_metatypes::Table;
use common_planners::Part;
use common_planners::Statistics;
use common_tracing::tracing;
use serde::Deserialize;
use serde::Serialize;

use crate::configs;
use crate::meta_service::placement::rand_n_from_m;
use crate::meta_service::raft_db::get_sled_db;
use crate::meta_service::sled_key_space;
use crate::meta_service::sled_key_space::StateMachineMeta;
use crate::meta_service::AppliedState;
use crate::meta_service::AsKeySpace;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::meta_service::NodeId;
use crate::meta_service::Placement;
use crate::meta_service::SledSerde;
use crate::meta_service::SledTree;
use crate::meta_service::StateMachineMetaKey::Initialized;
use crate::meta_service::StateMachineMetaKey::LastApplied;
use crate::meta_service::StateMachineMetaValue;

/// seq number key to generate seq for the value of a `generic_kv` record.
const SEQ_GENERIC_KV: &str = "generic_kv";
/// seq number key to generate database id
const SEQ_DATABASE_ID: &str = "database_id";
/// seq number key to generate table id
const SEQ_TABLE_ID: &str = "table_id";
/// seq number key to database meta version
const SEQ_DATABASE_META_ID: &str = "database_meta_id";

/// sled db tree name for nodes
// const TREE_NODES: &str = "nodes";
// const TREE_META: &str = "meta";
const TREE_STATE_MACHINE: &str = "state_machine";

const DB_PREFIX: &str = "df_db_";
const TBL_PREFIX: &str = "df_table_";

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
#[derive(Debug)]
pub struct StateMachine {
    // TODO(xp): config is not required. Remove it after snapshot is done.
    config: configs::Config,

    /// The dedicated sled db to store everything about a state machine.
    /// A state machine has several trees opened on this db.
    _db: sled::Db,

    /// The internal sled::Tree to store everything about a state machine:
    /// - Store initialization state and last applied in keyspace `StateMachineMeta`.
    /// - Every other state is store in its own keyspace such as `Nodes`.
    ///
    /// TODO(xp): migrate other in-memory fields to `sm_tree`.
    pub sm_tree: SledTree,

    /// raft state: A mapping of client IDs to their state info:
    /// (serial, RaftResponse)
    /// This is used to de-dup client request, to impl idempotent operations.
    pub client_last_resp: HashMap<String, (u64, AppliedState)>,

    /// The file names stored in this cluster
    pub keys: BTreeMap<String, String>,

    /// storage of auto-incremental number.
    /// TODO(xp): temporarily make it a Arc<Mutex>, need to be modified while &StateMachine is immutably borrow.
    ///           remove it after making it a sled store.
    pub sequences: Arc<Mutex<BTreeMap<String, u64>>>,

    // cluster nodes, key distribution etc.
    pub slots: Vec<Slot>,

    pub replication: Replication,

    /// table parts， table id -> data parts
    pub table_parts: HashMap<u64, Vec<DataPartInfo>>,
}

/// Initialize state machine for the first time it is brought online.
#[derive(Debug, Default, Clone)]
pub struct StateMachineInitializer {
    /// The number of slots to allocated.
    initial_slots: Option<u64>,
    /// The replication strategy.
    replication: Option<Replication>,
}

impl StateMachineInitializer {
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

    /// Initialized the state machine for when it is created.
    pub fn init(&self, mut sm: StateMachine) -> StateMachine {
        let initial_slots = self.initial_slots.unwrap_or(3);
        let replication = self.replication.clone().unwrap_or(Replication::Mirror(1));

        sm.replication = replication;

        sm.slots.clear();

        for _i in 0..initial_slots {
            sm.slots.push(Slot::default());
        }

        sm
    }
}

/// A key-value pair in a snapshot is a vec of two `Vec<u8>`.
type SnapshotKeyValue = Vec<Vec<u8>>;

/// Snapshot data for serialization and for transport.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct SerializableSnapshot {
    /// A list of kv pairs.
    kvs: Vec<SnapshotKeyValue>,
}

impl SerializableSnapshot {
    /// Convert the snapshot to a `Vec<(type, name, iter)>` format for sled to import.
    pub fn sled_importable(self) -> Vec<(Vec<u8>, Vec<u8>, impl Iterator<Item = Vec<Vec<u8>>>)> {
        vec![(
            "tree".as_bytes().to_vec(),
            TREE_STATE_MACHINE.as_bytes().to_vec(),
            self.kvs.into_iter(),
        )]
    }
}

impl StateMachine {
    pub fn initializer() -> StateMachineInitializer {
        StateMachineInitializer {
            ..Default::default()
        }
    }

    /// Returns the temp dir for sled:Db for rebuilding state machine from snapshot.
    fn tmp_state_machine_dir(config: &configs::Config) -> String {
        config.meta_dir.clone() + "/sm-tmp"
    }

    pub async fn open(config: &configs::Config) -> common_exception::Result<StateMachine> {
        let db = get_sled_db();

        let tree_name = config.tree_name(TREE_STATE_MACHINE);

        let sm_tree = SledTree::open(&db, &tree_name, config.meta_sync()).await?;

        let sm = StateMachine {
            config: config.clone(),
            _db: db,

            sm_tree,

            client_last_resp: Default::default(),
            keys: BTreeMap::new(),
            sequences: Arc::new(Mutex::new(BTreeMap::new())),
            slots: Vec::new(),

            replication: Replication::Mirror(1),
            table_parts: HashMap::new(),
        };

        let inited = {
            let sm_meta = sm.sm_meta();
            sm_meta.get(&Initialized)?
        };

        if inited.is_some() {
            Ok(sm)
        } else {
            // Run the default init on a new state machine.
            // TODO(xp): initialization should be customizable.
            let sm = StateMachine::initializer().init(sm);
            let sm_meta = sm.sm_meta();
            sm_meta
                .insert(&Initialized, &StateMachineMetaValue::Bool(true))
                .await?;
            Ok(sm)
        }
    }

    /// Create a snapshot.
    /// TODO(xp): we only need iter one sled::Tree to take a snapshot.
    pub fn snapshot(&self) -> impl Iterator<Item = Vec<Vec<u8>>> {
        let its = self._db.export();
        for (typ, name, it) in its {
            if typ == b"tree" && name == TREE_STATE_MACHINE.as_bytes() {
                return it;
            }
        }
        panic!("no tree found: {}", TREE_STATE_MACHINE)
    }

    /// Serialize a snapshot for transport.
    /// TODO(xp): This step does not require a lock, since sled::Tree::iter() creates a consistent view on a tree
    ///           no matter if there are other writes applied to the tree.
    pub fn serialize_snapshot(
        view: impl Iterator<Item = Vec<Vec<u8>>>,
    ) -> common_exception::Result<Vec<u8>> {
        let mut kvs = Vec::new();
        for kv in view {
            kvs.push(kv);
        }

        let snap = SerializableSnapshot { kvs };

        let snap = serde_json::to_vec(&snap)?;
        Ok(snap)
    }

    /// Install a snapshot to build a state machine from it and replace the state machine with the new one.
    pub async fn install_snapshot(&mut self, data: &[u8]) -> common_exception::Result<()> {
        // TODO(xp): test install snapshot:
        //           The rename is not atomic: a correct way should be: create a temp tree, swap state machine.

        let snap: SerializableSnapshot = serde_json::from_slice(data)?;

        let tmp_path = StateMachine::tmp_state_machine_dir(&self.config);

        remove_dir_all(&tmp_path).map_err_to_code(ErrorCode::MetaStoreDamaged, || {
            format!("remove tmp state machine dir: {}", tmp_path)
        })?;

        let db = get_sled_db();
        // TODO(xp): with a shared db import is now allowed. It populate the entire db.
        db.import(snap.sled_importable());

        // sled::Db does not have a "flush" method, need to flush every tree one by one.
        for name in db.tree_names() {
            let n = String::from_utf8(name.to_vec())
                .map_err_to_code(ErrorCode::MetaStoreDamaged, || "invalid tree name")?;
            let t = db
                .open_tree(&name)
                .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                    format!("open sled tree: {}", n)
                })?;
            t.flush_async()
                .await
                .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                    format!("flush sled tree: {}", n)
                })?;
        }

        // close it, move its data dir, re-open it.
        drop(db);

        // TODO(xp): use checksum to check consistency?
        // TODO(xp): use a pointer to state machine dir to atomically switch to the new sm dir.
        // TODO(xp): reopen and replace

        let new_sm = StateMachine::open(&self.config).await?;
        *self = new_sm;
        Ok(())
    }

    /// Internal func to get an auto-incr seq number.
    /// It is just what Cmd::IncrSeq does and is also used by Cmd that requires
    /// a unique id such as Cmd::AddDatabase which needs make a new database id.
    fn incr_seq(&self, key: &str) -> u64 {
        let mut sequences = self.sequences.lock().unwrap();
        let prev = sequences.get(key);
        let curr = match prev {
            Some(v) => v + 1,
            None => 1,
        };
        sequences.insert(key.to_string(), curr);
        tracing::debug!("applied IncrSeq: {}={}", key, curr);

        curr
    }

    /// Apply an log entry to state machine.
    ///
    /// If a duplicated log entry is detected by checking data.txid, no update
    /// will be made and the previous resp is returned. In this way a client is able to re-send a
    /// command safely in case of network failure etc.
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn apply(&mut self, log_id: &LogId, data: &LogEntry) -> anyhow::Result<AppliedState> {
        // TODO(xp): all update need to be done in a tx.

        let sm_meta = self.sm_meta();
        sm_meta
            .insert(&LastApplied, &StateMachineMetaValue::LogId(*log_id))
            .await?;

        if let Some(ref txid) = data.txid {
            if let Some((serial, resp)) = self.client_last_resp.get(&txid.client) {
                if serial == &txid.serial {
                    return Ok(resp.clone());
                }
            }
        }

        let resp = self.apply_non_dup(data).await?;

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
    pub async fn apply_non_dup(
        &mut self,
        data: &LogEntry,
    ) -> common_exception::Result<AppliedState> {
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
                let sm_nodes = self.nodes();

                let prev = sm_nodes.get(node_id)?;

                if prev.is_some() {
                    Ok((prev, None).into())
                } else {
                    sm_nodes.insert(node_id, node).await?;
                    tracing::info!("applied AddNode: {}={:?}", node_id, node);
                    Ok((prev, Some(node.clone())).into())
                }
            }

            Cmd::CreateDatabase { ref name, .. } => {
                // - If the db present, return it.
                // - Otherwise, create a new one with next seq number as database id, and add it in to store.

                // TODO(ariesdevil): move to upper level and check name already include prefix.
                let name: &String = &[DB_PREFIX, name].join("");
                let db = self.get_database(name);
                if db.is_some() {
                    Ok((db.clone(), db).into())
                } else {
                    let db = Database {
                        database_id: self.incr_seq(SEQ_DATABASE_ID),
                        tables: HashMap::new(),
                    };
                    self.incr_seq(SEQ_DATABASE_META_ID);
                    let db_vec = db.serialize_into_vec();

                    let (prev, result) =
                        self.upsert_kv(name, &MatchSeq::Any, &Some(db_vec)).await?;
                    let de_database = |r: Option<SeqValue>| -> Option<Database> {
                        if let Some(r) = r {
                            Database::deserialize_from_vec(&r.1)
                        } else {
                            None::<Database>
                        }
                    };
                    tracing::debug!("applied CreateDatabase: {}={:?}", name, db);
                    Ok((de_database(prev), de_database(result)).into())
                }
            }

            Cmd::DropDatabase { ref name } => {
                let name: &String = &[DB_PREFIX, name].join("");
                let (prev, result) = self.upsert_kv(name, &MatchSeq::Any, &None).await?;
                self.remove_db_data_parts(name).await;
                self.incr_seq(SEQ_DATABASE_META_ID);
                let de_database = |r: Option<SeqValue>| -> Option<Database> {
                    if let Some(r) = r {
                        Database::deserialize_from_vec(&r.1)
                    } else {
                        None::<Database>
                    }
                };
                tracing::debug!("applied DropDatabase: {}", name);
                Ok((de_database(prev), de_database(result)).into())
            }

            Cmd::CreateTable {
                ref db_name,
                ref table_name,
                if_not_exists: _,
                ref table,
            } => {
                let db_name: &String = &[DB_PREFIX, db_name].join("");
                let mut db = self.get_database(db_name).unwrap();

                if db.tables.contains_key(table_name) {
                    let table_id = db.tables.get(table_name).unwrap();
                    let prev = self.get_table(table_id);
                    Ok((prev.clone(), prev).into())
                } else {
                    let table = Table {
                        table_id: self.incr_seq(SEQ_TABLE_ID),
                        schema: table.schema.clone(),
                        parts: table.parts.clone(),
                    };
                    self.incr_seq(SEQ_DATABASE_META_ID);
                    db.tables.insert(table_name.clone(), table.table_id);
                    self.upsert_kv(db_name, &MatchSeq::Any, &Some(db.serialize_into_vec()))
                        .await?;
                    let table_key: &String = &[TBL_PREFIX, &table.table_id.to_string()].join("");
                    let (prev, result) = self
                        .upsert_kv(table_key, &MatchSeq::Any, &Some(table.serialize_into_vec()))
                        .await?;
                    let de_table = |r: Option<SeqValue>| -> Option<Table> {
                        if let Some(r) = r {
                            Table::deserialize_from_vec(&r.1)
                        } else {
                            None::<Table>
                        }
                    };
                    tracing::debug!("applied CreateTable: {}={:?}", table_name, table);

                    Ok((de_table(prev), de_table(result)).into())
                }
            }

            Cmd::DropTable {
                ref db_name,
                ref table_name,
                if_exists: _,
            } => {
                let db_name: &String = &[DB_PREFIX, db_name].join("");
                let mut db = self.get_database(db_name).unwrap();
                let tbl_id = db.tables.get(table_name);
                if let Some(tbl_id) = tbl_id {
                    let tbl_id = tbl_id.to_owned();
                    db.tables.remove(table_name);
                    self.upsert_kv(db_name, &MatchSeq::Any, &Some(db.serialize_into_vec()))
                        .await?;
                    let table_key: &String = &[TBL_PREFIX, &tbl_id.to_string()].join("");
                    let (prev, result) = self.upsert_kv(table_key, &MatchSeq::Any, &None).await?;

                    self.remove_table_data_parts(db_name, table_name).await;

                    let de_table = |r: Option<SeqValue>| -> Option<Table> {
                        if let Some(r) = r {
                            Table::deserialize_from_vec(&r.1)
                        } else {
                            None::<Table>
                        }
                    };

                    self.incr_seq(SEQ_DATABASE_META_ID);

                    Ok((de_table(prev), de_table(result)).into())
                } else {
                    Ok((None::<Table>, None::<Table>).into())
                }
            }

            Cmd::UpsertKV {
                ref key,
                ref seq,
                ref value,
            } => {
                let (prev, result) = self.upsert_kv(key, seq, value).await?;
                Ok((prev, result).into())
            }
        }
    }

    pub async fn upsert_kv(
        &mut self,
        key: &str,
        seq: &MatchSeq,
        value: &Option<Vec<u8>>,
    ) -> common_exception::Result<(Option<SeqValue>, Option<SeqValue>)> {
        let kvs = self.kvs();

        let k = &key.to_string();
        let prev = kvs.get(k)?;
        if seq.match_seq(&prev).is_err() {
            return Ok((prev.clone(), prev));
        }

        let record_value = if let Some(v) = value {
            let new_seq = self.incr_seq(SEQ_GENERIC_KV);

            let record_value = (new_seq, v.clone());
            kvs.insert(k, &record_value).await?;

            Some(record_value)
        } else {
            kvs.remove(k, true).await?;

            None
        };

        tracing::debug!("applied UpsertKV: {} {:?}", key, record_value);
        Ok((prev, record_value))
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

        let mut node_ids = self.list_node_ids();
        node_ids.sort_unstable();
        let total = node_ids.len();
        let node_indexes = rand_n_from_m(total, n)?;

        let mut slot = self
            .slots
            .get_mut(slot_index)
            .ok_or_else(|| ErrorCode::InvalidConfig(format!("slot not found: {}", slot_index)))?;

        slot.node_ids = node_indexes.iter().map(|i| node_ids[*i]).collect();

        Ok(())
    }

    fn list_node_ids(&self) -> Vec<NodeId> {
        let sm_nodes = self.nodes();
        sm_nodes.range_keys(..).expect("fail to list nodes")
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub fn get_file(&self, key: &str) -> Option<String> {
        tracing::info!("meta::get_file: {}", key);
        let x = self.keys.get(key);
        tracing::info!("meta::get_file: {}={:?}", key, x);
        x.cloned()
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        // TODO(xp): handle error

        let sm_nodes = self.nodes();
        sm_nodes.get(node_id).expect("fail to get node")
    }

    pub fn get_database(&self, name: &str) -> Option<Database> {
        let name = if name.starts_with(DB_PREFIX) {
            name.to_string()
        } else {
            let tmp: String = [DB_PREFIX, name].join("");
            tmp
        };
        let seq_value = self.get_kv(&name);
        if let Some(sv) = seq_value {
            return Database::deserialize_from_vec(&sv.1);
        }
        None
    }

    pub fn get_databases(&self) -> Vec<(String, Database)> {
        let r = self.prefix_list_kv(DB_PREFIX);
        r.iter()
            .map(|v| {
                let db_name = v.0.clone();
                let db_name = db_name.strip_prefix(DB_PREFIX);
                (
                    db_name.unwrap().to_string(),
                    Database::deserialize_from_vec(&v.1 .1).unwrap(),
                )
            })
            .collect()
    }

    pub fn get_database_meta_ver(&self) -> Option<u64> {
        self.sequences
            .lock()
            .unwrap()
            .get(SEQ_DATABASE_META_ID)
            .cloned()
    }

    pub fn get_table(&self, tid: &u64) -> Option<Table> {
        let table_key: &String = &[TBL_PREFIX, &tid.to_string()].join("");

        let v = self.get_kv(table_key);
        if let Some(v) = v {
            return Table::deserialize_from_vec(&v.1);
        }
        None
    }

    pub fn get_tables(&self) -> Vec<(u64, Table)> {
        let r = self.prefix_list_kv(TBL_PREFIX);
        r.iter()
            .map(|v| {
                let tbl_key = v.0.clone();
                let tbl_id = tbl_key
                    .strip_prefix(TBL_PREFIX)
                    .map(|t| t.parse::<u64>().unwrap());
                (
                    tbl_id.unwrap(),
                    Table::deserialize_from_vec(&v.1 .1).unwrap(),
                )
            })
            .collect()
    }

    pub fn get_kv(&self, key: &str) -> Option<SeqValue> {
        // TODO(xp) refine get(): a &str is enough for key
        // TODO(xp): handle error
        self.kvs().get(&key.to_string()).unwrap()
    }

    pub fn get_data_parts(&self, db_name: &str, table_name: &str) -> Option<Vec<DataPartInfo>> {
        let db_name = if db_name.starts_with(DB_PREFIX) {
            db_name.to_string()
        } else {
            let tmp: String = [DB_PREFIX, db_name].join("");
            tmp
        };
        let db = self.get_database(&db_name);
        if let Some(db) = db {
            let table_id = db.tables.get(table_name);
            if let Some(table_id) = table_id {
                return self.table_parts.get(table_id).map(Clone::clone);
            }
        }
        None
    }

    pub async fn append_data_parts(
        &mut self,
        db_name: &str,
        table_name: &str,
        append_res: &AppendResult,
    ) {
        let part_infos = append_res
            .parts
            .iter()
            .map(|p| {
                let loc = &p.location;
                DataPartInfo {
                    part: Part {
                        name: loc.clone(),
                        version: 0,
                    },
                    stats: Statistics::new_exact(p.rows, p.disk_bytes),
                }
            })
            .collect::<Vec<_>>();

        let db_name: &str = &[DB_PREFIX, db_name].join("");
        let db = self.get_database(db_name);
        if let Some(db) = db {
            let table_id = db.tables.get(table_name);
            if let Some(table_id) = table_id {
                for part in part_infos {
                    let mut table = self.get_table(table_id).unwrap();
                    table.parts.insert(part.part.name.clone());
                    let table_key: &String = &[TBL_PREFIX, &table_id.to_string()].join("");
                    self.upsert_kv(table_key, &MatchSeq::Any, &Some(table.serialize_into_vec()))
                        .await
                        .unwrap();
                    // These comments are intentionally left here.
                    // As rustc not smart enough, it says:
                    // for part in part_infos {
                    //     ---- move occurs because `part` has type `DataPartInfo`, which does not implement the `Copy` trait
                    //     .and_modify(|v| v.push(part))
                    //                 ---        ---- variable moved due to use in closure
                    //                 |
                    //                 value moved into closure here
                    //     .or_insert_with(|| vec![part]);
                    //                     ^^      ---- use occurs due to use in closure
                    //                     |
                    //                     value used here after move
                    // But obviously the two methods can't happen at the same time.
                    // ============== previous =============
                    // self.table_parts
                    //     .entry(*table_id)
                    //     .and_modify(|v| v.push(part))
                    //     .or_insert_with(|| vec![part]);
                    // ============ previous end ===========
                    match self.table_parts.get_mut(table_id) {
                        Some(p) => {
                            p.push(part);
                        }
                        None => {
                            self.table_parts.insert(*table_id, vec![part]);
                        }
                    }
                }
            }
        }
    }

    pub async fn remove_table_data_parts(&mut self, db_name: &str, table_name: &str) {
        let db_name = if db_name.starts_with(DB_PREFIX) {
            db_name.to_string()
        } else {
            let tmp: String = [DB_PREFIX, db_name].join("");
            tmp
        };
        let db = self.get_database(&db_name);
        if let Some(db) = db {
            let table_id = db.tables.get(table_name);
            if let Some(table_id) = table_id {
                let table = self.get_table(table_id);
                if let Some(mut table) = table {
                    table.parts.clear();
                    let table_key: &String = &[TBL_PREFIX, &table_id.to_string()].join("");
                    self.upsert_kv(table_key, &MatchSeq::Any, &Some(table.serialize_into_vec()))
                        .await
                        .unwrap();
                    self.table_parts.remove(table_id);
                }
            }
        }
    }

    pub async fn remove_db_data_parts(&mut self, db_name: &str) {
        let db_name = if db_name.starts_with(DB_PREFIX) {
            db_name.to_string()
        } else {
            let tmp: String = [DB_PREFIX, db_name].join("");
            tmp
        };
        let db = self.get_database(&db_name);
        if let Some(db) = db {
            for table_id in db.tables.values() {
                let table = self.get_table(table_id);
                if let Some(mut table) = table {
                    table.parts.clear();
                    let table_key: &String = &[TBL_PREFIX, &table_id.to_string()].join("");
                    self.upsert_kv(table_key, &MatchSeq::Any, &Some(table.serialize_into_vec()))
                        .await
                        .unwrap();
                    self.table_parts.remove(table_id);
                }
            }
        }
    }

    pub fn mget_kv(&self, keys: &[impl AsRef<str>]) -> Vec<Option<SeqValue>> {
        // TODO(xp): handle error
        let kvs = self.kvs();
        keys.iter()
            .map(|key| kvs.get(&key.as_ref().to_string()).unwrap())
            .collect()
    }

    pub fn prefix_list_kv(&self, prefix: &str) -> Vec<(String, SeqValue)> {
        let kvs = self.kvs();
        // TODO(xp): handle error
        let kv_pairs = kvs.range(prefix.to_string()..).unwrap();

        let x = kv_pairs
            .into_iter()
            .take_while(|(k, _)| k.starts_with(prefix));

        x.collect()
    }
}

/// Key space support
impl StateMachine {
    pub fn sm_meta(&self) -> AsKeySpace<StateMachineMeta> {
        self.sm_tree.key_space()
    }
    pub fn nodes(&self) -> AsKeySpace<sled_key_space::Nodes> {
        self.sm_tree.key_space()
    }
    pub fn files(&self) -> AsKeySpace<sled_key_space::Files> {
        self.sm_tree.key_space()
    }

    /// A kv store of all other general purpose information.
    /// The value is tuple of a monotonic sequence number and userdata value in string.
    /// The sequence number is guaranteed to increment(by some value greater than 0) everytime the record changes.
    pub fn kvs(&self) -> AsKeySpace<sled_key_space::GenericKV> {
        self.sm_tree.key_space()
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

/// For Node to be able to be stored in sled::Tree as a value.
impl SledSerde for Node {}

impl Placement for StateMachine {
    fn get_slots(&self) -> &[Slot] {
        &self.slots
    }

    fn get_placement_node(&self, node_id: &NodeId) -> Option<Node> {
        self.get_node(node_id)
    }
}
