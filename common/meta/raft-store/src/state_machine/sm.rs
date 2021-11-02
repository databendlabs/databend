// Copyright 2020 Datafuse Labs.
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

use std::fmt::Debug;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_raft::raft::Entry;
use async_raft::raft::EntryPayload;
use async_raft::raft::MembershipConfig;
use common_exception::prelude::ErrorCode;
use common_exception::ToErrorCode;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::sled;
use common_meta_sled_store::AsKeySpace;
use common_meta_sled_store::SledKeySpace;
use common_meta_sled_store::SledTree;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::KVMeta;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_tracing::tracing;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

use crate::config::RaftConfig;
use crate::sled_key_spaces::ClientLastResps;
use crate::sled_key_spaces::Databases;
use crate::sled_key_spaces::GenericKV;
use crate::sled_key_spaces::Nodes;
use crate::sled_key_spaces::Sequences;
use crate::sled_key_spaces::StateMachineMeta;
use crate::sled_key_spaces::TableLookup;
use crate::sled_key_spaces::Tables;
use crate::state_machine::AppliedState;
use crate::state_machine::ClientLastRespValue;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaKey::Initialized;
use crate::state_machine::StateMachineMetaKey::LastApplied;
use crate::state_machine::StateMachineMetaKey::LastMembership;
use crate::state_machine::StateMachineMetaValue;
use crate::state_machine::TableLookupKey;
use crate::state_machine::TableLookupValue;

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

/// The state machine of the `MemStore`.
/// It includes user data and two raft-related informations:
/// `last_applied_logs` and `client_serial_responses` to achieve idempotence.
#[derive(Debug)]
pub struct StateMachine {
    // TODO(xp): config is not required. Remove it after snapshot is done.
    _config: RaftConfig,

    /// The dedicated sled db to store everything about a state machine.
    /// A state machine has several trees opened on this db.
    _db: sled::Db,

    /// The internal sled::Tree to store everything about a state machine:
    /// - Store initialization state and last applied in keyspace `StateMachineMeta`.
    /// - Every other state is store in its own keyspace such as `Nodes`.
    pub sm_tree: SledTree,
}

/// A key-value pair in a snapshot is a vec of two `Vec<u8>`.
pub type SnapshotKeyValue = Vec<Vec<u8>>;

/// Snapshot data for serialization and for transport.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SerializableSnapshot {
    /// A list of kv pairs.
    pub kvs: Vec<SnapshotKeyValue>,
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
    #[tracing::instrument(level = "debug", skip(config), fields(config_id=%config.config_id, prefix=%config.sled_tree_prefix))]
    pub fn tree_name(config: &RaftConfig, sm_id: u64) -> String {
        config.tree_name(format!("{}/{}", TREE_STATE_MACHINE, sm_id))
    }

    #[tracing::instrument(level = "debug", skip(config), fields(config_id=config.config_id.as_str()))]
    pub fn clean(config: &RaftConfig, sm_id: u64) -> common_exception::Result<()> {
        let tree_name = StateMachine::tree_name(config, sm_id);

        let db = get_sled_db();

        // it blocks and slow
        db.drop_tree(tree_name)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "drop prev state machine")?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(config), fields(config_id=config.config_id.as_str()))]
    pub async fn open(config: &RaftConfig, sm_id: u64) -> common_exception::Result<StateMachine> {
        let db = get_sled_db();

        let tree_name = StateMachine::tree_name(config, sm_id);

        let sm_tree = SledTree::open(&db, &tree_name, config.is_sync())?;

        let sm = StateMachine {
            _config: config.clone(),
            _db: db,

            sm_tree,
        };

        let inited = {
            let sm_meta = sm.sm_meta();
            sm_meta.get(&Initialized)?
        };

        if inited.is_some() {
            Ok(sm)
        } else {
            let sm_meta = sm.sm_meta();
            sm_meta
                .insert(&Initialized, &StateMachineMetaValue::Bool(true))
                .await?;
            Ok(sm)
        }
    }

    /// Create a snapshot.
    /// Returns:
    /// - an consistent iterator of all kvs;
    /// - the last applied log id
    /// - the last applied membership config
    /// - and a snapshot id
    pub fn snapshot(
        &self,
    ) -> common_exception::Result<(
        impl Iterator<Item = sled::Result<(IVec, IVec)>>,
        LogId,
        MembershipConfig,
        String,
    )> {
        let last_applied = self.get_last_applied()?;
        let mem = self.get_membership()?;

        // NOTE: An initialize node/cluster always has the first log contains membership config.
        let mem = mem.unwrap_or_default();

        let snapshot_idx = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied.term, last_applied.index, snapshot_idx
        );

        Ok((self.sm_tree.tree.iter(), last_applied, mem, snapshot_id))
    }

    /// Serialize a snapshot for transport.
    /// This step does not require a lock, since sled::Tree::iter() creates a consistent view on a tree
    /// no matter if there are other writes applied to the tree.
    pub fn serialize_snapshot(
        view: impl Iterator<Item = sled::Result<(IVec, IVec)>>,
    ) -> common_exception::Result<Vec<u8>> {
        let mut kvs = Vec::new();
        for rkv in view {
            let (k, v) = rkv.map_err_to_code(ErrorCode::MetaStoreDamaged, || "taking snapshot")?;
            kvs.push(vec![k.to_vec(), v.to_vec()]);
        }
        let snap = SerializableSnapshot { kvs };
        let snap = serde_json::to_vec(&snap)?;
        Ok(snap)
    }

    /// Internal func to get an auto-incr seq number.
    /// It is just what Cmd::IncrSeq does and is also used by Cmd that requires
    /// a unique id such as Cmd::AddDatabase which needs make a new database id.
    ///
    /// Note: this can only be called inside apply().
    async fn incr_seq(&self, key: &str) -> common_exception::Result<u64> {
        let sequences = self.sequences();

        let curr = sequences
            .update_and_fetch(&key.to_string(), |old| Some(old.unwrap_or_default() + 1))
            .await?;

        let curr = curr.unwrap();

        tracing::debug!("applied IncrSeq: {}={}", key, curr);

        Ok(curr.0)
    }

    /// Apply an log entry to state machine.
    ///
    /// If a duplicated log entry is detected by checking data.txid, no update
    /// will be made and the previous resp is returned. In this way a client is able to re-send a
    /// command safely in case of network failure etc.
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn apply(
        &mut self,
        entry: &Entry<LogEntry>,
    ) -> common_exception::Result<AppliedState> {
        // TODO(xp): all update need to be done in a tx.

        let log_id = &entry.log_id;

        let sm_meta = self.sm_meta();
        sm_meta
            .insert(&LastApplied, &StateMachineMetaValue::LogId(*log_id))
            .await?;

        match entry.payload {
            EntryPayload::Blank => {}
            EntryPayload::Normal(ref norm) => {
                let data = &norm.data;
                if let Some(ref txid) = data.txid {
                    if let Some((serial, resp)) = self.get_client_last_resp(&txid.client)? {
                        if serial == txid.serial {
                            return Ok(resp);
                        }
                    }
                }

                let resp = self.apply_cmd(&data.cmd).await?;

                if let Some(ref txid) = data.txid {
                    self.client_last_resp_update(&txid.client, (txid.serial, resp.clone()))
                        .await?;
                }
                return Ok(resp);
            }
            EntryPayload::ConfigChange(ref mem) => {
                sm_meta
                    .insert(
                        &LastMembership,
                        &StateMachineMetaValue::Membership(mem.membership.clone()),
                    )
                    .await?;
                return Ok(AppliedState::None);
            }
            EntryPayload::SnapshotPointer(_) => {}
        };

        Ok(AppliedState::None)
    }

    /// Apply a `Cmd` to state machine.
    ///
    /// Already applied log should be filtered out before passing into this function.
    /// This is the only entry to modify state machine.
    /// The `cmd` is always committed by raft before applying.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn apply_cmd(&mut self, cmd: &Cmd) -> common_exception::Result<AppliedState> {
        match cmd {
            Cmd::IncrSeq { ref key } => Ok(self.incr_seq(key).await?.into()),

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

            Cmd::CreateDatabase { ref name } => {
                let db_id = self.incr_seq(SEQ_DATABASE_ID).await?;

                let dbs = self.databases();

                let (prev, result) = self
                    .sub_tree_upsert(
                        dbs,
                        name,
                        &MatchSeq::Exact(0),
                        Operation::Update(db_id),
                        None,
                    )
                    .await?;

                // if it is just created
                if prev.is_none() && result.is_some() {
                    // TODO(xp): reconsider this impl. it may not be required.
                    self.incr_seq(SEQ_DATABASE_META_ID).await?;
                }

                tracing::debug!("applied create Database: {} {:?}", name, result);
                Ok(Change::new(prev, result).into())
            }

            Cmd::DropDatabase { ref name } => {
                let dbs = self.databases();

                let (prev, result) = self
                    .sub_tree_upsert(dbs, name, &MatchSeq::Any, Operation::Delete, None)
                    .await?;

                // if it is just deleted
                if prev.is_some() && result.is_none() {
                    // TODO(xp): reconsider this impl. it may not be required.
                    self.incr_seq(SEQ_DATABASE_META_ID).await?;
                }

                tracing::debug!("applied drop Database: {} {:?}", name, result);
                Ok(Change::new(prev, result).into())
            }

            Cmd::CreateTable {
                ref db_name,
                ref table_name,
                ref table_meta,
            } => {
                let db_id = self.get_db_id(db_name).await?;

                let lookup_key = TableLookupKey {
                    database_id: db_id,
                    table_name: table_name.to_string(),
                };

                let table_lookup_tree = self.table_lookup();
                let seq_table_id = table_lookup_tree.get(&lookup_key)?;

                if let Some(u) = seq_table_id {
                    let table_id = u.data.0;

                    let prev = self.get_table_by_id(&table_id)?;
                    let prev = prev.map(|x| TableIdent::new(table_id, x.seq));
                    return Ok((prev.clone(), prev).into());
                }

                let table_meta = table_meta.clone();
                let table_id = self.incr_seq(SEQ_TABLE_ID).await?;

                self.sub_tree_upsert(
                    table_lookup_tree,
                    &lookup_key,
                    &MatchSeq::Exact(0),
                    Operation::Update(TableLookupValue(table_id)),
                    None,
                )
                .await?;

                let (prev, result) = self
                    .sub_tree_upsert(
                        self.tables(),
                        &table_id,
                        &MatchSeq::Exact(0),
                        Operation::Update(table_meta),
                        None,
                    )
                    .await?;

                tracing::debug!("applied create Table: {}={:?}", table_name, result);

                if prev.is_none() && result.is_some() {
                    self.incr_seq(SEQ_DATABASE_META_ID).await?;
                }

                Ok(AppliedState::TableIdent {
                    prev: prev.map(|x| TableIdent::new(table_id, x.seq)),
                    result: result.map(|x| TableIdent::new(table_id, x.seq)),
                })
            }

            Cmd::DropTable {
                ref db_name,
                ref table_name,
            } => {
                let db_id = self.get_db_id(db_name).await?;

                let lookup_key = TableLookupKey {
                    database_id: db_id,
                    table_name: table_name.to_string(),
                };

                let table_lookup_tree = self.table_lookup();
                let seq_table_id = table_lookup_tree.get(&lookup_key)?;

                if seq_table_id.is_none() {
                    return Ok(Change::<TableMeta>::new(None, None).into());
                }

                self.sub_tree_upsert(
                    table_lookup_tree,
                    &lookup_key,
                    &MatchSeq::Any,
                    Operation::Delete,
                    None,
                )
                .await?;

                let tables = self.tables();
                let (prev, result) = self
                    .sub_tree_upsert(
                        tables,
                        &seq_table_id.unwrap().data.0,
                        &MatchSeq::Any,
                        Operation::Delete,
                        None,
                    )
                    .await?;
                if prev.is_some() && result.is_none() {
                    self.incr_seq(SEQ_DATABASE_META_ID).await?;
                }
                tracing::debug!("applied drop Table: {} {:?}", table_name, result);
                Ok(Change::new(prev, result).into())
            }

            Cmd::UpsertKV {
                key,
                seq,
                value: value_op,
                value_meta,
            } => {
                let (prev, result) = self
                    .sub_tree_upsert(self.kvs(), key, seq, value_op.clone(), value_meta.clone())
                    .await?;

                tracing::debug!("applied UpsertKV: {} {:?}", key, result);
                Ok(Change::new(prev, result).into())
            }

            Cmd::UpsertTableOptions {
                ref table_id,
                ref seq,
                ref table_options,
            } => {
                let prev = self.tables().get(table_id)?;

                // Unlike other Cmd, prev to be None is not allowed for upsert-options.
                let prev = match prev {
                    None => {
                        return Err(ErrorCode::UnknownTableId(format!("table_id:{}", table_id)))
                    }
                    Some(x) => x,
                };

                if seq.match_seq(&prev).is_err() {
                    let res = AppliedState::TableMeta(Change::new(Some(prev.clone()), Some(prev)));
                    return Ok(res);
                }

                let meta = prev.meta.clone();
                let mut table_meta = prev.data.clone();
                let opts = &mut table_meta.options;

                for (k, opt_v) in table_options {
                    match opt_v {
                        None => {
                            opts.remove(k);
                        }
                        Some(v) => {
                            opts.insert(k.to_string(), v.to_string());
                        }
                    }
                }

                let new_seq = self.incr_seq(Tables::NAME).await?;
                let sv = SeqV {
                    seq: new_seq,
                    meta,
                    data: table_meta,
                };

                self.tables().insert(table_id, &sv).await?;

                Ok(AppliedState::TableMeta(Change::new(Some(prev), Some(sv))))
            }
        }
    }

    async fn sub_tree_upsert<'s, V, KS>(
        &'s self,
        sub_tree: AsKeySpace<'s, KS>,
        key: &KS::K,
        seq: &MatchSeq,
        value_op: Operation<V>,
        value_meta: Option<KVMeta>,
    ) -> common_exception::Result<(Option<SeqV<V>>, Option<SeqV<V>>)>
    where
        V: Clone + Debug,
        KS: SledKeySpace<V = SeqV<V>>,
    {
        // TODO(xp): need to be done all in a tx

        let prev = sub_tree.get(key)?;

        // If prev is timed out, treat it as a None.
        let prev = Self::unexpired_opt(prev);

        if seq.match_seq(&prev).is_err() {
            return Ok((prev.clone(), prev));
        }

        // result is the state after applying an operation.
        let result = self
            .sub_tree_do_update(&sub_tree, key, prev.clone(), value_meta, value_op)
            .await?;

        tracing::debug!("applied upsert: {} {:?}", key, result);
        Ok((prev, result))
    }

    /// Update a record into a sled tree sub tree, defined by a KeySpace, without seq check.
    ///
    /// TODO(xp); this should be a method of sled sub tree
    async fn sub_tree_do_update<'s, V, KS>(
        &'s self,
        sub_tree: &AsKeySpace<'s, KS>,
        key: &KS::K,
        prev: Option<SeqV<V>>,
        value_meta: Option<KVMeta>,
        value_op: Operation<V>,
    ) -> common_exception::Result<Option<SeqV<V>>>
    where
        V: Clone + Debug,
        KS: SledKeySpace<V = SeqV<V>>,
    {
        let mut seq_kv_value = match value_op {
            Operation::Update(v) => SeqV::with_meta(0, value_meta.clone(), v),
            Operation::Delete => {
                sub_tree.remove(key, true).await?;
                return Ok(None);
            }
            Operation::AsIs => match prev {
                None => return Ok(None),
                Some(ref prev_kv_value) => prev_kv_value.clone().set_meta(value_meta),
            },
        };

        // insert the updated record.

        seq_kv_value.seq = self.incr_seq(KS::NAME).await?;

        sub_tree.insert(key, &seq_kv_value).await?;

        Ok(Some(seq_kv_value))
    }

    #[allow(clippy::ptr_arg)]
    async fn get_db_id(&self, db_name: &String) -> common_exception::Result<u64> {
        let seq_dbi = self
            .databases()
            .get(db_name)?
            .ok_or_else(|| ErrorCode::UnknownDatabase(db_name.to_string()))?;

        Ok(seq_dbi.data)
    }

    async fn client_last_resp_update(
        &self,
        key: &str,
        value: (u64, AppliedState),
    ) -> common_exception::Result<AppliedState> {
        let v = ClientLastRespValue {
            req_serial_num: value.0,
            res: value.1.clone(),
        };
        let kvs = self.client_last_resps();
        kvs.insert(&key.to_string(), &v).await?;

        Ok(value.1)
    }

    pub fn get_membership(&self) -> common_exception::Result<Option<MembershipConfig>> {
        let sm_meta = self.sm_meta();
        let mem = sm_meta
            .get(&StateMachineMetaKey::LastMembership)?
            .map(MembershipConfig::from);

        Ok(mem)
    }

    pub fn get_last_applied(&self) -> common_exception::Result<LogId> {
        let sm_meta = self.sm_meta();
        let last_applied = sm_meta
            .get(&LastApplied)?
            .map(LogId::from)
            .unwrap_or_default();

        Ok(last_applied)
    }

    pub fn get_client_last_resp(
        &self,
        key: &str,
    ) -> common_exception::Result<Option<(u64, AppliedState)>> {
        let client_last_resps = self.client_last_resps();
        let v: Option<ClientLastRespValue> = client_last_resps.get(&key.to_string())?;

        if let Some(resp) = v {
            return Ok(Some((resp.req_serial_num, resp.res)));
        }

        Ok(Some((0, AppliedState::None)))
    }

    #[allow(dead_code)]
    fn list_node_ids(&self) -> Vec<NodeId> {
        let sm_nodes = self.nodes();
        sm_nodes.range_keys(..).expect("fail to list nodes")
    }

    pub fn get_node(&self, node_id: &NodeId) -> common_exception::Result<Option<Node>> {
        let sm_nodes = self.nodes();
        sm_nodes.get(node_id)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn get_database(&self, name: &str) -> Result<Option<SeqV<u64>>, ErrorCode> {
        let dbs = self.databases();
        let x = dbs.get(&name.to_string())?;
        Ok(x)
    }

    pub fn get_databases(&self) -> Result<Vec<(String, u64)>, ErrorCode> {
        let mut res = vec![];

        let it = self.databases().range(..)?;
        for r in it {
            let (a, b) = r?;
            res.push((a, b.data));
        }

        Ok(res)
    }

    pub fn get_database_meta_ver(&self) -> common_exception::Result<Option<u64>> {
        let sequences = self.sequences();
        let res = sequences.get(&SEQ_DATABASE_META_ID.to_string())?;
        Ok(res.map(|x| x.0))
    }

    pub fn get_table_by_id(&self, tid: &u64) -> Result<Option<SeqV<TableMeta>>, ErrorCode> {
        let x = self.tables().get(tid)?;
        Ok(x)
    }

    pub async fn upsert_table(
        &self,
        table_id: u64,
        tbl: TableMeta,
        seq: &MatchSeq,
    ) -> Result<Option<SeqV<TableMeta>>, ErrorCode> {
        let tables = self.tables();
        let (_prev, result) = self
            .sub_tree_upsert(tables, &table_id, seq, Operation::Update(tbl), None)
            .await?;
        self.incr_seq(SEQ_DATABASE_META_ID).await?; // need this?
        Ok(result)
    }

    pub fn get_tables(&self, db_name: &str) -> Result<Vec<TableInfo>, ErrorCode> {
        let db = self.get_database(db_name)?;
        let db = match db {
            Some(x) => x,
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "unknown database {}",
                    db_name
                )));
            }
        };

        let db_id = db.data;

        let mut tbls = vec![];
        let tables = self.tables();
        let tables_iter = self.table_lookup().range(..)?;
        for r in tables_iter {
            let (k, seq_table_id) = r?;

            let got_db_id = k.database_id;
            let table_name = k.table_name;

            if got_db_id == db_id {
                let table_id = seq_table_id.data.0;

                let seq_table_meta = tables.get(&table_id)?.ok_or_else(|| {
                    ErrorCode::IllegalMetaState(format!(" table of id {}, not found", table_id))
                })?;

                let version = seq_table_meta.seq;
                let table_meta = seq_table_meta.data;

                let table_info = TableInfo::new(
                    db_name,
                    &table_name,
                    TableIdent::new(table_id, version),
                    table_meta,
                );

                tbls.push(table_info);
            }
        }

        Ok(tbls)
    }

    pub fn get_kv(&self, key: &str) -> common_exception::Result<Option<SeqV<Vec<u8>>>> {
        // TODO(xp) refine get(): a &str is enough for key
        let sv = self.kvs().get(&key.to_string())?;
        tracing::debug!("get_kv sv:{:?}", sv);
        let sv = match sv {
            None => return Ok(None),
            Some(sv) => sv,
        };

        Ok(Self::unexpired(sv))
    }

    pub fn mget_kv(
        &self,
        keys: &[impl AsRef<str>],
    ) -> common_exception::Result<Vec<Option<SeqV<Vec<u8>>>>> {
        let kvs = self.kvs();
        let mut res = vec![];
        for x in keys.iter() {
            let v = kvs.get(&x.as_ref().to_string())?;
            let v = Self::unexpired_opt(v);
            res.push(v)
        }

        Ok(res)
    }

    pub fn prefix_list_kv(
        &self,
        prefix: &str,
    ) -> common_exception::Result<Vec<(String, SeqV<Vec<u8>>)>> {
        let kvs = self.kvs();
        let kv_pairs = kvs.scan_prefix(&prefix.to_string())?;

        let x = kv_pairs.into_iter();

        // Convert expired to None
        let x = x.map(|(k, v)| (k, Self::unexpired(v)));
        // Remove None
        let x = x.filter(|(_k, v)| v.is_some());
        // Extract from an Option
        let x = x.map(|(k, v)| (k, v.unwrap()));

        Ok(x.collect())
    }

    fn unexpired_opt<V: Debug>(seq_value: Option<SeqV<V>>) -> Option<SeqV<V>> {
        match seq_value {
            None => None,
            Some(sv) => Self::unexpired(sv),
        }
    }
    fn unexpired<V: Debug>(seq_value: SeqV<V>) -> Option<SeqV<V>> {
        // TODO(xp): log must be assigned with a ts.

        // TODO(xp): background task to clean expired

        // TODO(xp): Caveat: The cleanup must be consistent across raft nodes:
        //           A conditional update, e.g. an upsert_kv() with MatchSeq::Eq(some_value),
        //           must be applied with the same timestamp on every raft node.
        //           Otherwise: node-1 could have applied a log with a ts that is smaller than value.expire_at,
        //           while node-2 may fail to apply the same log if it use a greater ts > value.expire_at.
        //           Thus:
        //           1. A raft log must have a field ts assigned by the leader. When applying, use this ts to
        //              check against expire_at to decide whether to purge it.
        //           2. A GET operation must not purge any expired entry. Since a GET is only applied to a node itself.
        //           3. The background task can only be triggered by the raft leader, by submit a "clean expired" log.

        // TODO(xp): maybe it needs a expiration queue for efficient cleaning up.

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        tracing::debug!("seq_value: {:?} now: {}", seq_value, now);

        if seq_value.get_expire_at() < now {
            None
        } else {
            Some(seq_value)
        }
    }
}

/// Key space support
impl StateMachine {
    pub fn sm_meta(&self) -> AsKeySpace<StateMachineMeta> {
        self.sm_tree.key_space()
    }

    pub fn nodes(&self) -> AsKeySpace<Nodes> {
        self.sm_tree.key_space()
    }

    /// A kv store of all other general purpose information.
    /// The value is tuple of a monotonic sequence number and userdata value in string.
    /// The sequence number is guaranteed to increment(by some value greater than 0) everytime the record changes.
    pub fn kvs(&self) -> AsKeySpace<GenericKV> {
        self.sm_tree.key_space()
    }

    /// storage of auto-incremental number.
    pub fn sequences(&self) -> AsKeySpace<Sequences> {
        self.sm_tree.key_space()
    }

    /// storage of client last resp to keep idempotent.
    pub fn client_last_resps(&self) -> AsKeySpace<ClientLastResps> {
        self.sm_tree.key_space()
    }

    pub fn databases(&self) -> AsKeySpace<Databases> {
        self.sm_tree.key_space()
    }

    pub fn tables(&self) -> AsKeySpace<Tables> {
        self.sm_tree.key_space()
    }

    pub fn table_lookup(&self) -> AsKeySpace<TableLookup> {
        self.sm_tree.key_space()
    }
}
