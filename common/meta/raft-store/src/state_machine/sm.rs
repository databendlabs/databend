// Copyright 2021 Datafuse Labs.
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

use std::convert::TryInto;
use std::fmt::Debug;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::EffectiveMembership;
use common_meta_sled_store::openraft::MessageSummary;
use common_meta_sled_store::sled;
use common_meta_sled_store::AsKeySpace;
use common_meta_sled_store::AsTxnKeySpace;
use common_meta_sled_store::SledKeySpace;
use common_meta_sled_store::SledTree;
use common_meta_sled_store::Store;
use common_meta_sled_store::TransactionSledTree;
use common_meta_types::error_context::WithContext;
use common_meta_types::AppError;
use common_meta_types::AppliedState;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::DatabaseMeta;
use common_meta_types::KVMeta;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::MetaError;
use common_meta_types::MetaResult;
use common_meta_types::MetaStorageError;
use common_meta_types::MetaStorageResult;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::TableMeta;
use common_meta_types::UnknownDatabase;
use common_meta_types::UnknownDatabaseId;
use common_meta_types::UnknownTableId;
use common_tracing::tracing;
use openraft::raft::Entry;
use openraft::raft::EntryPayload;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

use crate::config::RaftConfig;
use crate::sled_key_spaces::ClientLastResps;
use crate::sled_key_spaces::DatabaseLookup;
use crate::sled_key_spaces::Databases;
use crate::sled_key_spaces::GenericKV;
use crate::sled_key_spaces::Nodes;
use crate::sled_key_spaces::Sequences;
use crate::sled_key_spaces::StateMachineMeta;
use crate::sled_key_spaces::TableLookup;
use crate::sled_key_spaces::Tables;
use crate::state_machine::ClientLastRespValue;
use crate::state_machine::DatabaseLookupKey;
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
    pub fn clean(config: &RaftConfig, sm_id: u64) -> Result<(), MetaStorageError> {
        let tree_name = StateMachine::tree_name(config, sm_id);

        let db = get_sled_db();

        // it blocks and slow
        db.drop_tree(tree_name)
            .context(|| "drop prev state machine")?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(config), fields(config_id=config.config_id.as_str()))]
    pub async fn open(config: &RaftConfig, sm_id: u64) -> Result<StateMachine, MetaStorageError> {
        let db = get_sled_db();

        let tree_name = StateMachine::tree_name(config, sm_id);

        let sm_tree = SledTree::open(&db, &tree_name, config.is_sync())?;

        let sm = StateMachine { sm_tree };

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
    ) -> MetaStorageResult<(
        impl Iterator<Item = sled::Result<(IVec, IVec)>>,
        LogId,
        String,
    )> {
        let last_applied = self.get_last_applied()?;

        // NOTE: An initialize node/cluster always has the first log contains membership config.

        let last_applied =
            last_applied.expect("not allowed to build snapshot with empty state machine");

        let snapshot_idx = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied.term, last_applied.index, snapshot_idx
        );

        Ok((self.sm_tree.tree.iter(), last_applied, snapshot_id))
    }

    /// Serialize a snapshot for transport.
    /// This step does not require a lock, since sled::Tree::iter() creates a consistent view on a tree
    /// no matter if there are other writes applied to the tree.
    pub fn serialize_snapshot(
        view: impl Iterator<Item = sled::Result<(IVec, IVec)>>,
    ) -> MetaStorageResult<Vec<u8>> {
        let mut kvs = Vec::new();
        for rkv in view {
            let (k, v) = rkv.context(|| "taking snapshot")?;
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
    async fn incr_seq(&self, key: &str) -> MetaResult<u64> {
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
    #[tracing::instrument(level = "debug", skip(self, entry), fields(log_id=%entry.log_id))]
    pub async fn apply(&self, entry: &Entry<LogEntry>) -> Result<AppliedState, MetaStorageError> {
        tracing::debug!("apply: summary: {}", entry.summary());
        tracing::debug!("apply: payload: {:?}", entry.payload);

        let log_id = &entry.log_id;

        tracing::debug!("sled tx start: {:?}", entry);

        let result = self.sm_tree.txn(true, move |txn_tree| {
            let txn_sm_meta = txn_tree.key_space::<StateMachineMeta>();
            txn_sm_meta.insert(&LastApplied, &StateMachineMetaValue::LogId(*log_id))?;

            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(ref data) => {
                    if let Some(ref txid) = data.txid {
                        let (serial, resp) =
                            self.txn_get_client_last_resp(&txid.client, &txn_tree)?;
                        if serial == txid.serial {
                            return Ok(Some(resp));
                        }
                    }

                    let res = self.apply_cmd(&data.cmd, &txn_tree);
                    let applied_state = res?;

                    if let Some(ref txid) = data.txid {
                        self.txn_client_last_resp_update(
                            &txid.client,
                            (txid.serial, applied_state.clone()),
                            &txn_tree,
                        )?;
                    }
                    return Ok(Some(applied_state));
                }
                EntryPayload::Membership(ref mem) => {
                    txn_sm_meta.insert(
                        &LastMembership,
                        &StateMachineMetaValue::Membership(EffectiveMembership {
                            log_id: *log_id,
                            membership: mem.clone(),
                        }),
                    )?;
                    return Ok(Some(AppliedState::None));
                }
            };

            Ok(None)
        });

        let opt_applied_state = match result {
            Ok(x) => x,
            Err(meta_sto_err) => {
                return match meta_sto_err {
                    MetaStorageError::AppError(app_err) => Ok(AppliedState::AppError(app_err)),
                    _ => Err(meta_sto_err),
                }
            }
        };

        tracing::debug!("sled tx done: {:?}", entry);

        let applied_state = match opt_applied_state {
            Some(r) => r,
            None => AppliedState::None,
        };

        Ok(applied_state)
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_incr_seq_cmd(
        &self,
        key: &str,

        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let r = self.txn_incr_seq(key, txn_tree)?;

        Ok(r.into())
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_add_node_cmd(
        &self,
        node_id: &u64,
        node: &Node,

        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let sm_nodes = txn_tree.key_space::<Nodes>();

        let prev = sm_nodes.get(node_id)?;

        if prev.is_some() {
            Ok((prev, None).into())
        } else {
            sm_nodes.insert(node_id, node)?;
            tracing::info!("applied AddNode: {}={:?}", node_id, node);
            Ok((prev, Some(node.clone())).into())
        }
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_create_database_cmd(
        &self,
        tenant: &str,
        name: &str,
        meta: &DatabaseMeta,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let db_id = self.txn_incr_seq(SEQ_DATABASE_ID, txn_tree)?;

        let db_lookup_tree = txn_tree.key_space::<DatabaseLookup>();

        let (prev, result) = self.txn_sub_tree_upsert(
            &db_lookup_tree,
            &DatabaseLookupKey::new(tenant.to_string(), name.to_string()),
            &MatchSeq::Exact(0),
            Operation::Update(db_id),
            None,
        )?;

        // if it is just created
        if prev.is_none() && result.is_some() {
            // TODO(xp): reconsider this impl. it may not be required.
            self.txn_incr_seq(SEQ_DATABASE_META_ID, txn_tree)?;
        } else {
            // exist
            let db_id = prev.unwrap().data;
            let prev = self.txn_get_database_meta_by_id(&db_id, txn_tree)?;
            if let Some(prev) = prev {
                return Ok(AppliedState::DatabaseMeta(Change::nochange_with_id(
                    db_id,
                    Some(prev),
                )));
            }
        }

        let dbs = txn_tree.key_space::<Databases>();
        let (prev_meta, result_meta) = self.txn_sub_tree_upsert(
            &dbs,
            &db_id,
            &MatchSeq::Exact(0),
            Operation::Update(meta.clone()),
            None,
        )?;

        if prev_meta.is_none() && result_meta.is_some() {
            self.txn_incr_seq(SEQ_DATABASE_META_ID, txn_tree)?;
        }

        tracing::debug!(
            "applied create Database: {}, db_id: {}, meta: {:?}",
            name,
            db_id,
            result
        );

        Ok(AppliedState::DatabaseMeta(Change::new_with_id(
            db_id,
            prev_meta,
            result_meta,
        )))
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_drop_database_cmd(
        &self,
        tenant: &str,
        name: &str,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let dbs = txn_tree.key_space::<DatabaseLookup>();

        let (prev, result) = self.txn_sub_tree_upsert(
            &dbs,
            &DatabaseLookupKey::new(tenant.to_string(), name.to_string()),
            &MatchSeq::Any,
            Operation::Delete,
            None,
        )?;

        assert!(
            result.is_none(),
            "delete with MatchSeq::Any always succeeds"
        );

        // if it is just deleted
        if let Some(seq_db_id) = prev {
            // TODO(xp): reconsider this impl. it may not be required.
            self.txn_incr_seq(SEQ_DATABASE_META_ID, txn_tree)?;

            let db_id = seq_db_id.data;

            let dbs = txn_tree.key_space::<Databases>();
            let (prev_meta, result_meta) =
                self.txn_sub_tree_upsert(&dbs, &db_id, &MatchSeq::Any, Operation::Delete, None)?;

            tracing::debug!("applied drop Database: {} {:?}", name, result);

            return Ok(AppliedState::DatabaseMeta(Change::new_with_id(
                db_id,
                prev_meta,
                result_meta,
            )));
        }

        // not exist

        tracing::debug!("applied drop Database: {} {:?}", name, result);
        Ok(AppliedState::DatabaseMeta(Change::new(None, None)))
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_create_table_cmd(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
        table_meta: &TableMeta,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let db_id = self.txn_get_database_id(tenant, db_name, txn_tree)?;

        let lookup_key = TableLookupKey {
            database_id: db_id,
            table_name: table_name.to_string(),
        };

        let table_lookup_tree = txn_tree.key_space::<TableLookup>();
        let seq_table_id = table_lookup_tree.get(&lookup_key)?;

        if let Some(u) = seq_table_id {
            let table_id = u.data.0;

            let prev = self.txn_get_table_meta_by_id(&table_id, txn_tree)?;

            return Ok(AppliedState::TableMeta(Change::nochange_with_id(
                table_id, prev,
            )));
        }

        let table_meta = table_meta.clone();
        let table_id = self.txn_incr_seq(SEQ_TABLE_ID, txn_tree)?;

        self.txn_sub_tree_upsert(
            &table_lookup_tree,
            &lookup_key,
            &MatchSeq::Exact(0),
            Operation::Update(TableLookupValue(table_id)),
            None,
        )?;

        let table_tree = txn_tree.key_space::<Tables>();
        let (prev, result) = self.txn_sub_tree_upsert(
            &table_tree,
            &table_id,
            &MatchSeq::Exact(0),
            Operation::Update(table_meta),
            None,
        )?;

        tracing::debug!("applied create Table: {}={:?}", table_name, result);

        if prev.is_none() && result.is_some() {
            self.txn_incr_seq(SEQ_DATABASE_META_ID, txn_tree)?;
        }

        Ok(AppliedState::TableMeta(Change::new_with_id(
            table_id, prev, result,
        )))
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_drop_table_cmd(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let db_id = self.txn_get_database_id(tenant, db_name, txn_tree)?;

        let lookup_key = TableLookupKey {
            database_id: db_id,
            table_name: table_name.to_string(),
        };

        let table_lookup_tree = txn_tree.key_space::<TableLookup>();
        let seq_table_id = table_lookup_tree.get(&lookup_key)?;

        if seq_table_id.is_none() {
            return Ok(Change::<TableMeta>::new(None, None).into());
        }

        let table_id = seq_table_id.unwrap().data.0;

        self.txn_sub_tree_upsert(
            &table_lookup_tree,
            &lookup_key,
            &MatchSeq::Any,
            Operation::Delete,
            None,
        )?;

        let tables = txn_tree.key_space::<Tables>();
        let (prev, result) =
            self.txn_sub_tree_upsert(&tables, &table_id, &MatchSeq::Any, Operation::Delete, None)?;
        if prev.is_some() && result.is_none() {
            self.txn_incr_seq(SEQ_DATABASE_META_ID, txn_tree)?;
        }
        tracing::debug!("applied drop Table: {} {:?}", table_name, result);
        Ok(Change::new_with_id(table_id, prev, result).into())
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_update_kv_cmd(
        &self,
        key: &str,
        seq: &MatchSeq,
        value_op: &Operation<Vec<u8>>,
        value_meta: &Option<KVMeta>,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let sub_tree = txn_tree.key_space::<GenericKV>();
        let (prev, result) = self.txn_sub_tree_upsert(
            &sub_tree,
            &key.to_string(),
            seq,
            value_op.clone(),
            value_meta.clone(),
        )?;

        tracing::debug!("applied UpsertKV: {} {:?}", key, result);
        Ok(Change::new(prev, result).into())
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_upsert_table_options_cmd(
        &self,
        req: &common_meta_types::UpsertTableOptionReq,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let table_tree = txn_tree.key_space::<Tables>();
        let prev = table_tree.get(&req.table_id)?;

        // Unlike other Cmd, prev to be None is not allowed for upsert-options.
        let prev = prev.ok_or_else(|| {
            MetaStorageError::AppError(AppError::UnknownTableId(UnknownTableId::new(
                req.table_id,
                "apply_upsert_table_options_cmd".to_string(),
            )))
        })?;

        if req.seq.match_seq(&prev).is_err() {
            let res = AppliedState::TableMeta(Change::new(Some(prev.clone()), Some(prev)));
            return Ok(res);
        }

        let meta = prev.meta.clone();
        let mut table_meta = prev.data.clone();
        let opts = &mut table_meta.options;

        for (k, opt_v) in &req.options {
            match opt_v {
                None => {
                    opts.remove(k);
                }
                Some(v) => {
                    opts.insert(k.to_string(), v.to_string());
                }
            }
        }

        let new_seq = self.txn_incr_seq(Tables::NAME, txn_tree)?;
        let sv = SeqV {
            seq: new_seq,
            meta,
            data: table_meta,
        };

        table_tree.insert(&req.table_id, &sv)?;

        Ok(AppliedState::TableMeta(Change::new_with_id(
            req.table_id,
            Some(prev),
            Some(sv),
        )))
    }

    /// Apply a `Cmd` to state machine.
    ///
    /// Already applied log should be filtered out before passing into this function.
    /// This is the only entry to modify state machine.
    /// The `cmd` is always committed by raft before applying.
    #[tracing::instrument(level = "debug", skip(self, cmd, txn_tree))]
    pub fn apply_cmd(
        &self,
        cmd: &Cmd,
        txn_tree: &TransactionSledTree,
    ) -> Result<AppliedState, MetaStorageError> {
        tracing::debug!("apply_cmd: {:?}", cmd);

        match cmd {
            Cmd::IncrSeq { ref key } => self.apply_incr_seq_cmd(key, txn_tree),

            Cmd::AddNode {
                ref node_id,
                ref node,
            } => self.apply_add_node_cmd(node_id, node, txn_tree),

            Cmd::CreateDatabase {
                ref tenant,
                ref name,
                ref meta,
            } => self.apply_create_database_cmd(tenant, name, meta, txn_tree),

            Cmd::DropDatabase {
                ref tenant,
                ref name,
            } => self.apply_drop_database_cmd(tenant, name, txn_tree),

            Cmd::CreateTable {
                ref tenant,
                ref db_name,
                ref table_name,
                ref table_meta,
            } => self.apply_create_table_cmd(tenant, db_name, table_name, table_meta, txn_tree),

            Cmd::DropTable {
                tenant,
                ref db_name,
                ref table_name,
            } => self.apply_drop_table_cmd(tenant, db_name, table_name, txn_tree),

            Cmd::UpsertKV {
                key,
                seq,
                value: value_op,
                value_meta,
            } => self.apply_update_kv_cmd(key, seq, value_op, value_meta, txn_tree),

            Cmd::UpsertTableOptions(ref req) => self.apply_upsert_table_options_cmd(req, txn_tree),
        }
    }

    async fn sub_tree_upsert<'s, V, KS>(
        &'s self,
        sub_tree: AsKeySpace<'s, KS>,
        key: &KS::K,
        seq: &MatchSeq,
        value_op: Operation<V>,
        value_meta: Option<KVMeta>,
    ) -> MetaResult<(Option<SeqV<V>>, Option<SeqV<V>>)>
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

    async fn sub_tree_do_update<'s, V, KS>(
        &'s self,
        sub_tree: &AsKeySpace<'s, KS>,
        key: &KS::K,
        prev: Option<SeqV<V>>,
        value_meta: Option<KVMeta>,
        value_op: Operation<V>,
    ) -> MetaResult<Option<SeqV<V>>>
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

    fn txn_incr_seq(&self, key: &str, txn_tree: &TransactionSledTree) -> MetaStorageResult<u64> {
        let seq_sub_tree = txn_tree.key_space::<Sequences>();

        let key = key.to_string();
        let curr = seq_sub_tree.update_and_fetch(&key, |old| Some(old.unwrap_or_default() + 1))?;
        let curr = curr.unwrap();

        tracing::debug!("applied IncrSeq: {}={}", key, curr);

        Ok(curr.0)
    }

    #[allow(clippy::type_complexity)]
    fn txn_sub_tree_upsert<'s, V, KS>(
        &'s self,
        sub_tree: &AsTxnKeySpace<'s, KS>,
        key: &KS::K,
        seq: &MatchSeq,
        value_op: Operation<V>,
        value_meta: Option<KVMeta>,
    ) -> MetaStorageResult<(Option<SeqV<V>>, Option<SeqV<V>>)>
    where
        V: Clone + Debug,
        KS: SledKeySpace<V = SeqV<V>>,
    {
        let prev = sub_tree.get(key)?;

        // If prev is timed out, treat it as a None.
        let prev = Self::unexpired_opt(prev);

        if seq.match_seq(&prev).is_err() {
            return Ok((prev.clone(), prev));
        }

        // result is the state after applying an operation.
        let result =
            self.txn_sub_tree_do_update(sub_tree, key, prev.clone(), value_meta, value_op)?;

        tracing::debug!("applied upsert: {} {:?}", key, result);
        Ok((prev, result))
    }

    /// Update a record into a sled tree sub tree, defined by a KeySpace, without seq check.
    ///
    /// TODO(xp); this should be a method of sled sub tree
    fn txn_sub_tree_do_update<'s, V, KS>(
        &'s self,
        sub_tree: &AsTxnKeySpace<'s, KS>,
        key: &KS::K,
        prev: Option<SeqV<V>>,
        value_meta: Option<KVMeta>,
        value_op: Operation<V>,
    ) -> MetaStorageResult<Option<SeqV<V>>>
    where
        V: Clone + Debug,
        KS: SledKeySpace<V = SeqV<V>>,
    {
        let mut seq_kv_value = match value_op {
            Operation::Update(v) => SeqV::with_meta(0, value_meta, v),
            Operation::Delete => {
                sub_tree.remove(key)?;
                return Ok(None);
            }
            Operation::AsIs => match prev {
                None => return Ok(None),
                Some(ref prev_kv_value) => prev_kv_value.clone().set_meta(value_meta),
            },
        };

        seq_kv_value.seq = self.txn_incr_seq(KS::NAME, sub_tree)?;

        sub_tree.insert(key, &seq_kv_value)?;

        Ok(Some(seq_kv_value))
    }

    pub fn get_database_id(&self, tenant: &str, db_name: &str) -> MetaStorageResult<u64> {
        let seq_dbi = self
            .database_lookup()
            .get(&(DatabaseLookupKey::new(tenant.to_string(), db_name.to_string())))?
            .ok_or_else(|| AppError::from(UnknownDatabase::new(db_name, "get_database_id")))?;

        Ok(seq_dbi.data)
    }

    pub fn txn_get_database_id(
        &self,
        tenant: &str,
        db_name: &str,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<u64> {
        let txn_db_lookup = txn_tree.key_space::<DatabaseLookup>();
        let seq_dbi = txn_db_lookup
            .get(&(DatabaseLookupKey::new(tenant.to_string(), db_name.to_string())))?
            .ok_or_else(|| {
                AppError::UnknownDatabase(UnknownDatabase::new(
                    db_name.to_string(),
                    "txn_get_database_id".to_string(),
                ))
            })?;

        Ok(seq_dbi.data)
    }

    fn txn_client_last_resp_update(
        &self,
        key: &str,
        value: (u64, AppliedState),
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let v = ClientLastRespValue {
            req_serial_num: value.0,
            res: value.1.clone(),
        };
        let txn_ks = txn_tree.key_space::<ClientLastResps>();
        txn_ks.insert(&key.to_string(), &v)?;

        Ok(value.1)
    }

    pub fn get_membership(&self) -> MetaStorageResult<Option<EffectiveMembership>> {
        let sm_meta = self.sm_meta();
        let mem = sm_meta
            .get(&StateMachineMetaKey::LastMembership)?
            .map(|x| x.try_into().expect("Membership"));

        Ok(mem)
    }

    pub fn get_last_applied(&self) -> MetaStorageResult<Option<LogId>> {
        let sm_meta = self.sm_meta();
        let last_applied = sm_meta
            .get(&LastApplied)?
            .map(|x| x.try_into().expect("LogId"));

        Ok(last_applied)
    }

    pub fn get_client_last_resp(&self, key: &str) -> MetaResult<Option<(u64, AppliedState)>> {
        let client_last_resps = self.client_last_resps();
        let v: Option<ClientLastRespValue> = client_last_resps.get(&key.to_string())?;

        if let Some(resp) = v {
            return Ok(Some((resp.req_serial_num, resp.res)));
        }

        Ok(Some((0, AppliedState::None)))
    }

    pub fn txn_get_client_last_resp(
        &self,
        key: &str,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<(u64, AppliedState)> {
        let client_last_resps = txn_tree.key_space::<ClientLastResps>();
        let v = client_last_resps.get(&key.to_string())?;

        if let Some(resp) = v {
            return Ok((resp.req_serial_num, resp.res));
        }
        Ok((0, AppliedState::None))
    }

    #[allow(dead_code)]
    fn list_node_ids(&self) -> Vec<NodeId> {
        let sm_nodes = self.nodes();
        sm_nodes.range_keys(..).expect("fail to list nodes")
    }

    pub fn get_node(&self, node_id: &NodeId) -> MetaResult<Option<Node>> {
        let sm_nodes = self.nodes();
        match sm_nodes.get(node_id) {
            Ok(e) => Ok(e),
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_nodes(&self) -> MetaResult<Vec<Node>> {
        let sm_nodes = self.nodes();
        match sm_nodes.range_values(..) {
            Ok(e) => Ok(e),
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_database_meta_by_id(&self, db_id: &u64) -> MetaStorageResult<SeqV<DatabaseMeta>> {
        let x = self.databases().get(db_id)?.ok_or_else(|| {
            MetaStorageError::AppError(AppError::UnknownDatabaseId(UnknownDatabaseId::new(
                *db_id,
                "get_database_meta_by_id".to_string(),
            )))
        })?;
        Ok(x)
    }

    pub fn txn_get_database_meta_by_id(
        &self,
        db_id: &u64,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<Option<SeqV<DatabaseMeta>>> {
        let txn_databases = txn_tree.key_space::<Databases>();
        let x = txn_databases.get(db_id)?;

        Ok(x)
    }

    pub fn get_database_meta_ver(&self) -> MetaResult<Option<u64>> {
        let sequences = self.sequences();
        let res = sequences.get(&SEQ_DATABASE_META_ID.to_string())?;
        Ok(res.map(|x| x.0))
    }

    // TODO(xp): need a better name.
    pub fn get_table_meta_by_id(&self, tid: &u64) -> MetaResult<Option<SeqV<TableMeta>>> {
        let x = self.tables().get(tid)?;
        Ok(x)
    }

    pub fn txn_get_table_meta_by_id(
        &self,
        tid: &u64,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<Option<SeqV<TableMeta>>> {
        let txn_table = txn_tree.key_space::<Tables>();
        let x = txn_table.get(tid)?;

        Ok(x)
    }

    pub async fn upsert_table(
        &self,
        table_id: u64,
        tbl: TableMeta,
        seq: &MatchSeq,
    ) -> Result<Option<SeqV<TableMeta>>, MetaError> {
        let tables = self.tables();
        let (_prev, result) = self
            .sub_tree_upsert(tables, &table_id, seq, Operation::Update(tbl), None)
            .await?;
        self.incr_seq(SEQ_DATABASE_META_ID).await?; // need this?
        Ok(result)
    }

    pub fn unexpired_opt<V: Debug>(seq_value: Option<SeqV<V>>) -> Option<SeqV<V>> {
        seq_value.and_then(Self::unexpired)
    }

    pub fn unexpired<V: Debug>(seq_value: SeqV<V>) -> Option<SeqV<V>> {
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn lookup_table_id(
        &self,
        db_id: u64,
        name: &str,
    ) -> Result<Option<SeqV<TableLookupValue>>, MetaError> {
        match self.table_lookup().get(
            &(TableLookupKey {
                database_id: db_id,
                table_name: name.to_string(),
            }),
        ) {
            Ok(e) => Ok(e),
            Err(e) => Err(e.into()),
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

    pub fn database_lookup(&self) -> AsKeySpace<DatabaseLookup> {
        self.sm_tree.key_space()
    }

    pub fn tables(&self) -> AsKeySpace<Tables> {
        self.sm_tree.key_space()
    }

    pub fn table_lookup(&self) -> AsKeySpace<TableLookup> {
        self.sm_tree.key_space()
    }
}
