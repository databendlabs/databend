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
use common_meta_sled_store::AsKeySpace;
use common_meta_sled_store::AsTxnKeySpace;
use common_meta_sled_store::SledKeySpace;
use common_meta_sled_store::SledTree;
use common_meta_sled_store::Store;
use common_meta_sled_store::TransactionSledTree;
use common_meta_types::convert_seqv_to_pb;
use common_meta_types::error_context::WithContext;
use common_meta_types::txn_condition;
use common_meta_types::txn_op;
use common_meta_types::txn_op_response;
use common_meta_types::AppError;
use common_meta_types::AppliedState;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::ConditionResult;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateShareReq;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseMeta;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropShareReq;
use common_meta_types::DropTableReq;
use common_meta_types::KVMeta;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::MetaResult;
use common_meta_types::MetaStorageError;
use common_meta_types::MetaStorageResult;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::RenameTableReq;
use common_meta_types::SeqV;
use common_meta_types::ShareInfo;
use common_meta_types::TableAlreadyExists;
use common_meta_types::TableMeta;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnDeleteResponse;
use common_meta_types::TxnGetRequest;
use common_meta_types::TxnGetResponse;
use common_meta_types::TxnOp;
use common_meta_types::TxnOpResponse;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnPutResponse;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UnknownDatabase;
use common_meta_types::UnknownDatabaseId;
use common_meta_types::UnknownShare;
use common_meta_types::UnknownTable;
use common_meta_types::UnknownTableId;
use common_tracing::tracing;
use openraft::raft::Entry;
use openraft::raft::EntryPayload;
use serde::Deserialize;
use serde::Serialize;

use crate::config::RaftConfig;
use crate::sled_key_spaces::ClientLastResps;
use crate::sled_key_spaces::DatabaseLookup;
use crate::sled_key_spaces::Databases;
use crate::sled_key_spaces::GenericKV;
use crate::sled_key_spaces::Nodes;
use crate::sled_key_spaces::Sequences;
use crate::sled_key_spaces::ShareLookup;
use crate::sled_key_spaces::Shares;
use crate::sled_key_spaces::StateMachineMeta;
use crate::sled_key_spaces::TableLookup;
use crate::sled_key_spaces::Tables;
use crate::state_machine::share_lookup::ShareLookupKey;
use crate::state_machine::share_lookup::ShareLookupValue;
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
/// seq number key to generate share id
const SEQ_SHARE_ID: &str = "share_id";

/// sled db tree name for nodes
// const TREE_NODES: &str = "nodes";
// const TREE_META: &str = "meta";
const TREE_STATE_MACHINE: &str = "state_machine";

/// StateMachine subscriber trait
pub trait StateMachineSubscriber: Debug + Sync + Send {
    fn kv_changed(&self, key: &str, prev: Option<SeqV>, current: Option<SeqV>);
}

/// The state machine of the `MemStore`.
/// It includes user data and two raft-related informations:
/// `last_applied_logs` and `client_serial_responses` to achieve idempotence.
#[derive(Debug)]
pub struct StateMachine {
    /// The internal sled::Tree to store everything about a state machine:
    /// - Store initialization state and last applied in keyspace `StateMachineMeta`.
    /// - Every other state is store in its own keyspace such as `Nodes`.
    pub sm_tree: SledTree,

    /// subscriber of statemachine data
    pub subscriber: Option<Box<dyn StateMachineSubscriber>>,
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

        let sm = StateMachine {
            sm_tree,
            subscriber: None,
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

    pub fn set_subscriber(&mut self, subscriber: Box<dyn StateMachineSubscriber>) {
        self.subscriber = Some(subscriber);
    }

    /// Create a snapshot.
    ///
    /// Returns:
    /// - all key values in state machine;
    /// - the last applied log id
    /// - and a snapshot id that uniquely identifies this snapshot.
    pub fn build_snapshot(
        &self,
    ) -> std::result::Result<(SerializableSnapshot, LogId, String), MetaStorageError> {
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

        let view = self.sm_tree.tree.iter();

        let mut kvs = Vec::new();
        for rkv in view {
            let (k, v) = rkv.context(|| "taking snapshot")?;
            kvs.push(vec![k.to_vec(), v.to_vec()]);
        }
        let snap = SerializableSnapshot { kvs };

        Ok((snap, last_applied, snapshot_id))
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
        req: &CreateDatabaseReq,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let tenant = &req.tenant;
        let name = &req.db_name;
        let meta = &req.meta;

        let db_id = self.txn_incr_seq(SEQ_DATABASE_ID, txn_tree)?;

        let db_lookup_tree = txn_tree.key_space::<DatabaseLookup>();
        let db_key = DatabaseLookupKey::new(tenant.to_string(), name.to_string());
        let (prev, result) = self.txn_sub_tree_upsert(
            &db_lookup_tree,
            &db_key,
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
        req: &DropDatabaseReq,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let tenant = &req.tenant;
        let name = &req.db_name;
        let dbs = txn_tree.key_space::<DatabaseLookup>();

        let db_key = DatabaseLookupKey::new(tenant.to_string(), name.to_string());
        let (prev, result) =
            self.txn_sub_tree_upsert(&dbs, &db_key, &MatchSeq::Any, Operation::Delete, None)?;

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
        req: &CreateTableReq,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let db_id = self.txn_get_database_id(&req.tenant, &req.db_name, txn_tree)?;

        let (table_id, prev, result) =
            self.txn_create_table(txn_tree, db_id, None, &req.table_name, &req.table_meta)?;
        let table_id = table_id.unwrap();

        if prev.is_some() {
            return Ok(AppliedState::TableMeta(Change::nochange_with_id(
                table_id, prev,
            )));
        }
        if result.is_some() {
            self.txn_incr_seq(SEQ_DATABASE_META_ID, txn_tree)?;
        }

        Ok(AppliedState::TableMeta(Change::new_with_id(
            table_id, prev, result,
        )))
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_drop_table_cmd(
        &self,
        req: &DropTableReq,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let db_id = self.txn_get_database_id(&req.tenant, &req.db_name, txn_tree)?;

        let (table_id, prev, result) = self.txn_drop_table(txn_tree, db_id, &req.table_name)?;
        if prev.is_none() {
            return Ok(Change::<TableMeta>::new(None, None).into());
        }
        if result.is_none() {
            self.txn_incr_seq(SEQ_DATABASE_META_ID, txn_tree)?;
        }

        Ok(Change::new_with_id(table_id.unwrap(), prev, result).into())
    }

    fn apply_rename_table_cmd(
        &self,
        req: &RenameTableReq,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let db_id = self.txn_get_database_id(&req.tenant, &req.db_name, txn_tree)?;
        let (table_id, prev, result) = self.txn_drop_table(txn_tree, db_id, &req.table_name)?;
        if prev.is_none() {
            return Err(MetaStorageError::AppError(AppError::UnknownTable(
                UnknownTable::new(&req.table_name, "apply_rename_table_cmd"),
            )));
        }
        assert!(result.is_none());

        let table_meta = &prev.as_ref().unwrap().data;
        let db_id = self.txn_get_database_id(&req.tenant, &req.new_db_name, txn_tree)?;
        let (new_table_id, new_prev, new_result) =
            self.txn_create_table(txn_tree, db_id, table_id, &req.new_table_name, table_meta)?;
        if new_prev.is_some() {
            return Err(MetaStorageError::AppError(AppError::TableAlreadyExists(
                TableAlreadyExists::new(&req.new_table_name, "apply_rename_table_cmd"),
            )));
        }
        assert!(new_result.is_some());

        self.txn_incr_seq(SEQ_DATABASE_META_ID, txn_tree)?;
        tracing::debug!("applied {}", req);

        Ok(AppliedState::TableMeta(Change::new_with_id(
            new_table_id.unwrap(),
            prev,
            new_result,
        )))
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
        let key_str = key.to_string();
        let (prev, result) = self.txn_sub_tree_upsert(
            &sub_tree,
            &key_str,
            seq,
            value_op.clone(),
            value_meta.clone(),
        )?;

        tracing::debug!("applied UpsertKV: {} {:?}", key, result);

        if let Some(subscriber) = &self.subscriber {
            subscriber.kv_changed(&key_str, prev.clone(), result.clone());
        }

        Ok(Change::new(prev, result).into())
    }

    pub fn return_value_condition_result(
        &self,
        expected: i32,
        target_value: &Vec<u8>,
        value: SeqV,
    ) -> bool {
        if expected == ConditionResult::Eq as i32 {
            return value.data == *target_value;
        }
        if expected == ConditionResult::Gt as i32 {
            return value.data > *target_value;
        }
        if expected == ConditionResult::Lt as i32 {
            return value.data < *target_value;
        }
        if expected == ConditionResult::Ne as i32 {
            return value.data != *target_value;
        }
        if expected == ConditionResult::Ge as i32 {
            return value.data >= *target_value;
        }
        if expected == ConditionResult::Le as i32 {
            return value.data <= *target_value;
        }
        false
    }

    pub fn return_seq_condition_result(
        &self,
        expected: i32,
        target_seq: &u64,
        value: SeqV,
    ) -> bool {
        if expected == ConditionResult::Eq as i32 {
            return value.seq == *target_seq;
        }
        if expected == ConditionResult::Gt as i32 {
            return value.seq > *target_seq;
        }
        if expected == ConditionResult::Lt as i32 {
            return value.seq < *target_seq;
        }
        if expected == ConditionResult::Ne as i32 {
            return value.seq != *target_seq;
        }
        if expected == ConditionResult::Ge as i32 {
            return value.seq >= *target_seq;
        }
        if expected == ConditionResult::Le as i32 {
            return value.seq <= *target_seq;
        }
        false
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn txn_execute_one_condition(
        &self,
        txn_tree: &TransactionSledTree,
        cond: &TxnCondition,
    ) -> bool {
        let key = cond.key.clone();

        let sub_tree = txn_tree.key_space::<GenericKV>();
        let sv = sub_tree.get(&key);
        if sv.is_err() {
            return false;
        }
        let sv = sv.unwrap();
        tracing::debug!("txn_execute_one_condition: {:?} {:?}", key, sv);

        if let Some(sv) = sv {
            if let Some(target_union) = &cond.target_union {
                match target_union {
                    txn_condition::TargetUnion::Seq(target_seq) => {
                        return self.return_seq_condition_result(cond.expected, target_seq, sv);
                    }
                    txn_condition::TargetUnion::Value(target_value) => {
                        return self.return_value_condition_result(cond.expected, target_value, sv);
                    }
                }
            };
        }

        false
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn txn_execute_condition(
        &self,
        txn_tree: &TransactionSledTree,
        condition: &Vec<TxnCondition>,
    ) -> bool {
        for cond in condition {
            if !self.txn_execute_one_condition(txn_tree, cond) {
                return false;
            }
        }

        true
    }

    fn txn_execute_get_operation(
        &self,
        txn_tree: &TransactionSledTree,
        get: &TxnGetRequest,
        resp: &mut TxnReply,
    ) {
        let key = get.key.clone();

        let sub_tree = txn_tree.key_space::<GenericKV>();
        let sv = sub_tree.get(&key);

        let get = match sv {
            Ok(v) => match v {
                Some(v) => TxnGetResponse {
                    key: get.key.clone(),
                    value: convert_seqv_to_pb(Some(v)),
                },
                None => TxnGetResponse {
                    key: get.key.clone(),
                    value: None,
                },
            },
            Err(_v) => TxnGetResponse {
                key: get.key.clone(),
                value: None,
            },
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Get(get)),
        });
    }

    fn txn_execute_put_operation(
        &self,
        txn_tree: &TransactionSledTree,
        put: &TxnPutRequest,
        resp: &mut TxnReply,
    ) {
        let key = put.key.clone();
        let sub_tree = txn_tree.key_space::<GenericKV>();
        let key_str = key.to_string();

        let result = self.txn_sub_tree_upsert(
            &sub_tree,
            &key_str,
            &MatchSeq::Any,
            Operation::Update(put.value.clone()),
            None,
        );

        let put_resp = match result {
            Ok((prev, result)) => {
                tracing::debug!("applied txn_execute_put_operation: {} {:?}", key, result);

                TxnPutResponse {
                    key: put.key.clone(),
                    prev_value: if put.prev_kv {
                        convert_seqv_to_pb(prev)
                    } else {
                        None
                    },
                }
            }
            Err(err) => {
                tracing::debug!("txn_execute_put_operation fail: {} {:?}", key, err);
                TxnPutResponse {
                    key: put.key.clone(),
                    prev_value: None,
                }
            }
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Put(put_resp)),
        });
    }

    fn txn_execute_delete_operation(
        &self,
        txn_tree: &TransactionSledTree,
        delete: &TxnDeleteRequest,
        resp: &mut TxnReply,
    ) {
        let key = delete.key.clone();
        let sub_tree = txn_tree.key_space::<GenericKV>();
        let key_str = key.to_string();

        let del_resp = if let Ok(result) =
            self.txn_sub_tree_upsert(&sub_tree, &key_str, &MatchSeq::Any, Operation::Delete, None)
        {
            let (prev, result) = result;
            tracing::debug!("applied txn_execute_delete_operation: {} {:?}", key, result);

            TxnDeleteResponse {
                key: delete.key.clone(),
                success: true,
                prev_value: if delete.prev_kv {
                    convert_seqv_to_pb(prev)
                } else {
                    None
                },
            }
        } else {
            TxnDeleteResponse {
                key: delete.key.clone(),
                success: false,
                prev_value: None,
            }
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Delete(del_resp)),
        });
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn txn_execute_operation(
        &self,
        txn_tree: &TransactionSledTree,
        op: &TxnOp,
        resp: &mut TxnReply,
    ) {
        match &op.request {
            Some(txn_op::Request::Get(get)) => {
                self.txn_execute_get_operation(txn_tree, get, resp);
            }
            Some(txn_op::Request::Put(put)) => {
                self.txn_execute_put_operation(txn_tree, put, resp);
            }
            Some(txn_op::Request::Delete(delete)) => {
                self.txn_execute_delete_operation(txn_tree, delete, resp);
            }
            None => {}
        }
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_txn_cmd(
        &self,
        req: &TxnRequest,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let condition = &req.condition;

        let ops: &Vec<TxnOp>;
        let success = if self.txn_execute_condition(txn_tree, condition) {
            ops = &req.if_then;
            true
        } else {
            ops = &req.else_then;
            false
        };

        let mut resp: TxnReply = TxnReply {
            success,
            responses: vec![],
        };

        for op in ops {
            self.txn_execute_operation(txn_tree, op, &mut resp);
        }

        Ok(AppliedState::TxnReply(resp))
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

            Cmd::CreateDatabase(req) => self.apply_create_database_cmd(req, txn_tree),

            Cmd::DropDatabase(req) => self.apply_drop_database_cmd(req, txn_tree),

            Cmd::CreateTable(req) => self.apply_create_table_cmd(req, txn_tree),

            Cmd::DropTable(req) => self.apply_drop_table_cmd(req, txn_tree),

            Cmd::RenameTable(req) => self.apply_rename_table_cmd(req, txn_tree),

            Cmd::CreateShare(req) => self.apply_create_share_cmd(req, txn_tree),

            Cmd::DropShare(req) => self.apply_drop_share_cmd(req, txn_tree),

            Cmd::UpsertKV {
                key,
                seq,
                value: value_op,
                value_meta,
            } => self.apply_update_kv_cmd(key, seq, value_op, value_meta, txn_tree),

            Cmd::UpsertTableOptions(ref req) => self.apply_upsert_table_options_cmd(req, txn_tree),

            Cmd::Transaction(txn) => self.apply_txn_cmd(txn, txn_tree),
        }
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

    #[allow(clippy::type_complexity)]
    fn txn_create_table(
        &self,
        txn_tree: &TransactionSledTree,
        db_id: u64,
        table_id: Option<u64>,
        table_name: &str,
        table_meta: &TableMeta,
    ) -> MetaStorageResult<(
        Option<u64>,
        Option<SeqV<TableMeta>>,
        Option<SeqV<TableMeta>>,
    )> {
        let lookup_key = TableLookupKey {
            database_id: db_id,
            table_name: table_name.to_string(),
        };

        let table_lookup_tree = txn_tree.key_space::<TableLookup>();
        let seq_table_id = table_lookup_tree.get(&lookup_key)?;

        if let Some(u) = seq_table_id {
            let table_id = u.data.0;

            let prev = self.txn_get_table_meta_by_id(&table_id, txn_tree)?;

            return Ok((Some(table_id), prev, None));
        }

        let table_meta = table_meta.clone();
        let table_id = if let Some(table_id) = table_id {
            table_id
        } else {
            self.txn_incr_seq(SEQ_TABLE_ID, txn_tree)?
        };

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
        Ok((Some(table_id), prev, result))
    }

    #[allow(clippy::type_complexity)]
    fn txn_drop_table(
        &self,
        txn_tree: &TransactionSledTree,
        db_id: u64,
        table_name: &str,
    ) -> MetaStorageResult<(
        Option<u64>,
        Option<SeqV<TableMeta>>,
        Option<SeqV<TableMeta>>,
    )> {
        let lookup_key = TableLookupKey {
            database_id: db_id,
            table_name: table_name.to_string(),
        };

        let table_lookup_tree = txn_tree.key_space::<TableLookup>();
        let seq_table_id = table_lookup_tree.get(&lookup_key)?;

        if seq_table_id.is_none() {
            return Ok((None, None, None));
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
        tracing::debug!("applied drop Table: {} {:?}", table_name, result);
        Ok((Some(table_id), prev, result))
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

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_create_share_cmd(
        &self,
        req: &CreateShareReq,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let tenant = &req.tenant;
        let share_name = &req.share_name;
        let share_lookup_tree = txn_tree.key_space::<ShareLookup>();
        let share_tree = txn_tree.key_space::<Shares>();

        let share_lookup_key = ShareLookupKey::new(tenant.to_string(), share_name.to_string());

        let seq_share_id = share_lookup_tree.get(&share_lookup_key)?;

        if let Some(u) = seq_share_id {
            let share_id = u.data.0;
            let prev = share_tree.get(&share_id)?;
            return Ok(AppliedState::ShareInfo(Change::nochange_with_id(
                share_id, prev,
            )));
        }

        let share_id = self.txn_incr_seq(SEQ_SHARE_ID, txn_tree)?;

        let (prev, _) = self.txn_sub_tree_upsert(
            &share_lookup_tree,
            &share_lookup_key,
            &MatchSeq::Exact(0),
            Operation::Update(ShareLookupValue(share_id)),
            None,
        )?;

        // if it is just created
        if let Some(prev) = prev {
            let share_id = prev.data;
            let prev = share_tree.get(&share_id.0)?;
            if let Some(prev) = prev {
                return Ok(AppliedState::ShareInfo(Change::nochange_with_id(
                    share_id.0,
                    Some(prev),
                )));
            }
        }

        let share_info = ShareInfo::new(share_id, share_name);

        let (prev, result) = self.txn_sub_tree_upsert(
            &share_tree,
            &share_id,
            &MatchSeq::Exact(0),
            Operation::Update(share_info),
            None,
        )?;

        tracing::debug!(
            "applied create Share: {}, share_id: {}",
            share_name,
            share_id,
        );

        Ok(AppliedState::ShareInfo(Change::new_with_id(
            share_id, prev, result,
        )))
    }

    fn apply_drop_share_cmd(
        &self,
        req: &DropShareReq,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let share_lookup_tree = txn_tree.key_space::<ShareLookup>();

        let share_lookup_key =
            ShareLookupKey::new(req.tenant.to_string(), req.share_name.to_string());

        let (prev, result) = self.txn_sub_tree_upsert(
            &share_lookup_tree,
            &share_lookup_key,
            &MatchSeq::Any,
            Operation::Delete,
            None,
        )?;

        assert!(
            result.is_none(),
            "delete with MatchSeq::Any always succeeds"
        );

        // if it is just deleted
        if let Some(seq_share_id) = prev {
            let share_id = seq_share_id.data;
            let share_tree = txn_tree.key_space::<Shares>();

            let (prev_meta, result_meta) = self.txn_sub_tree_upsert(
                &share_tree,
                &share_id.0,
                &MatchSeq::Any,
                Operation::Delete,
                None,
            )?;

            tracing::debug!("applied {}, result = {:?}", &req, result);

            return Ok(AppliedState::ShareInfo(Change::new_with_id(
                share_id.0,
                prev_meta,
                result_meta,
            )));
        }

        // not exist

        tracing::debug!("applied drop Share: {} {:?}", req.share_name, result);
        Ok(AppliedState::ShareInfo(Change::new(None, None)))
    }

    pub fn get_share_id(&self, tenant: &str, share_name: &str) -> MetaStorageResult<u64> {
        let seq_share_id = self
            .share_lookup()
            .get(&(ShareLookupKey::new(tenant.to_string(), share_name.to_string())))?
            .ok_or_else(|| AppError::from(UnknownShare::new(share_name, "get_share_id")))?;

        Ok(seq_share_id.data.0)
    }

    pub fn get_share_info_by_id(&self, share_id: &u64) -> MetaStorageResult<SeqV<ShareInfo>> {
        let x = self.shares().get(share_id)?.ok_or_else(|| {
            MetaStorageError::AppError(AppError::UnknownTableId(UnknownTableId::new(
                *share_id,
                "get_share_info_by_id".to_string(),
            )))
        })?;
        Ok(x)
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

    pub fn shares(&self) -> AsKeySpace<Shares> {
        self.sm_tree.key_space()
    }

    pub fn share_lookup(&self) -> AsKeySpace<ShareLookup> {
        self.sm_tree.key_space()
    }
}
