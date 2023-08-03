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
use std::convert::TryInto;
use std::fmt::Debug;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::openraft::MessageSummary;
use common_meta_sled_store::AsKeySpace;
use common_meta_sled_store::SledKeySpace;
use common_meta_sled_store::SledTree;
use common_meta_sled_store::Store;
use common_meta_sled_store::TransactionSledTree;
use common_meta_stoerr::MetaStorageError;
use common_meta_types::protobuf as pb;
use common_meta_types::txn_condition;
use common_meta_types::txn_op;
use common_meta_types::txn_op_response;
use common_meta_types::AppliedState;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::ConditionResult;
use common_meta_types::Entry;
use common_meta_types::EntryPayload;
use common_meta_types::KVMeta;
use common_meta_types::LogId;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::StoredMembership;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteByPrefixRequest;
use common_meta_types::TxnDeleteByPrefixResponse;
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
use common_meta_types::UpsertKV;
use common_meta_types::With;
use log::as_debug;
use log::as_display;
use log::debug;
use log::error;
use log::info;
use log::warn;
use num::FromPrimitive;
use serde::Deserialize;
use serde::Serialize;

use crate::config::RaftConfig;
use crate::key_spaces::ClientLastResps;
use crate::key_spaces::Expire;
use crate::key_spaces::GenericKV;
use crate::key_spaces::Nodes;
use crate::key_spaces::Sequences;
use crate::key_spaces::StateMachineMeta;
use crate::state_machine::ClientLastRespValue;
use crate::state_machine::ExpireKey;
use crate::state_machine::ExpireValue;
use crate::state_machine::MetaSnapshotId;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaKey::Initialized;
use crate::state_machine::StateMachineMetaKey::LastApplied;
use crate::state_machine::StateMachineMetaKey::LastMembership;
use crate::state_machine::StateMachineMetaValue;

/// sled db tree name for nodes
// const TREE_NODES: &str = "nodes";
// const TREE_META: &str = "meta";
const TREE_STATE_MACHINE: &str = "state_machine";

/// StateMachine subscriber trait
pub trait StateMachineSubscriber: Debug + Sync + Send {
    fn kv_changed(&self, change: Change<Vec<u8>, String>);
}

/// The state machine of the `MemStore`.
/// It includes user data and two raft-related information:
/// `last_applied_logs` and `client_serial_responses` to achieve idempotence.
#[derive(Debug)]
pub struct StateMachine {
    /// The internal sled::Tree to store everything about a state machine:
    /// - Store initialization state and last applied in keyspace `StateMachineMeta`.
    /// - Every other state is store in its own keyspace such as `Nodes`.
    pub sm_tree: SledTree,

    blocking_config: BlockingConfig,

    /// subscriber of state machine data
    pub subscriber: Option<Box<dyn StateMachineSubscriber>>,
}

/// A key-value pair in a snapshot is a vec of two `Vec<u8>`.
pub type SnapshotKeyValue = Vec<Vec<u8>>;
type DeleteByPrefixKeyMap = BTreeMap<TxnDeleteByPrefixRequest, Vec<(String, SeqV)>>;

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

/// Configuration of what operation to block for testing purpose.
#[derive(Debug, Clone, Default)]
pub struct BlockingConfig {
    pub dump_snapshot: Duration,
    pub serde_snapshot: Duration,
}

impl StateMachine {
    /// Return a Arc of the blocking config. It is only used for testing.
    pub fn blocking_config_mut(&mut self) -> &mut BlockingConfig {
        &mut self.blocking_config
    }

    pub fn blocking_config(&self) -> &BlockingConfig {
        &self.blocking_config
    }

    pub fn tree_name(config: &RaftConfig, sm_id: u64) -> String {
        config.tree_name(format!("{}/{}", TREE_STATE_MACHINE, sm_id))
    }

    #[minitrace::trace]
    pub fn clean(config: &RaftConfig, sm_id: u64) -> Result<(), MetaStorageError> {
        let tree_name = StateMachine::tree_name(config, sm_id);
        debug!("cleaning tree: {}", &tree_name);

        let db = get_sled_db();

        // it blocks and slow
        db.drop_tree(tree_name)?;

        Ok(())
    }

    #[minitrace::trace]
    pub async fn open(config: &RaftConfig, sm_id: u64) -> Result<StateMachine, MetaStorageError> {
        let db = get_sled_db();

        let tree_name = StateMachine::tree_name(config, sm_id);
        debug!("opening tree: {}", &tree_name);

        let sm_tree = SledTree::open(&db, &tree_name, config.is_sync())?;

        let sm = StateMachine {
            sm_tree,
            blocking_config: BlockingConfig::default(),
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
    ) -> Result<
        (
            SerializableSnapshot,
            Option<LogId>,
            StoredMembership,
            MetaSnapshotId,
        ),
        MetaStorageError,
    > {
        let last_applied = self.get_last_applied()?;
        let last_membership = self.get_membership()?.unwrap_or_default();

        let snapshot_idx = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot_id = MetaSnapshotId::new(last_applied, snapshot_idx);

        let view = self.sm_tree.tree.iter();

        let mut kvs = Vec::new();
        for rkv in view {
            let (k, v) = rkv?;
            kvs.push(vec![k.to_vec(), v.to_vec()]);
        }
        let snap = SerializableSnapshot { kvs };

        if cfg!(debug_assertions) {
            let sl = self.blocking_config().dump_snapshot;
            if !sl.is_zero() {
                warn!("start    build snapshot sleep 1000s");
                std::thread::sleep(sl);
                warn!("finished build snapshot sleep 1000s");
            }
        }

        Ok((snap, last_applied, last_membership, snapshot_id))
    }

    fn scan_prefix_if_needed(
        &self,
        entry: &Entry,
    ) -> Result<Option<(DeleteByPrefixKeyMap, DeleteByPrefixKeyMap)>, MetaStorageError> {
        match entry.payload {
            EntryPayload::Normal(ref data) => match &data.cmd {
                Cmd::Transaction(txn) => {
                    let kvs = self.kvs();
                    let mut if_map = BTreeMap::new();
                    let mut else_map = BTreeMap::new();
                    for op in txn.if_then.iter() {
                        if let Some(txn_op::Request::DeleteByPrefix(delete_by_prefix)) = &op.request
                        {
                            if_map.insert(
                                delete_by_prefix.clone(),
                                kvs.scan_prefix(&delete_by_prefix.prefix)?,
                            );
                        }
                    }
                    for op in txn.else_then.iter() {
                        if let Some(txn_op::Request::DeleteByPrefix(delete_by_prefix)) = &op.request
                        {
                            else_map.insert(
                                delete_by_prefix.clone(),
                                kvs.scan_prefix(&delete_by_prefix.prefix)?,
                            );
                        }
                    }
                    Ok(Some((if_map, else_map)))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// Apply an log entry to state machine.
    ///
    /// If a duplicated log entry is detected by checking data.txid, no update
    /// will be made and the previous resp is returned. In this way a client is able to re-send a
    /// command safely in case of network failure etc.
    #[minitrace::trace]
    pub async fn apply(&self, entry: &Entry) -> Result<AppliedState, MetaStorageError> {
        info!("apply: summary: {}", entry.summary(),);
        debug!(log_id = as_display!(&entry.log_id); "sled tx start: {:?}", entry);

        let log_id = &entry.log_id;
        let log_time_ms = Self::get_log_time(entry);

        let expired = self.list_expired_kvs(log_time_ms)?;
        debug!("expired keys: {:?}", expired);

        let kv_pairs = self.scan_prefix_if_needed(entry)?;

        let result = self.sm_tree.txn(true, move |mut txn_tree| {
            self.clean_expired_kvs(&mut txn_tree, &expired)?;

            let txn_sm_meta = txn_tree.key_space::<StateMachineMeta>();
            txn_sm_meta.insert(&LastApplied, &StateMachineMetaValue::LogId(*log_id))?;

            match entry.payload {
                EntryPayload::Blank => {
                    info!("apply: blank");
                }
                EntryPayload::Normal(ref data) => {
                    info!("apply: {}", data);
                    if let Some(ref txid) = data.txid {
                        let (serial, resp) =
                            self.txn_get_client_last_resp(&txid.client, &txn_tree)?;
                        if serial == txid.serial {
                            return Ok((Some(resp), txn_tree.changes));
                        }
                    }

                    let res =
                        self.apply_cmd(&data.cmd, &mut txn_tree, kv_pairs.as_ref(), log_time_ms);
                    if let Ok(ok) = &res {
                        info!("apply_result: summary: {}; res ok: {}", entry.summary(), ok);
                    }
                    if let Err(err) = &res {
                        info!(
                            "apply_result: summary: {}; res err: {:?}",
                            entry.summary(),
                            err
                        );
                    }

                    let applied_state = res?;

                    if let Some(ref txid) = data.txid {
                        self.txn_client_last_resp_update(
                            &txid.client,
                            (txid.serial, applied_state.clone()),
                            &txn_tree,
                        )?;
                    }
                    return Ok((Some(applied_state), txn_tree.changes));
                }
                EntryPayload::Membership(ref mem) => {
                    info!("apply: membership: {:?}", mem);
                    txn_sm_meta.insert(
                        &LastMembership,
                        &StateMachineMetaValue::Membership(StoredMembership::new(
                            Some(*log_id),
                            mem.clone(),
                        )),
                    )?;
                    return Ok((Some(AppliedState::None), txn_tree.changes));
                }
            };

            Ok((None, txn_tree.changes))
        });

        let (opt_applied_state, changes) = result?;

        debug!("sled tx done: {:?}", entry);

        let applied_state = opt_applied_state.unwrap_or(AppliedState::None);

        // Send queued change events to subscriber
        if let Some(subscriber) = &self.subscriber {
            for event in changes {
                subscriber.kv_changed(event);
            }
        }

        Ok(applied_state)
    }

    /// Retrieve the proposing time from a raft-log.
    ///
    /// Only `Normal` log has a time embedded.
    #[minitrace::trace]
    fn get_log_time(entry: &Entry) -> u64 {
        match &entry.payload {
            EntryPayload::Normal(data) => match data.time_ms {
                None => {
                    error!(
                        "log has no time: {}, treat every record with non-none `expire` as timed out",
                        entry.summary()
                    );
                    0
                }
                Some(x) => {
                    let t = SystemTime::UNIX_EPOCH + Duration::from_millis(x);
                    info!("apply: raft-log time: {:?}", t);
                    x
                }
            },
            _ => 0,
        }
    }

    #[minitrace::trace]
    fn apply_add_node_cmd(
        &self,
        node_id: &u64,
        node: &Node,
        overriding: bool,
        txn_tree: &TransactionSledTree,
    ) -> Result<AppliedState, MetaStorageError> {
        let sm_nodes = txn_tree.key_space::<Nodes>();

        let prev = sm_nodes.get(node_id)?;

        if prev.is_none() {
            sm_nodes.insert(node_id, node)?;
            info!("applied AddNode(non-overriding): {}={:?}", node_id, node);
            return Ok((prev, Some(node.clone())).into());
        }

        if overriding {
            sm_nodes.insert(node_id, node)?;
            info!("applied AddNode(overriding): {}={:?}", node_id, node);
            Ok((prev, Some(node.clone())).into())
        } else {
            Ok((prev.clone(), prev).into())
        }
    }

    #[minitrace::trace]
    fn apply_remove_node_cmd(
        &self,
        node_id: &u64,
        txn_tree: &TransactionSledTree,
    ) -> Result<AppliedState, MetaStorageError> {
        let sm_nodes = txn_tree.key_space::<Nodes>();

        let prev = sm_nodes.get(node_id)?;

        if prev.is_some() {
            info!("applied RemoveNode: {}={:?}", node_id, prev);
            sm_nodes.remove(node_id)?;
        }
        Ok((prev, None).into())
    }

    #[minitrace::trace]
    fn apply_update_kv_cmd(
        &self,
        upsert_kv: &UpsertKV,
        txn_tree: &mut TransactionSledTree,
        log_time_ms: u64,
    ) -> Result<AppliedState, MetaStorageError> {
        debug!(upsert_kv = as_debug!(upsert_kv); "apply_update_kv_cmd");

        let (expired, prev, result) = Self::txn_upsert_kv(txn_tree, upsert_kv, log_time_ms)?;

        debug!("applied UpsertKV: {:?} {:?}", upsert_kv, result);

        if expired.is_some() {
            txn_tree.push_change(&upsert_kv.key, expired, None);
        }
        txn_tree.push_change(&upsert_kv.key, prev.clone(), result.clone());

        Ok(Change::new(prev, result).into())
    }

    fn return_value_condition_result(
        &self,
        expected: i32,
        target_value: &Vec<u8>,
        value: &SeqV,
    ) -> bool {
        match FromPrimitive::from_i32(expected) {
            Some(ConditionResult::Eq) => value.data == *target_value,
            Some(ConditionResult::Gt) => value.data > *target_value,
            Some(ConditionResult::Lt) => value.data < *target_value,
            Some(ConditionResult::Ne) => value.data != *target_value,
            Some(ConditionResult::Ge) => value.data >= *target_value,
            Some(ConditionResult::Le) => value.data <= *target_value,
            _ => false,
        }
    }

    pub fn return_seq_condition_result(
        &self,
        expected: i32,
        target_seq: &u64,
        value: &SeqV,
    ) -> bool {
        match FromPrimitive::from_i32(expected) {
            Some(ConditionResult::Eq) => value.seq == *target_seq,
            Some(ConditionResult::Gt) => value.seq > *target_seq,
            Some(ConditionResult::Lt) => value.seq < *target_seq,
            Some(ConditionResult::Ne) => value.seq != *target_seq,
            Some(ConditionResult::Ge) => value.seq >= *target_seq,
            Some(ConditionResult::Le) => value.seq <= *target_seq,
            _ => false,
        }
    }

    #[minitrace::trace]
    fn txn_execute_one_condition(
        &self,
        txn_tree: &TransactionSledTree,
        cond: &TxnCondition,
    ) -> Result<bool, MetaStorageError> {
        debug!(cond = as_display!(cond); "txn_execute_one_condition");

        let key = cond.key.clone();

        let sub_tree = txn_tree.key_space::<GenericKV>();
        let sv = sub_tree.get(&key)?;

        debug!("txn_execute_one_condition: {:?} {:?}", key, sv);

        if let Some(target) = &cond.target {
            match target {
                txn_condition::Target::Seq(target_seq) => {
                    return Ok(self.return_seq_condition_result(
                        cond.expected,
                        target_seq,
                        // seq is 0 if the record does not exist.
                        &sv.unwrap_or_default(),
                    ));
                }
                txn_condition::Target::Value(target_value) => {
                    if let Some(sv) = sv {
                        return Ok(self.return_value_condition_result(
                            cond.expected,
                            target_value,
                            &sv,
                        ));
                    } else {
                        return Ok(false);
                    }
                }
            }
        };

        Ok(false)
    }

    #[minitrace::trace]
    fn txn_execute_condition(
        &self,
        txn_tree: &TransactionSledTree,
        condition: &Vec<TxnCondition>,
    ) -> Result<bool, MetaStorageError> {
        for cond in condition {
            debug!(condition = as_display!(cond); "txn_execute_condition");

            if !self.txn_execute_one_condition(txn_tree, cond)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn txn_execute_get_operation(
        &self,
        txn_tree: &TransactionSledTree,
        get: &TxnGetRequest,
        resp: &mut TxnReply,
    ) -> Result<(), MetaStorageError> {
        let sub_tree = txn_tree.key_space::<GenericKV>();
        let sv = sub_tree.get(&get.key)?;
        let value = sv.map(to_pb_seq_v);
        let get_resp = TxnGetResponse {
            key: get.key.clone(),
            value,
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Get(get_resp)),
        });

        Ok(())
    }

    fn txn_execute_put_operation(
        &self,
        txn_tree: &mut TransactionSledTree,
        put: &TxnPutRequest,
        resp: &mut TxnReply,
        log_time_ms: u64,
    ) -> Result<(), MetaStorageError> {
        let (expired, prev, result) = Self::txn_upsert_kv(
            txn_tree,
            &UpsertKV::update(&put.key, &put.value).with(KVMeta {
                expire_at: put.expire_at,
            }),
            log_time_ms,
        )?;

        if expired.is_some() {
            txn_tree.push_change(&put.key, expired, None);
        }
        txn_tree.push_change(&put.key, prev.clone(), result);

        let put_resp = TxnPutResponse {
            key: put.key.clone(),
            prev_value: if put.prev_value {
                prev.map(to_pb_seq_v)
            } else {
                None
            },
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Put(put_resp)),
        });

        Ok(())
    }

    fn txn_execute_delete_operation(
        &self,
        txn_tree: &mut TransactionSledTree,
        delete: &TxnDeleteRequest,
        resp: &mut TxnReply,
        log_time_ms: u64,
    ) -> Result<(), MetaStorageError> {
        let upsert = UpsertKV::delete(&delete.key);

        // If `delete.match_seq` is `Some`, only delete the record with the exact `seq`.
        let upsert = if let Some(seq) = delete.match_seq {
            upsert.with(MatchSeq::Exact(seq))
        } else {
            upsert
        };

        let (expired, prev, result) = Self::txn_upsert_kv(txn_tree, &upsert, log_time_ms)?;
        let is_deleted = prev.is_some() && result.is_none();

        if expired.is_some() {
            txn_tree.push_change(&delete.key, expired, None);
        }
        txn_tree.push_change(&delete.key, prev.clone(), result);

        let del_resp = TxnDeleteResponse {
            key: delete.key.clone(),
            success: is_deleted,
            prev_value: if delete.prev_value {
                prev.map(to_pb_seq_v)
            } else {
                None
            },
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Delete(del_resp)),
        });

        Ok(())
    }

    fn txn_execute_delete_by_prefix_operation(
        &self,
        txn_tree: &mut TransactionSledTree,
        delete_by_prefix: &TxnDeleteByPrefixRequest,
        kv_pairs: Option<&DeleteByPrefixKeyMap>,
        resp: &mut TxnReply,
        log_time_ms: u64,
    ) -> Result<(), MetaStorageError> {
        let mut count: u32 = 0;
        if let Some(kv_pairs) = kv_pairs {
            if let Some(kv_pairs) = kv_pairs.get(delete_by_prefix) {
                for (key, _seq) in kv_pairs.iter() {
                    let (expired, prev, res) =
                        Self::txn_upsert_kv(txn_tree, &UpsertKV::delete(key), log_time_ms)?;

                    count += 1;

                    if expired.is_some() {
                        txn_tree.push_change(key, expired, None);
                    }
                    txn_tree.push_change(key, prev, res);
                }
            }
        }

        let del_resp = TxnDeleteByPrefixResponse {
            prefix: delete_by_prefix.prefix.clone(),
            count,
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::DeleteByPrefix(del_resp)),
        });

        Ok(())
    }

    #[minitrace::trace]
    fn txn_execute_operation(
        &self,
        txn_tree: &mut TransactionSledTree,
        op: &TxnOp,
        kv_pairs: Option<&DeleteByPrefixKeyMap>,
        resp: &mut TxnReply,
        log_time_ms: u64,
    ) -> Result<(), MetaStorageError> {
        debug!(op = as_display!(op); "txn execute TxnOp");
        match &op.request {
            Some(txn_op::Request::Get(get)) => {
                self.txn_execute_get_operation(txn_tree, get, resp)?;
            }
            Some(txn_op::Request::Put(put)) => {
                self.txn_execute_put_operation(txn_tree, put, resp, log_time_ms)?;
            }
            Some(txn_op::Request::Delete(delete)) => {
                self.txn_execute_delete_operation(txn_tree, delete, resp, log_time_ms)?;
            }
            Some(txn_op::Request::DeleteByPrefix(delete_by_prefix)) => {
                self.txn_execute_delete_by_prefix_operation(
                    txn_tree,
                    delete_by_prefix,
                    kv_pairs,
                    resp,
                    log_time_ms,
                )?;
            }
            None => {}
        }

        Ok(())
    }

    #[minitrace::trace]
    fn apply_txn_cmd(
        &self,
        req: &TxnRequest,
        txn_tree: &mut TransactionSledTree,
        kv_pairs: Option<&(DeleteByPrefixKeyMap, DeleteByPrefixKeyMap)>,
        log_time_ms: u64,
    ) -> Result<AppliedState, MetaStorageError> {
        debug!(txn = as_display!(req); "apply txn cmd");

        let condition = &req.condition;

        let ops: &Vec<TxnOp>;
        let kv_op_pairs: Option<&DeleteByPrefixKeyMap>;
        let success = if self.txn_execute_condition(txn_tree, condition)? {
            ops = &req.if_then;
            kv_op_pairs = if let Some(kv_pairs) = kv_pairs {
                Some(&kv_pairs.0)
            } else {
                None
            };
            true
        } else {
            ops = &req.else_then;
            kv_op_pairs = if let Some(kv_pairs) = kv_pairs {
                Some(&kv_pairs.1)
            } else {
                None
            };
            false
        };

        let mut resp: TxnReply = TxnReply {
            success,
            error: "".to_string(),
            responses: vec![],
        };

        for op in ops {
            self.txn_execute_operation(txn_tree, op, kv_op_pairs, &mut resp, log_time_ms)?;
        }

        Ok(AppliedState::TxnReply(resp))
    }

    /// Apply a `Cmd` to state machine.
    ///
    /// Already applied log should be filtered out before passing into this function.
    /// This is the only entry to modify state machine.
    /// The `cmd` is always committed by raft before applying.
    #[minitrace::trace]
    pub fn apply_cmd(
        &self,
        cmd: &Cmd,
        txn_tree: &mut TransactionSledTree,
        kv_pairs: Option<&(DeleteByPrefixKeyMap, DeleteByPrefixKeyMap)>,
        log_time_ms: u64,
    ) -> Result<AppliedState, MetaStorageError> {
        info!("apply_cmd: {}", cmd);

        let now = Instant::now();

        let res = match cmd {
            Cmd::AddNode {
                ref node_id,
                ref node,
                overriding,
            } => self.apply_add_node_cmd(node_id, node, *overriding, txn_tree),

            Cmd::RemoveNode { ref node_id } => self.apply_remove_node_cmd(node_id, txn_tree),

            Cmd::UpsertKV(ref upsert_kv) => {
                self.apply_update_kv_cmd(upsert_kv, txn_tree, log_time_ms)
            }

            Cmd::Transaction(txn) => self.apply_txn_cmd(txn, txn_tree, kv_pairs, log_time_ms),
        };

        let elapsed = now.elapsed().as_micros();
        debug!("apply_cmd: elapsed: {}", elapsed);

        res
    }

    /// Before applying, list expired keys to clean.
    ///
    /// Apply is done in a sled-txn tree, which does not provide listing function.
    #[minitrace::trace]
    pub fn list_expired_kvs(
        &self,
        log_time_ms: u64,
    ) -> Result<Vec<(String, ExpireKey)>, MetaStorageError> {
        if log_time_ms == 0 {
            return Ok(vec![]);
        }

        let at_most = 32;
        let mut to_clean = Vec::with_capacity(at_most);

        info!("list_expired_kv, log_time_ts: {}", log_time_ms);

        let expires = self.sm_tree.key_space::<Expire>();

        let it = expires.range(..)?.take(at_most);
        for item_res in it {
            let item = item_res?;
            let k: ExpireKey = item.key()?;
            if log_time_ms > k.time_ms {
                let v: ExpireValue = item.value()?;
                to_clean.push((v.key, k))
            }
        }

        Ok(to_clean)
    }

    /// Remove expired key-values, and corresponding secondary expiration index record.
    ///
    /// This should be done inside a sled-transaction.
    #[minitrace::trace]
    fn clean_expired_kvs(
        &self,
        txn_tree: &mut TransactionSledTree,
        expired: &[(String, ExpireKey)],
    ) -> Result<(), MetaStorageError> {
        for (key, expire_key) in expired.iter() {
            let sv = txn_tree.key_space::<GenericKV>().get(key)?;

            if let Some(seq_v) = &sv {
                if expire_key.seq == seq_v.seq {
                    info!("clean expired: {}, {}", key, expire_key);

                    txn_tree.key_space::<GenericKV>().remove(key)?;
                    txn_tree.key_space::<Expire>().remove(expire_key)?;

                    txn_tree.push_change(key, sv, None);
                    continue;
                }
            }

            unreachable!(
                "trying to remove un-cleanable: {}, {}, kv-record: {:?}",
                key, expire_key, sv
            );
        }
        Ok(())
    }

    fn txn_incr_seq(key: &str, txn_tree: &TransactionSledTree) -> Result<u64, MetaStorageError> {
        let seqs = txn_tree.key_space::<Sequences>();

        let key = key.to_string();

        let curr = seqs.get(&key)?;
        let new_value = curr.unwrap_or_default() + 1;
        seqs.insert(&key, &new_value)?;

        debug!("txn_incr_seq: {}={}", key, new_value);

        Ok(new_value.0)
    }

    /// Execute an upsert-kv operation on a transactional sled tree.
    ///
    /// KV has two indexes:
    /// - The primary index: `key -> (seq, meta(expire_time), value)`,
    /// - and a secondary expiration index: `(expire_time, seq) -> key`.
    ///
    /// Thus upsert a kv record is done in two steps:
    /// update the primary index and optionally update the secondary index.
    ///
    /// It returns 3 SeqV:
    /// - `(None, None, x)`: upsert nonexistent key;
    /// - `(None, Some, x)`: upsert existent and non-expired key;
    /// - `(Some, None, x)`: upsert existent but expired key;
    #[allow(clippy::type_complexity)]
    fn txn_upsert_kv(
        txn_tree: &TransactionSledTree,
        upsert_kv: &UpsertKV,
        log_time_ms: u64,
    ) -> Result<(Option<SeqV>, Option<SeqV>, Option<SeqV>), MetaStorageError> {
        let (expired, prev, res) =
            Self::txn_upsert_kv_primary_index(txn_tree, upsert_kv, log_time_ms)?;

        let expires = txn_tree.key_space::<Expire>();

        if let Some(sv) = &expired {
            if let Some(m) = &sv.meta {
                if let Some(exp) = m.expire_at {
                    expires.remove(&ExpireKey::new(exp * 1000, sv.seq))?;
                }
            }
        }

        // No change, no need to update expiration index
        if prev == res {
            return Ok((expired, prev, res));
        }

        // Remove previous expiration index, add a new one.

        if let Some(sv) = &prev {
            if let Some(m) = &sv.meta {
                if let Some(exp) = m.expire_at {
                    expires.remove(&ExpireKey::new(exp * 1000, sv.seq))?;
                }
            }
        }

        if let Some(sv) = &res {
            if let Some(m) = &sv.meta {
                if let Some(exp) = m.expire_at {
                    let k = ExpireKey::new(exp * 1000, sv.seq);
                    let v = ExpireValue::new(&upsert_kv.key, 0);
                    expires.insert(&k, &v)?;
                }
            }
        }

        Ok((expired, prev, res))
    }

    /// It returns 3 SeqV:
    /// - The first one is `Some` if an existent record expired.
    /// - The second and the third represent the change that is made by the upsert operation.
    ///
    /// Only one of the first and second can be `Some`.
    #[allow(clippy::type_complexity)]
    fn txn_upsert_kv_primary_index(
        txn_tree: &TransactionSledTree,
        upsert_kv: &UpsertKV,
        log_time_ms: u64,
    ) -> Result<(Option<SeqV>, Option<SeqV>, Option<SeqV>), MetaStorageError> {
        let kvs = txn_tree.key_space::<GenericKV>();

        let prev = kvs.get(&upsert_kv.key)?;

        // If prev is timed out, treat it as a None. But still keep the original value for cleaning up it.
        let (expired, prev) = Self::expire_seq_v(prev, log_time_ms);

        if upsert_kv.seq.match_seq(&prev).is_err() {
            return Ok((expired, prev.clone(), prev));
        }

        let mut new_seq_v = match &upsert_kv.value {
            Operation::Update(v) => SeqV::with_meta(0, upsert_kv.value_meta.clone(), v.clone()),
            Operation::Delete => {
                kvs.remove(&upsert_kv.key)?;
                return Ok((expired, prev, None));
            }
            Operation::AsIs => match prev {
                None => return Ok((expired, prev, None)),
                Some(ref prev_kv_value) => {
                    prev_kv_value.clone().set_meta(upsert_kv.value_meta.clone())
                }
            },
        };

        new_seq_v.seq = Self::txn_incr_seq(GenericKV::NAME, txn_tree)?;
        kvs.insert(&upsert_kv.key, &new_seq_v)?;

        debug!("applied upsert: {:?} res: {:?}", upsert_kv, new_seq_v);
        Ok((expired, prev, Some(new_seq_v)))
    }

    fn txn_client_last_resp_update(
        &self,
        key: &str,
        value: (u64, AppliedState),
        txn_tree: &TransactionSledTree,
    ) -> Result<AppliedState, MetaStorageError> {
        let v = ClientLastRespValue {
            req_serial_num: value.0,
            res: value.1.clone(),
        };
        let txn_ks = txn_tree.key_space::<ClientLastResps>();
        txn_ks.insert(&key.to_string(), &v)?;

        Ok(value.1)
    }

    pub fn get_membership(&self) -> Result<Option<StoredMembership>, MetaStorageError> {
        let sm_meta = self.sm_meta();
        let mem = sm_meta
            .get(&StateMachineMetaKey::LastMembership)?
            .map(|x| x.try_into().expect("Membership"));

        Ok(mem)
    }

    pub fn get_last_applied(&self) -> Result<Option<LogId>, MetaStorageError> {
        let sm_meta = self.sm_meta();
        let last_applied = sm_meta
            .get(&LastApplied)?
            .map(|x| x.try_into().expect("LogId"));

        Ok(last_applied)
    }

    pub async fn add_node(&self, node_id: u64, node: &Node) -> Result<(), MetaStorageError> {
        let sm_nodes = self.nodes();
        sm_nodes.insert(&node_id, node).await?;
        Ok(())
    }

    pub fn get_client_last_resp(
        &self,
        key: &str,
    ) -> Result<Option<(u64, AppliedState)>, MetaStorageError> {
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
    ) -> Result<(u64, AppliedState), MetaStorageError> {
        let client_last_resps = txn_tree.key_space::<ClientLastResps>();
        let v = client_last_resps.get(&key.to_string())?;

        if let Some(resp) = v {
            return Ok((resp.req_serial_num, resp.res));
        }
        Ok((0, AppliedState::None))
    }

    pub fn get_node(&self, node_id: &NodeId) -> Result<Option<Node>, MetaStorageError> {
        let sm_nodes = self.nodes();
        sm_nodes.get(node_id)
    }

    pub fn get_nodes(&self) -> Result<Vec<Node>, MetaStorageError> {
        let sm_nodes = self.nodes();
        sm_nodes.range_values(..)
    }

    /// Expire an `SeqV` and returns the value discarded by expiration and the unexpired value:
    /// - `(Some, None)` if it expires.
    /// - `(None, Some)` if it does not.
    /// - `(None, None)` if the input is None.
    pub fn expire_seq_v<V>(
        seq_value: Option<SeqV<V>>,
        log_time_ms: u64,
    ) -> (Option<SeqV<V>>, Option<SeqV<V>>) {
        if let Some(s) = &seq_value {
            if s.get_expire_at() < log_time_ms {
                (seq_value, None)
            } else {
                (None, seq_value)
            }
        } else {
            (None, None)
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
}

/// Convert SeqV defined in rust types to SeqV defined in protobuf.
fn to_pb_seq_v(seq_v: SeqV) -> pb::SeqV {
    pb::SeqV {
        seq: seq_v.seq,
        data: seq_v.data,
    }
}

#[cfg(test)]
mod tests {
    use common_meta_types::KVMeta;
    use common_meta_types::SeqV;

    use crate::state_machine::StateMachine;

    #[test]
    fn test_expire_seq_v() -> anyhow::Result<()> {
        let sv = || SeqV::new(1, ());
        let expire_seq_v = StateMachine::expire_seq_v;

        assert_eq!((None, None), expire_seq_v(None, 10_000));
        assert_eq!((None, Some(sv())), expire_seq_v(Some(sv()), 10_000));

        assert_eq!(
            (None, Some(sv().set_meta(Some(KVMeta { expire_at: None })))),
            expire_seq_v(
                Some(sv().set_meta(Some(KVMeta { expire_at: None }))),
                10_000
            )
        );
        assert_eq!(
            (
                None,
                Some(sv().set_meta(Some(KVMeta {
                    expire_at: Some(20)
                })))
            ),
            expire_seq_v(
                Some(sv().set_meta(Some(KVMeta {
                    expire_at: Some(20)
                }))),
                10_000
            )
        );
        assert_eq!(
            (
                Some(sv().set_meta(Some(KVMeta { expire_at: Some(5) }))),
                None
            ),
            expire_seq_v(
                Some(sv().set_meta(Some(KVMeta { expire_at: Some(5) }))),
                10_000
            )
        );

        Ok(())
    }
}
