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

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::time::Duration;
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
use common_meta_types::error_context::WithContext;
use common_meta_types::txn_condition;
use common_meta_types::txn_op;
use common_meta_types::txn_op_response;
use common_meta_types::AppliedState;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::ConditionResult;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::MatchSeqExt;
use common_meta_types::MetaResult;
use common_meta_types::MetaStorageError;
use common_meta_types::MetaStorageResult;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::PbSeqV;
use common_meta_types::SeqV;
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
use num::FromPrimitive;
use openraft::raft::Entry;
use openraft::raft::EntryPayload;
use serde::Deserialize;
use serde::Serialize;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::config::RaftConfig;
use crate::sled_key_spaces::ClientLastResps;
use crate::sled_key_spaces::GenericKV;
use crate::sled_key_spaces::Nodes;
use crate::sled_key_spaces::Sequences;
use crate::sled_key_spaces::StateMachineMeta;
use crate::state_machine::ClientLastRespValue;
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
    fn kv_changed(&self, key: &str, prev: Option<SeqV>, current: Option<SeqV>);
}

type NotifyKVEvent = (String, Option<SeqV>, Option<SeqV>);

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
    ) -> Result<(SerializableSnapshot, Option<LogId>, String), MetaStorageError> {
        let last_applied = self.get_last_applied()?;

        let snapshot_idx = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot_id = if let Some(last) = last_applied {
            format!("{}-{}-{}", last.term, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let view = self.sm_tree.tree.iter();

        let mut kvs = Vec::new();
        for rkv in view {
            let (k, v) = rkv.context(|| "taking snapshot")?;
            kvs.push(vec![k.to_vec(), v.to_vec()]);
        }
        let snap = SerializableSnapshot { kvs };

        Ok((snap, last_applied, snapshot_id))
    }

    fn scan_prefix_if_needed(
        &self,
        entry: &Entry<LogEntry>,
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
    #[tracing::instrument(level = "debug", skip(self, entry), fields(log_id=%entry.log_id))]
    pub async fn apply(&self, entry: &Entry<LogEntry>) -> Result<AppliedState, MetaStorageError> {
        info!(
            "apply: summary: {}; payload: {:?}",
            entry.summary(),
            entry.payload
        );

        let log_id = &entry.log_id;

        debug!("sled tx start: {:?}", entry);

        let kv_pairs = self.scan_prefix_if_needed(entry)?;

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

                    let log_time_ms = match data.time_ms {
                        None => {
                            error!("log has no time: {}, treat every record with non-none `expire` as timed out", entry.summary());
                            0
                        }
                        Some(x) => {
                            let t = SystemTime::UNIX_EPOCH + Duration::from_millis(x);
                            info!("apply: raft-log time: {:?}", t);
                            x
                        },
                    };

                    let res = self.apply_cmd(&data.cmd, &txn_tree, kv_pairs.as_ref(), log_time_ms);
                    info!("apply_result: summary: {}; res: {:?}", entry.summary(), res);

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

        let opt_applied_state = result?;

        debug!("sled tx done: {:?}", entry);

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
            info!("applied AddNode: {}={:?}", node_id, node);
            Ok((prev, Some(node.clone())).into())
        }
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree))]
    fn apply_remove_node_cmd(
        &self,
        node_id: &u64,
        txn_tree: &TransactionSledTree,
    ) -> MetaStorageResult<AppliedState> {
        let sm_nodes = txn_tree.key_space::<Nodes>();

        let prev = sm_nodes.get(node_id)?;

        if prev.is_some() {
            info!("applied RemoveNode: {}={:?}", node_id, prev);
            sm_nodes.remove(node_id)?;
        }
        Ok((prev, None).into())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn apply_update_kv_cmd(
        &self,
        upsert_kv: &UpsertKV,
        txn_tree: &TransactionSledTree,
        log_time_ms: u64,
    ) -> MetaStorageResult<AppliedState> {
        debug!(upsert_kv = debug(upsert_kv), "apply_update_kv_cmd");

        let sub_tree = txn_tree.key_space::<GenericKV>();
        let (prev, result) = self.txn_sub_tree_upsert(&sub_tree, upsert_kv, log_time_ms)?;

        debug!("applied UpsertKV: {:?} {:?}", upsert_kv, result);

        if let Some(subscriber) = &self.subscriber {
            subscriber.kv_changed(&upsert_kv.key, prev.clone(), result.clone());
        }

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

    #[tracing::instrument(level = "debug", skip(self, txn_tree, cond))]
    fn txn_execute_one_condition(
        &self,
        txn_tree: &TransactionSledTree,
        cond: &TxnCondition,
    ) -> MetaStorageResult<bool> {
        debug!(cond = display(cond), "txn_execute_one_condition");

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

    #[tracing::instrument(level = "debug", skip(self, txn_tree, condition))]
    fn txn_execute_condition(
        &self,
        txn_tree: &TransactionSledTree,
        condition: &Vec<TxnCondition>,
    ) -> MetaStorageResult<bool> {
        for cond in condition {
            debug!(condition = display(cond), "txn_execute_condition");

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
    ) -> MetaStorageResult<()> {
        let sub_tree = txn_tree.key_space::<GenericKV>();
        let sv = sub_tree.get(&get.key)?;
        let value = sv.map(PbSeqV::from);
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
        txn_tree: &TransactionSledTree,
        put: &TxnPutRequest,
        resp: &mut TxnReply,
        events: &mut Option<Vec<NotifyKVEvent>>,
        log_time_ms: u64,
    ) -> MetaStorageResult<()> {
        let sub_tree = txn_tree.key_space::<GenericKV>();

        let (prev, result) = self.txn_sub_tree_upsert(
            &sub_tree,
            &UpsertKV::update(&put.key, &put.value),
            log_time_ms,
        )?;

        if let Some(events) = events {
            events.push((put.key.to_string(), prev.clone(), result));
        }

        let put_resp = TxnPutResponse {
            key: put.key.clone(),
            prev_value: if put.prev_value {
                prev.map(PbSeqV::from)
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
        txn_tree: &TransactionSledTree,
        delete: &TxnDeleteRequest,
        resp: &mut TxnReply,
        events: &mut Option<Vec<NotifyKVEvent>>,
        log_time_ms: u64,
    ) -> MetaStorageResult<()> {
        let sub_tree = txn_tree.key_space::<GenericKV>();

        let (prev, result) =
            self.txn_sub_tree_upsert(&sub_tree, &UpsertKV::delete(&delete.key), log_time_ms)?;

        if let Some(events) = events {
            events.push((delete.key.to_string(), prev.clone(), result));
        }

        let del_resp = TxnDeleteResponse {
            key: delete.key.clone(),
            success: prev.is_some(),
            prev_value: if delete.prev_value {
                prev.map(PbSeqV::from)
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
        txn_tree: &TransactionSledTree,
        delete_by_prefix: &TxnDeleteByPrefixRequest,
        kv_pairs: Option<&DeleteByPrefixKeyMap>,
        resp: &mut TxnReply,
        events: &mut Option<Vec<NotifyKVEvent>>,
        log_time_ms: u64,
    ) -> MetaStorageResult<()> {
        let mut count: u32 = 0;
        if let Some(kv_pairs) = kv_pairs {
            if let Some(kv_pairs) = kv_pairs.get(delete_by_prefix) {
                let sub_tree = txn_tree.key_space::<GenericKV>();
                for (key, _seq) in kv_pairs.iter() {
                    let ret =
                        self.txn_sub_tree_upsert(&sub_tree, &UpsertKV::delete(key), log_time_ms);

                    if let Ok(ret) = ret {
                        count += 1;

                        if let Some(events) = events {
                            events.push((key.to_string(), ret.0.clone(), ret.1));
                        }
                    }
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

    #[tracing::instrument(level = "debug", skip(self, txn_tree, op, resp))]
    fn txn_execute_operation(
        &self,
        txn_tree: &TransactionSledTree,
        op: &TxnOp,
        kv_pairs: Option<&DeleteByPrefixKeyMap>,
        resp: &mut TxnReply,
        events: &mut Option<Vec<NotifyKVEvent>>,
        log_time_ms: u64,
    ) -> MetaStorageResult<()> {
        debug!(op = display(op), "txn execute TxnOp");
        match &op.request {
            Some(txn_op::Request::Get(get)) => {
                self.txn_execute_get_operation(txn_tree, get, resp)?;
            }
            Some(txn_op::Request::Put(put)) => {
                self.txn_execute_put_operation(txn_tree, put, resp, events, log_time_ms)?;
            }
            Some(txn_op::Request::Delete(delete)) => {
                self.txn_execute_delete_operation(txn_tree, delete, resp, events, log_time_ms)?;
            }
            Some(txn_op::Request::DeleteByPrefix(delete_by_prefix)) => {
                self.txn_execute_delete_by_prefix_operation(
                    txn_tree,
                    delete_by_prefix,
                    kv_pairs,
                    resp,
                    events,
                    log_time_ms,
                )?;
            }
            None => {}
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, txn_tree, req))]
    fn apply_txn_cmd(
        &self,
        req: &TxnRequest,
        txn_tree: &TransactionSledTree,
        kv_pairs: Option<&(DeleteByPrefixKeyMap, DeleteByPrefixKeyMap)>,
        log_time_ms: u64,
    ) -> MetaStorageResult<AppliedState> {
        debug!(txn = display(req), "apply txn cmd");

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

        let mut events: Option<Vec<NotifyKVEvent>> = if self.subscriber.is_some() {
            Some(vec![])
        } else {
            None
        };
        for op in ops {
            self.txn_execute_operation(
                txn_tree,
                op,
                kv_op_pairs,
                &mut resp,
                &mut events,
                log_time_ms,
            )?;
        }

        if let Some(subscriber) = &self.subscriber {
            if let Some(events) = events {
                for event in events {
                    subscriber.kv_changed(&event.0, event.1, event.2);
                }
            }
        }

        Ok(AppliedState::TxnReply(resp))
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
        kv_pairs: Option<&(DeleteByPrefixKeyMap, DeleteByPrefixKeyMap)>,
        log_time_ms: u64,
    ) -> Result<AppliedState, MetaStorageError> {
        info!("apply_cmd: {:?}", cmd);

        match cmd {
            Cmd::IncrSeq { ref key } => self.apply_incr_seq_cmd(key, txn_tree),

            Cmd::AddNode {
                ref node_id,
                ref node,
            } => self.apply_add_node_cmd(node_id, node, txn_tree),

            Cmd::RemoveNode { ref node_id } => self.apply_remove_node_cmd(node_id, txn_tree),

            Cmd::UpsertKV(ref upsert_kv) => {
                self.apply_update_kv_cmd(upsert_kv, txn_tree, log_time_ms)
            }

            Cmd::Transaction(txn) => self.apply_txn_cmd(txn, txn_tree, kv_pairs, log_time_ms),
        }
    }

    fn txn_incr_seq(&self, key: &str, txn_tree: &TransactionSledTree) -> MetaStorageResult<u64> {
        let seq_sub_tree = txn_tree.key_space::<Sequences>();

        let key = key.to_string();
        let curr = seq_sub_tree.update_and_fetch(&key, |old| Some(old.unwrap_or_default() + 1))?;
        let curr = curr.unwrap();

        debug!("applied IncrSeq: {}={}", key, curr);

        Ok(curr.0)
    }

    #[allow(clippy::type_complexity)]
    fn txn_sub_tree_upsert<'s, KS>(
        &'s self,
        sub_tree: &AsTxnKeySpace<'s, KS>,
        upsert_kv: &UpsertKV,
        log_time_ms: u64,
    ) -> MetaStorageResult<(Option<SeqV>, Option<SeqV>)>
    where
        KS: SledKeySpace<K = String, V = SeqV>,
    {
        let prev = sub_tree.get(&upsert_kv.key)?;

        // If prev is timed out, treat it as a None.
        let prev = Self::unexpired_opt(prev, log_time_ms);

        if upsert_kv.seq.match_seq(&prev).is_err() {
            return Ok((prev.clone(), prev));
        }

        let mut new_seq_v = match &upsert_kv.value {
            Operation::Update(v) => SeqV::with_meta(0, upsert_kv.value_meta.clone(), v.clone()),
            Operation::Delete => {
                sub_tree.remove(&upsert_kv.key)?;
                return Ok((prev, None));
            }
            Operation::AsIs => match prev {
                None => return Ok((prev, None)),
                Some(ref prev_kv_value) => {
                    prev_kv_value.clone().set_meta(upsert_kv.value_meta.clone())
                }
            },
        };

        new_seq_v.seq = self.txn_incr_seq(KS::NAME, sub_tree)?;
        sub_tree.insert(&upsert_kv.key, &new_seq_v)?;

        debug!("applied upsert: {:?} res: {:?}", upsert_kv, new_seq_v);
        Ok((prev, Some(new_seq_v)))
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

    pub async fn add_node(&self, node_id: u64, node: &Node) -> MetaStorageResult<()> {
        let sm_nodes = self.nodes();
        sm_nodes.insert(&node_id, node).await?;
        Ok(())
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

    pub fn unexpired_opt<V: Debug>(
        seq_value: Option<SeqV<V>>,
        log_time_ms: u64,
    ) -> Option<SeqV<V>> {
        seq_value.and_then(|x| Self::unexpired(x, log_time_ms))
    }

    pub fn unexpired<V: Debug>(seq_value: SeqV<V>, log_time_ms: u64) -> Option<SeqV<V>> {
        // Caveat: The cleanup must be consistent across raft nodes:
        //         A conditional update, e.g. an upsert_kv() with MatchSeq::Eq(some_value),
        //         must be applied with the same timestamp on every raft node.
        //         Otherwise: node-1 could have applied a log with a ts that is smaller than value.expire_at,
        //         while node-2 may fail to apply the same log if it use a greater ts > value.expire_at.
        //
        // Thus:
        //
        // 1. A raft log must have a field ts assigned by the leader. When applying, use this ts to
        //    check against expire_at to decide whether to purge it.
        // 2. A GET operation must not purge any expired entry. Since a GET is only applied to a node itself.
        // 3. The background task can only be triggered by the raft leader, by submit a "clean expired" log.

        // TODO(xp): background task to clean expired
        // TODO(xp): maybe it needs a expiration queue for efficient cleaning up.

        debug!("seq_value: {:?} log_time_ms: {}", seq_value, log_time_ms);

        if seq_value.get_expire_at() < log_time_ms {
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
}
