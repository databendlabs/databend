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

use std::io;
use std::time::Duration;
use std::time::SystemTime;

use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::txn_condition;
use databend_common_meta_types::txn_op;
use databend_common_meta_types::txn_op_response;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Change;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::CmdContext;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::Interval;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::Node;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::StoredMembership;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnDeleteByPrefixRequest;
use databend_common_meta_types::TxnDeleteByPrefixResponse;
use databend_common_meta_types::TxnDeleteRequest;
use databend_common_meta_types::TxnDeleteResponse;
use databend_common_meta_types::TxnGetRequest;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnPutRequest;
use databend_common_meta_types::TxnPutResponse;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use futures::stream::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use num::FromPrimitive;

use crate::sm_v003::SMV003;

/// A helper that applies raft log `Entry` to the state machine.
pub struct Applier<'a> {
    sm: &'a mut SMV003,

    /// The context of the current applying log.
    cmd_ctx: CmdContext,

    /// The changes has been made by the applying one log entry
    changes: Vec<Change<Vec<u8>, String>>,
}

impl<'a> Applier<'a> {
    pub fn new(sm: &'a mut SMV003) -> Self {
        Self {
            sm,
            cmd_ctx: CmdContext::from_millis(0),
            changes: Vec::new(),
        }
    }

    /// Apply an log entry to state machine.
    ///
    /// And publish kv change events to subscriber.
    #[fastrace::trace]
    pub async fn apply(&mut self, entry: &Entry) -> Result<AppliedState, io::Error> {
        let log_id = &entry.log_id;
        let log_time_ms = Self::get_log_time(entry);

        debug!("apply: entry: {}, log_time_ms: {}", entry, log_time_ms);

        self.cmd_ctx = CmdContext::from_millis(log_time_ms);

        self.clean_expired_kvs(log_time_ms).await?;

        *self.sm.sys_data_mut().last_applied_mut() = Some(*log_id);

        let applied_state = match entry.payload {
            EntryPayload::Blank => {
                info!("apply: blank: {}", log_id);

                AppliedState::None
            }
            EntryPayload::Normal(ref data) => {
                info!("apply: normal: {} {}", log_id, data);
                assert!(data.txid.is_none(), "txid is disabled");

                self.apply_cmd(&data.cmd).await?
            }
            EntryPayload::Membership(ref mem) => {
                info!("apply: membership: {} {:?}", log_id, mem);

                *self.sm.sys_data_mut().last_membership_mut() =
                    StoredMembership::new(Some(*log_id), mem.clone());
                AppliedState::None
            }
        };

        // Send queued change events to subscriber
        if let Some(subscriber) = &self.sm.subscriber {
            for event in self.changes.drain(..) {
                subscriber.kv_changed(event);
            }
        }

        Ok(applied_state)
    }

    /// Apply a `Cmd` to state machine.
    ///
    /// Already applied log should be filtered out before passing into this function.
    /// This is the only entry to modify state machine.
    /// The `cmd` is always committed by raft before applying.
    #[fastrace::trace]
    pub async fn apply_cmd(&mut self, cmd: &Cmd) -> Result<AppliedState, io::Error> {
        debug!("apply_cmd: {}", cmd);

        let res = match cmd {
            Cmd::AddNode {
                node_id,
                node,
                overriding,
            } => self.apply_add_node(node_id, node, *overriding),

            Cmd::RemoveNode { ref node_id } => self.apply_remove_node(node_id),

            Cmd::UpsertKV(ref upsert_kv) => self.apply_upsert_kv(upsert_kv).await?,

            Cmd::Transaction(txn) => self.apply_txn(txn).await?,
        };

        debug!("apply_result: cmd: {}; res: {}", cmd, res);

        Ok(res)
    }

    /// Insert a node only when it does not exist or `overriding` is true.
    #[fastrace::trace]
    fn apply_add_node(&mut self, node_id: &u64, node: &Node, overriding: bool) -> AppliedState {
        let prev = self.sm.sys_data_mut().nodes_mut().get(node_id).cloned();

        if prev.is_none() {
            self.sm
                .sys_data_mut()
                .nodes_mut()
                .insert(*node_id, node.clone());
            info!("applied AddNode(non-overriding): {}={:?}", node_id, node);
            return (prev, Some(node.clone())).into();
        }

        if overriding {
            self.sm
                .sys_data_mut()
                .nodes_mut()
                .insert(*node_id, node.clone());
            info!("applied AddNode(overriding): {}={:?}", node_id, node);
            (prev, Some(node.clone())).into()
        } else {
            (prev.clone(), prev).into()
        }
    }

    #[fastrace::trace]
    fn apply_remove_node(&mut self, node_id: &u64) -> AppliedState {
        let prev = self.sm.sys_data_mut().nodes_mut().remove(node_id);
        info!("applied RemoveNode: {}={:?}", node_id, prev);

        (prev, None).into()
    }

    /// Execute an upsert-kv operation.
    ///
    /// KV has two indexes:
    /// - The primary index: `key -> (seq, meta(expire_time), value)`,
    /// - and a secondary expiration index: `(expire_time, seq) -> key`.
    ///
    /// Thus upsert a kv entry is done in two steps:
    /// update the primary index and optionally update the secondary index.
    #[fastrace::trace]
    async fn apply_upsert_kv(&mut self, upsert_kv: &UpsertKV) -> Result<AppliedState, io::Error> {
        debug!(upsert_kv :? =(upsert_kv); "apply_update_kv_cmd");

        let (prev, result) = self.upsert_kv(upsert_kv).await?;

        let st = Change::new(prev, result).into();
        Ok(st)
    }

    /// Update or insert a kv entry.
    ///
    /// If the input entry has expired, it performs a delete operation.
    #[fastrace::trace]
    pub(crate) async fn upsert_kv(
        &mut self,
        upsert_kv: &UpsertKV,
    ) -> Result<(Option<SeqV>, Option<SeqV>), io::Error> {
        debug!(upsert_kv :? =(upsert_kv); "upsert_kv");

        let (prev, result) = self
            .sm
            .upsert_kv_primary_index(upsert_kv, &self.cmd_ctx)
            .await?;

        self.sm
            .update_expire_index(&upsert_kv.key, &prev, &result)
            .await?;

        let prev = Into::<Option<SeqV>>::into(prev);
        let result = Into::<Option<SeqV>>::into(result);

        debug!(
            "applied UpsertKV: {:?}; prev: {:?}; result: {:?}",
            upsert_kv, prev, result
        );

        self.push_change(&upsert_kv.key, prev.clone(), result.clone());

        Ok((prev, result))
    }

    #[fastrace::trace]
    async fn apply_txn(&mut self, req: &TxnRequest) -> Result<AppliedState, io::Error> {
        debug!(txn :% =(req); "apply txn cmd");

        let success = self.eval_txn_conditions(&req.condition).await?;

        let ops = if success {
            &req.if_then
        } else {
            &req.else_then
        };

        let mut resp: TxnReply = TxnReply {
            success,
            error: "".to_string(),
            responses: vec![],
        };

        for op in ops {
            self.txn_execute_operation(op, &mut resp).await?;
        }

        Ok(AppliedState::TxnReply(resp))
    }

    #[fastrace::trace]
    async fn eval_txn_conditions(
        &mut self,
        condition: &Vec<TxnCondition>,
    ) -> Result<bool, io::Error> {
        for cond in condition {
            debug!(condition :% =(cond); "txn_execute_condition");

            if !self.eval_one_condition(cond).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    #[fastrace::trace]
    async fn eval_one_condition(&self, cond: &TxnCondition) -> Result<bool, io::Error> {
        debug!(cond :% =(cond); "txn_execute_one_condition");

        let key = &cond.key;
        // No expiration check:
        // If the key expired, it should be treated as `None` value.
        // sm.get_kv() does not check expiration.
        // Expired keys are cleaned before applying a log, see: `clean_expired_kvs()`.
        let seqv = self.sm.get_maybe_expired_kv(key).await?;

        debug!(
            "txn_execute_one_condition: key: {} curr: seq:{} value:{:?}",
            key,
            seqv.seq(),
            seqv.value()
        );

        let target = if let Some(target) = &cond.target {
            target
        } else {
            return Ok(false);
        };

        let positive = match target {
            txn_condition::Target::Seq(right) => {
                Self::eval_seq_condition(seqv.seq(), cond.expected, right)
            }
            txn_condition::Target::Value(right) => {
                if let Some(v) = seqv.value() {
                    Self::eval_value_condition(v, cond.expected, right)
                } else {
                    false
                }
            }
        };
        Ok(positive)
    }

    fn eval_seq_condition(left: u64, op: i32, right: &u64) -> bool {
        match FromPrimitive::from_i32(op) {
            Some(ConditionResult::Eq) => left == *right,
            Some(ConditionResult::Gt) => left > *right,
            Some(ConditionResult::Lt) => left < *right,
            Some(ConditionResult::Ne) => left != *right,
            Some(ConditionResult::Ge) => left >= *right,
            Some(ConditionResult::Le) => left <= *right,
            _ => false,
        }
    }

    fn eval_value_condition(left: &Vec<u8>, op: i32, right: &Vec<u8>) -> bool {
        match FromPrimitive::from_i32(op) {
            Some(ConditionResult::Eq) => left == right,
            Some(ConditionResult::Gt) => left > right,
            Some(ConditionResult::Lt) => left < right,
            Some(ConditionResult::Ne) => left != right,
            Some(ConditionResult::Ge) => left >= right,
            Some(ConditionResult::Le) => left <= right,
            _ => false,
        }
    }

    #[fastrace::trace]
    async fn txn_execute_operation(
        &mut self,
        op: &TxnOp,
        resp: &mut TxnReply,
    ) -> Result<(), io::Error> {
        debug!(op :% =(op); "txn execute TxnOp");
        match &op.request {
            Some(txn_op::Request::Get(get)) => {
                self.txn_execute_get(get, resp).await?;
            }
            Some(txn_op::Request::Put(put)) => {
                self.txn_execute_put(put, resp).await?;
            }
            Some(txn_op::Request::Delete(delete)) => {
                self.txn_execute_delete(delete, resp).await?;
            }
            Some(txn_op::Request::DeleteByPrefix(delete_by_prefix)) => {
                self.txn_execute_delete_by_prefix(delete_by_prefix, resp)
                    .await?;
            }
            None => {}
        }
        Ok(())
    }

    async fn txn_execute_get(
        &self,
        get: &TxnGetRequest,
        resp: &mut TxnReply,
    ) -> Result<(), io::Error> {
        let sv = self.sm.get_maybe_expired_kv(&get.key).await?;
        let get_resp = TxnOpResponse::get(get.key.clone(), sv);

        resp.responses.push(get_resp);

        Ok(())
    }

    async fn txn_execute_put(
        &mut self,
        put: &TxnPutRequest,
        resp: &mut TxnReply,
    ) -> Result<(), io::Error> {
        let upsert = UpsertKV::update(&put.key, &put.value).with(MetaSpec::new(
            put.expire_at,
            put.ttl_ms.map(Interval::from_millis),
        ));

        let (prev, _result) = self.upsert_kv(&upsert).await?;

        let put_resp = TxnPutResponse {
            key: put.key.clone(),
            prev_value: prev.map(pb::SeqV::from),
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Put(put_resp)),
        });

        Ok(())
    }

    async fn txn_execute_delete(
        &mut self,
        delete: &TxnDeleteRequest,
        resp: &mut TxnReply,
    ) -> Result<(), io::Error> {
        let upsert = UpsertKV::delete(&delete.key);

        // If `delete.match_seq` is `Some`, only delete the entry with the exact `seq`.
        let upsert = if let Some(seq) = delete.match_seq {
            upsert.with(MatchSeq::Exact(seq))
        } else {
            upsert
        };

        let (prev, result) = self.upsert_kv(&upsert).await?;
        let is_deleted = prev.is_some() && result.is_none();

        let del_resp = TxnDeleteResponse {
            key: delete.key.clone(),
            success: is_deleted,
            prev_value: prev.map(pb::SeqV::from),
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Delete(del_resp)),
        });
        Ok(())
    }

    async fn txn_execute_delete_by_prefix(
        &mut self,
        delete_by_prefix: &TxnDeleteByPrefixRequest,
        resp: &mut TxnReply,
    ) -> Result<(), io::Error> {
        let mut strm = self.sm.list_kv(&delete_by_prefix.prefix).await?;
        let mut count = 0;

        while let Some((key, _seq_v)) = strm.try_next().await? {
            let (prev, res) = self.upsert_kv(&UpsertKV::delete(&key)).await?;
            self.push_change(key, prev, res);
            count += 1;
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

    /// Before applying, list expired keys to clean.
    ///
    /// All expired keys will be removed before applying a log.
    /// This is different from the sled based implementation.
    #[fastrace::trace]
    async fn clean_expired_kvs(&mut self, log_time_ms: u64) -> Result<(), io::Error> {
        if log_time_ms == 0 {
            return Ok(());
        }

        debug!("to clean expired kvs, log_time_ts: {}", log_time_ms);

        let mut to_clean = vec![];
        let mut strm = self.sm.list_expire_index(log_time_ms).await?;

        {
            let mut strm = std::pin::pin!(strm);
            while let Some((expire_key, key)) = strm.try_next().await? {
                if !expire_key.is_expired(log_time_ms) {
                    break;
                }

                to_clean.push((expire_key, key));
            }
        }

        for (expire_key, key) in to_clean {
            let curr = self.sm.get_maybe_expired_kv(&key).await?;
            if let Some(seq_v) = &curr {
                assert_eq!(expire_key.seq, seq_v.seq);
                info!("clean expired: {}, {}", key, expire_key);

                self.upsert_kv(&UpsertKV::delete(key.clone())).await?;
            } else {
                unreachable!(
                    "trying to remove un-cleanable: {}, {}, kv-entry: {:?}",
                    key, expire_key, curr
                );
            }
        }

        self.sm.update_expire_cursor(log_time_ms);

        Ok(())
    }

    /// Push a **change** that is applied to `key`.
    ///
    /// It does nothing if `prev == result`
    pub fn push_change(&mut self, key: impl ToString, prev: Option<SeqV>, result: Option<SeqV>) {
        if prev == result {
            return;
        }

        self.changes
            .push(Change::new(prev, result).with_id(key.to_string()))
    }

    /// Retrieve the proposing time from a raft-log.
    ///
    /// Only `Normal` log has a time embedded.
    #[fastrace::trace]
    fn get_log_time(entry: &Entry) -> u64 {
        match &entry.payload {
            EntryPayload::Normal(data) => match data.time_ms {
                None => {
                    error!(
                        "log has no time: {}, treat every entry with non-none `expire` as timed out",
                        entry
                    );
                    0
                }
                Some(ms) => {
                    let t = SystemTime::UNIX_EPOCH + Duration::from_millis(ms);
                    debug!("apply: raft-log time: {:?}", t);
                    ms
                }
            },
            _ => 0,
        }
    }
}
