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

//! Raft log entry application to state machine.
//!
//! Applies committed raft log entries to the state machine including membership changes,
//! KV operations, transactions, and TTL cleanup.

use std::future::ready;
use std::io;
use std::time::Duration;

use databend_common_meta_types::node::Node;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::protobuf::boolean_expression::CombiningOperator;
use databend_common_meta_types::protobuf::BooleanExpression;
use databend_common_meta_types::protobuf::FetchAddU64;
use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::EntryPayload;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::txn_condition::Target;
use databend_common_meta_types::txn_op::Request;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Change;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::CmdContext;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::Interval;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnDeleteByPrefixRequest;
use databend_common_meta_types::TxnDeleteByPrefixResponse;
use databend_common_meta_types::TxnDeleteRequest;
use databend_common_meta_types::TxnDeleteResponse;
use databend_common_meta_types::TxnGetRequest;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnPutRequest;
use databend_common_meta_types::TxnPutResponse;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use display_more::DisplayUnixTimeStampExt;
use fastrace::func_name;
use futures::stream::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use num::FromPrimitive;
use seq_marked::SeqValue;
use state_machine_api::StateMachineApi;

use crate::state_machine_api_ext::StateMachineApiExt;

pub(crate) mod applier_data;

/// A helper that applies raft log `Entry` to the state machine.
pub struct Applier<SM>
where SM: StateMachineApi<SysData> + 'static
{
    pub(crate) sm: SM,

    /// The context of the current applying log.
    pub(crate) cmd_ctx: CmdContext,

    /// The changes have been made by the applying one log entry
    /// `(key, prev, result)`.
    changes: Vec<(String, Option<SeqV>, Option<SeqV>)>,
}

impl<SM> Applier<SM>
where SM: StateMachineApi<SysData> + 'static
{
    pub fn new(sm: SM) -> Self {
        Self {
            sm,
            cmd_ctx: CmdContext::from_millis(0),
            changes: Vec::new(),
        }
    }

    pub async fn commit(self) -> Result<(), io::Error> {
        self.sm.commit().await
    }

    /// Apply a log entry to state machine.
    ///
    /// And publish kv change events to subscriber.
    #[fastrace::trace]
    pub async fn apply(&mut self, entry: &Entry) -> Result<AppliedState, io::Error> {
        let log_id = &entry.log_id;
        let log_time_ms = Self::get_log_time(entry);

        debug!(
            "apply: entry: {}, log_time_ms: {}",
            entry,
            Duration::from_millis(log_time_ms).display_unix_timestamp_short()
        );

        self.cmd_ctx = CmdContext::from_millis(log_time_ms);

        self.clean_expired_kvs(log_time_ms).await?;

        self.sm.with_sys_data(move |sys_data| {
            *sys_data.last_applied_mut() = Some(*log_id);
        });

        let applied_state = match entry.payload {
            EntryPayload::Blank => {
                info!("apply: blank: {}", log_id);

                AppliedState::None
            }
            EntryPayload::Normal(ref data) => {
                info!("apply: normal: {} {}", log_id, data);

                self.apply_cmd(&data.cmd).await?
            }
            EntryPayload::Membership(ref mem) => {
                info!("apply: membership: {} {:?}", log_id, mem);

                let membership = StoredMembership::new(Some(*log_id), mem.clone());

                self.sm.with_sys_data(move |sys_data| {
                    *sys_data.last_membership_mut() = membership;
                });

                AppliedState::None
            }
        };

        // Send queued change events to subscriber
        for event in self.changes.drain(..) {
            debug!("send to EventSender: {:?}", event);
            self.sm.on_change_applied(event);
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

            Cmd::SetFeature { feature, enable } => self.apply_set_feature(feature, *enable),

            Cmd::UpsertKV(ref upsert_kv) => self.apply_upsert_kv(upsert_kv).await?,

            Cmd::Transaction(txn) => self.apply_txn(txn).await?,
        };

        debug!("apply_result: cmd: {}; res: {}", cmd, res);

        Ok(res)
    }

    /// Insert a node only when it does not exist or `overriding` is true.
    #[fastrace::trace]
    fn apply_add_node(&mut self, node_id: &u64, node: &Node, overriding: bool) -> AppliedState {
        let prev = self
            .sm
            .with_sys_data(|sys_data| sys_data.nodes_mut().get(node_id).cloned());

        if prev.is_none() || overriding {
            self.sm
                .with_sys_data(|sys_data| sys_data.nodes_mut().insert(*node_id, node.clone()));

            if prev.is_none() {
                info!("applied AddNode(non-overriding): {}={:?}", node_id, node);
            } else {
                info!("applied AddNode(overriding): {}={:?}", node_id, node);
            }

            (prev, Some(node.clone())).into()
        } else {
            (prev.clone(), prev).into()
        }
    }

    #[fastrace::trace]
    fn apply_remove_node(&mut self, node_id: &u64) -> AppliedState {
        let prev = self
            .sm
            .with_sys_data(|sys_data| sys_data.nodes_mut().remove(node_id));

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

    /// Toggle a state machine functional feature.
    #[fastrace::trace]
    fn apply_set_feature(&mut self, feature: &str, enable: bool) -> AppliedState {
        let prev = self.sm.with_sys_data(|sys_data| {
            let prev = sys_data.feature_enabled(feature);

            if enable {
                sys_data.features_mut().insert(feature.to_string());
            } else {
                sys_data.features_mut().remove(feature);
            }

            prev
        });

        info!(
            "apply_set_feature done: {} from {} to {}",
            feature, prev, enable
        );

        AppliedState::None
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
    pub(crate) async fn apply_txn(&mut self, req: &TxnRequest) -> Result<AppliedState, io::Error> {
        debug!(txn :% =(req); "apply txn cmd");

        // 1. Evaluate conditional operations one by one.
        //    Once one of them is successful, execute the corresponding operations and retrun.
        //    Otherwise, try next.
        for (i, conditional) in req.operations.iter().enumerate() {
            let success = if let Some(predicate) = &conditional.predicate {
                self.eval_bool_expression(predicate).await?
            } else {
                true
            };

            if success {
                let mut resp: TxnReply = TxnReply::new(format!("operation:{i}"));

                for op in &conditional.operations {
                    if let Some(request) = &op.request {
                        let r = self.txn_execute_operation(request).await?;
                        resp.responses.push(r);
                    }
                }

                return Ok(AppliedState::TxnReply(resp));
            }
        }

        // 2. For backward compatibility, evaluate the `condition` as the last conditional-operation.
        //    If success, execute the `if_then` operations

        let success = self.eval_txn_conditions(&req.condition).await?;

        let (ops, path) = if success {
            (&req.if_then, "then")
        } else {
            (&req.else_then, "else")
        };

        let mut resp: TxnReply = TxnReply::new(path);

        for op in ops {
            if let Some(request) = &op.request {
                let r = self.txn_execute_operation(request).await?;
                resp.responses.push(r);
            }
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

    fn eval_bool_expression<'x>(
        &'x mut self,
        tree: &'x BooleanExpression,
    ) -> BoxFuture<'x, Result<bool, io::Error>> {
        let op = tree.operator();

        let fu = async move {
            match op {
                CombiningOperator::And => {
                    for expr in tree.sub_expressions.iter() {
                        if !self.eval_bool_expression(expr).await? {
                            return Ok(false);
                        }
                    }

                    for cond in tree.conditions.iter() {
                        if !self.eval_one_condition(cond).await? {
                            return Ok(false);
                        }
                    }
                }
                CombiningOperator::Or => {
                    for expr in tree.sub_expressions.iter() {
                        if self.eval_bool_expression(expr).await? {
                            return Ok(true);
                        }
                    }

                    for cond in tree.conditions.iter() {
                        if self.eval_one_condition(cond).await? {
                            return Ok(true);
                        }
                    }
                }
            }
            Ok(true)
        };

        Box::pin(fu)
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

        let op = FromPrimitive::from_i32(cond.expected);
        let Some(op) = op else {
            warn!(
                "Invalid condition: {}; TxnCondition: {}",
                cond.expected, cond
            );
            return Ok(false);
        };

        let Some(against) = &cond.target else {
            return Ok(false);
        };

        let positive = match against {
            Target::Seq(against_seq) => Self::eval_compare(seqv.seq(), op, *against_seq),
            Target::Value(against_value) => {
                if let Some(stored) = seqv.value() {
                    Self::eval_compare(stored, op, against_value)
                } else {
                    false
                }
            }
            Target::KeysWithPrefix(against_n) => {
                let against_n = *against_n;

                let strm = self.sm.list_kv(key).await?;
                // Taking at most `against_n + 1` keys is just enough for every predicate.
                let strm = strm.take((against_n + 1) as usize);
                let count: u64 = strm.try_fold(0, |acc, _| ready(Ok(acc + 1))).await?;

                Self::eval_compare(count, op, against_n)
            }
        };
        Ok(positive)
    }

    fn eval_compare<T>(left: T, op: ConditionResult, right: T) -> bool
    where T: PartialOrd + PartialEq {
        use ConditionResult::*;
        match op {
            Eq => left == right,
            Gt => left > right,
            Lt => left < right,
            Ne => left != right,
            Ge => left >= right,
            Le => left <= right,
        }
    }

    #[fastrace::trace]
    async fn txn_execute_operation(
        &mut self,
        request: &pb::txn_op::Request,
    ) -> Result<TxnOpResponse, io::Error> {
        debug!("{}: request={}", func_name!(), request);

        let resp = match request {
            Request::Get(get) => {
                let r = self.txn_execute_get(get).await?;
                TxnOpResponse::new(r)
            }
            Request::Put(put) => {
                let r = self.txn_execute_put(put).await?;
                TxnOpResponse::new(r)
            }
            Request::Delete(delete) => {
                let r = self.txn_execute_delete(delete).await?;
                TxnOpResponse::new(r)
            }
            Request::DeleteByPrefix(delete_by_prefix) => {
                let r = self.txn_execute_delete_by_prefix(delete_by_prefix).await?;
                TxnOpResponse::new(r)
            }
            Request::FetchAddU64(fetch_add_u64) => {
                let r = self.txn_execute_fetch_add_u64(fetch_add_u64).await?;
                TxnOpResponse::new(r)
            }
            Request::PutSequential(put_sequential) => {
                let r = self.txn_execute_put_sequential(put_sequential).await?;
                TxnOpResponse::new(r)
            }
        };

        Ok(resp)
    }

    async fn txn_execute_get(&self, get: &TxnGetRequest) -> Result<pb::TxnGetResponse, io::Error> {
        let sv = self.sm.get_maybe_expired_kv(&get.key).await?;
        let get_resp = pb::TxnGetResponse {
            key: get.key.clone(),
            value: sv.map(pb::SeqV::from),
        };

        Ok(get_resp)
    }

    async fn txn_execute_put(&mut self, put: &TxnPutRequest) -> Result<TxnPutResponse, io::Error> {
        let upsert = UpsertKV::update(&put.key, &put.value).with(MetaSpec::new(
            put.expire_at,
            put.ttl_ms.map(Interval::from_millis),
        ));

        let (prev, result) = self.upsert_kv(&upsert).await?;

        let put_resp = TxnPutResponse {
            key: put.key.clone(),
            prev_value: prev.map(pb::SeqV::from),
            current: result.map(pb::SeqV::from),
        };

        Ok(put_resp)
    }

    async fn txn_execute_delete(
        &mut self,
        delete: &TxnDeleteRequest,
    ) -> Result<TxnDeleteResponse, io::Error> {
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

        Ok(del_resp)
    }

    async fn txn_execute_delete_by_prefix(
        &mut self,
        delete_by_prefix: &TxnDeleteByPrefixRequest,
    ) -> Result<TxnDeleteByPrefixResponse, io::Error> {
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

        Ok(del_resp)
    }

    async fn txn_execute_fetch_add_u64(
        &mut self,
        req: &FetchAddU64,
    ) -> Result<pb::FetchAddU64Response, io::Error> {
        let before_seqv = self.sm.get_maybe_expired_kv(&req.key).await?;

        let before_seq = before_seqv.seq();

        let before = if let Some(seqv) = &before_seqv {
            let bytes = &seqv.data;
            let u64_res: Result<u64, _> = serde_json::from_slice(bytes.as_slice());
            match u64_res {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        "fetch_add_u64: failed to deserialize u64 value: {:?}, error: {}",
                        bytes, e
                    );
                    0
                }
            }
        } else {
            0
        };

        if let Some(match_seq) = req.match_seq {
            if match_seq != before_seq {
                let response =
                    pb::FetchAddU64Response::new_unchanged(&req.key, SeqV::new(before_seq, before));
                return Ok(response);
            }
        }

        let after = before.saturating_add_signed(req.delta);

        let (_prev, result) = {
            let after_value = serde_json::to_vec(&after).expect("serialize u64 to json");
            let upsert = UpsertKV::update(&req.key, &after_value);
            self.upsert_kv(&upsert).await?
        };

        let response = pb::FetchAddU64Response::new(
            &req.key,
            SeqV::new(before_seq, before),
            SeqV::new(result.seq(), after),
        );

        Ok(response)
    }

    async fn txn_execute_put_sequential(
        &mut self,
        req: &pb::PutSequential,
    ) -> Result<TxnPutResponse, io::Error> {
        // Step 1. Build sequential key

        let fetch_add_u64 = FetchAddU64 {
            key: req.sequence_key.clone(),
            delta: 1,
            match_seq: None,
        };

        let fetch_add_response = self.txn_execute_fetch_add_u64(&fetch_add_u64).await?;
        let next_seq = fetch_add_response.before;

        let key = req.build_key(next_seq);

        // Step 2. Insert the key.

        let upsert = UpsertKV::update(&key, &req.value).with(MetaSpec::new(
            req.expires_at_ms,
            req.ttl_ms.map(Interval::from_millis),
        ));

        let (prev, result) = self.upsert_kv(&upsert).await?;

        let put_resp = TxnPutResponse {
            key,
            prev_value: prev.map(pb::SeqV::from),
            current: result.map(pb::SeqV::from),
        };

        Ok(put_resp)
    }

    /// Before applying, list expired keys to clean.
    ///
    /// All expired keys will be removed before applying a log.
    /// This is different from the sled based implementation.
    #[fastrace::trace]
    pub(crate) async fn clean_expired_kvs(&mut self, log_time_ms: u64) -> Result<(), io::Error> {
        if log_time_ms == 0 {
            return Ok(());
        }

        debug!(
            "to clean expired kvs, log_time_ts: {}",
            Duration::from_millis(log_time_ms).display_unix_timestamp_short()
        );

        let mut to_clean = vec![];
        let mut strm = self.sm.list_expire_index(log_time_ms).await?;

        // Save the log time for next cleaning.
        // Avoid listing tombstone records.
        self.sm
            .set_cleanup_start_timestamp(Duration::from_millis(log_time_ms));

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
            let upsert = UpsertKV::delete(key);
            self.upsert_kv(&upsert).await?;
            info!("clean expired: {} {expire_key}", upsert.key);
        }

        Ok(())
    }

    /// Push a **change** that is applied to `key`.
    ///
    /// It does nothing if `prev == result`
    pub fn push_change(&mut self, key: impl ToString, prev: Option<SeqV>, result: Option<SeqV>) {
        if prev == result {
            return;
        }

        self.changes.push((key.to_string(), prev, result))
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
                    debug!(
                        "apply: raft-log time: {}",
                        Duration::from_millis(ms).display_unix_timestamp_short()
                    );
                    ms
                }
            },
            _ => 0,
        }
    }
}
