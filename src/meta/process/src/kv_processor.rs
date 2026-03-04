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

use anyhow::Error;
use databend_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_meta_types::Cmd;
use databend_meta_types::LogEntry;
use databend_meta_types::Operation;
use databend_meta_types::SeqV;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnPutRequest;
use databend_meta_types::TxnRequest;
use databend_meta_types::UpsertKV;
use databend_meta_types::raft_types::Entry;
use databend_meta_types::txn_condition::Target;
use databend_meta_types::txn_op::Request;
use openraft::EntryPayload;

use crate::process::Process;

/// A processor that only processes GenericKV data.
///
/// GenericKV data will be found in state-machine, logs that operates a GenericKV or a Txn.
pub struct GenericKVProcessor<F>
where F: Fn(&str, Vec<u8>) -> Result<Vec<u8>, anyhow::Error>
{
    /// A callback to process serialized protobuf message
    process_pb: F,
}

impl<F> Process for GenericKVProcessor<F>
where F: Fn(&str, Vec<u8>) -> Result<Vec<u8>, anyhow::Error>
{
    type Inp = RaftStoreEntry;
    type Ret = RaftStoreEntry;

    fn process(&self, input: RaftStoreEntry) -> Result<Option<RaftStoreEntry>, Error> {
        self.proc_raft_store_entry(input)
    }
}

macro_rules! unwrap_or_return {
    ($v: expr) => {
        if let Some(x) = $v {
            x
        } else {
            return Ok(None);
        }
    };
}

impl<F> GenericKVProcessor<F>
where F: Fn(&str, Vec<u8>) -> Result<Vec<u8>, anyhow::Error>
{
    pub fn new(process_pb: F) -> Self {
        GenericKVProcessor { process_pb }
    }
    /// Convert log and state machine record in meta-service.
    fn proc_raft_store_entry(
        &self,
        input: RaftStoreEntry,
    ) -> Result<Option<RaftStoreEntry>, anyhow::Error> {
        match input {
            RaftStoreEntry::DataHeader { .. } => Ok(None),
            RaftStoreEntry::Logs { key, value } => {
                let x = RaftStoreEntry::Logs {
                    key,
                    value: unwrap_or_return!(self.proc_raft_entry(value)?),
                };
                Ok(Some(x))
            }

            RaftStoreEntry::LogEntry(entry) => {
                let x = RaftStoreEntry::LogEntry(unwrap_or_return!(self.proc_raft_entry(entry)?));
                Ok(Some(x))
            }

            RaftStoreEntry::GenericKV { key, value } => {
                let data = (self.process_pb)(&key, value.data)?;

                let x = RaftStoreEntry::GenericKV {
                    key,
                    value: SeqV {
                        seq: value.seq,
                        meta: value.meta,
                        data,
                    },
                };

                Ok(Some(x))
            }

            RaftStoreEntry::Nodes { .. } => Ok(None),
            RaftStoreEntry::StateMachineMeta { .. } => Ok(None),
            RaftStoreEntry::RaftStateKV { .. } => Ok(None),
            RaftStoreEntry::Expire { .. } => Ok(None),
            RaftStoreEntry::Sequences { .. } => Ok(None),
            RaftStoreEntry::LogMeta { .. } => Ok(None),

            RaftStoreEntry::NodeId(_) => Ok(None),
            RaftStoreEntry::Vote(_) => Ok(None),
            RaftStoreEntry::Committed(_) => Ok(None),
            RaftStoreEntry::Purged(_) => Ok(None),
        }
    }

    fn proc_raft_entry(&self, ent: Entry) -> Result<Option<Entry>, anyhow::Error> {
        match ent.payload {
            EntryPayload::Blank => Ok(None),
            EntryPayload::Membership(_) => Ok(None),
            EntryPayload::Normal(log_entry) => {
                let x = Entry {
                    log_id: ent.log_id,
                    payload: EntryPayload::Normal(unwrap_or_return!(
                        self.proc_log_entry(log_entry)?
                    )),
                };
                Ok(Some(x))
            }
        }
    }

    fn proc_log_entry(&self, log_entry: LogEntry) -> Result<Option<LogEntry>, anyhow::Error> {
        match log_entry.cmd {
            Cmd::AddNode { .. } => Ok(None),
            Cmd::RemoveNode { .. } => Ok(None),
            Cmd::SetFeature { .. } => Ok(None),
            Cmd::UpsertKV(ups) => {
                let x = LogEntry::new_with_time(
                    Cmd::UpsertKV(unwrap_or_return!(self.proc_upsert_kv(ups)?)),
                    log_entry.time_ms,
                );
                Ok(Some(x))
            }
            Cmd::Transaction(tx) => {
                let mut condition = vec![];
                for c in tx.condition {
                    condition.push(self.proc_condition(c));
                }

                let mut if_then = vec![];
                for op in tx.if_then {
                    if_then.push(self.proc_txop(op)?);
                }

                let mut else_then = vec![];
                for op in tx.else_then {
                    else_then.push(self.proc_txop(op)?);
                }

                Ok(Some(LogEntry::new_with_time(
                    Cmd::Transaction(TxnRequest::new(condition, if_then).with_else(else_then)),
                    log_entry.time_ms,
                )))
            }
        }
    }

    fn proc_upsert_kv(&self, ups: UpsertKV) -> Result<Option<UpsertKV>, anyhow::Error> {
        match ups.value {
            Operation::Update(v) => {
                let buf = (self.process_pb)(&ups.key, v)?;

                Ok(Some(UpsertKV::new(
                    ups.key,
                    ups.seq,
                    Operation::Update(buf),
                    ups.value_meta,
                )))
            }
            Operation::Delete => Ok(None),
            #[allow(deprecated)]
            Operation::AsIs => Ok(None),
        }
    }

    fn proc_condition(&self, c: TxnCondition) -> TxnCondition {
        debug_assert!(
            !matches!(&c.target, Some(Target::Value(_))),
            "we have never used value"
        );
        c
    }

    fn proc_txop(&self, op: TxnOp) -> Result<TxnOp, anyhow::Error> {
        let req = match op.request {
            None => {
                return Ok(op);
            }
            Some(x) => x,
        };

        match req {
            Request::Get(_) => {}
            Request::Put(p) => {
                return Ok(TxnOp {
                    request: Some(Request::Put(self.proc_tx_put_request(p)?)),
                });
            }
            Request::Delete(_) => {}
            Request::DeleteByPrefix(_) => {}
            Request::FetchIncreaseU64(_) => {}
            Request::PutSequential(_) => {}
        }

        Ok(TxnOp { request: Some(req) })
    }

    fn proc_tx_put_request(&self, p: TxnPutRequest) -> Result<TxnPutRequest, anyhow::Error> {
        let value = (self.process_pb)(&p.key, p.value)?;

        let pr = TxnPutRequest::new(p.key, value, p.prev_value, p.expire_at, p.ttl_ms);

        Ok(pr)
    }
}
