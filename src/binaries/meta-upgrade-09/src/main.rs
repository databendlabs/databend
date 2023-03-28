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

#![allow(clippy::uninlined_format_args)]

//! This program upgrades metadata written by databend-query before 0.9 to a databend-query-0.9 compatible format.
//!
//! It loads metadata from a `raft-dir` and upgrade TableMeta to new version and write them back.
//! Both in raft-log data and in state-machine data will be converted.
//!
//! Usage:
//!
//! - Shut down all databend-meta processes.
//!
//! - Backup before proceeding: https://databend.rs/doc/deploy/metasrv/metasrv-backup-restore
//!
//! - Build it with `cargo build --bin databend-meta-upgrade-09`.
//!
//! - To view the current TableMeta version, print all TableMeta records with the following command:
//!   It should display a list of TableMeta record.
//!   You need to upgrade only if there is a `ver` that is lower than 24.
//!
//!    ```text
//!    databend-meta-upgrade-09 --cmd print --raft-dir "<./your/raft-dir/>"
//!    # output:
//!    # TableMeta { ver: 23, ..
//!    ```
//!
//! - Run it:
//!
//!   ```text
//!   databend-meta-upgrade-09 --cmd upgrade --raft-dir "<./your/raft-dir/>"
//!   ```
//!
//! - To assert upgrade has finished successfully, print all TableMeta records that are found in meta dir with the following command:
//!   It should display a list of TableMeta record with a `ver` that is greater or equal 24.
//!
//!    ```text
//!    databend-meta-upgrade-09 --cmd print --raft-dir "<./your/raft-dir/>"
//!    # output:
//!    # TableMeta { ver: 25, ..
//!    # TableMeta { ver: 25, ..
//!    ```

mod rewrite;

use anyhow::Error;
use clap::Parser;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableMeta;
use common_meta_kvapi::kvapi::Key;
use common_meta_raft_store::key_spaces::RaftStoreEntry;
use common_meta_sled_store::init_sled_db;
use common_meta_types::txn_condition::Target;
use common_meta_types::txn_op::Request;
use common_meta_types::Cmd;
use common_meta_types::Entry;
use common_meta_types::LogEntry;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::TxnCondition;
use common_meta_types::TxnOp;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKV;
use common_proto_conv::FromToProto;
use common_protos::pb;
use common_tracing::init_logging;
use common_tracing::Config as LogConfig;
use openraft::EntryPayload;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, author)]
pub struct Config {
    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long, default_value = "")]
    pub cmd: String,

    #[clap(flatten)]
    pub raft_config: RaftConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct RaftConfig {
    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long, default_value = "./_meta")]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: String,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            raft_dir: "./_meta".to_string(),
        }
    }
}

/// Usage:
/// - To convert meta-data: `$0 --cmd upgrade --raft-dir ./_your_meta_dir/`:
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();

    let _guards = init_logging("databend-meta-upgrade-09", &LogConfig::default());

    eprintln!();
    eprintln!("███╗   ███╗███████╗████████╗ █████╗ ");
    eprintln!("████╗ ████║██╔════╝╚══██╔══╝██╔══██╗");
    eprintln!("██╔████╔██║█████╗     ██║   ███████║");
    eprintln!("██║╚██╔╝██║██╔══╝     ██║   ██╔══██║");
    eprintln!("██║ ╚═╝ ██║███████╗   ██║   ██║  ██║");
    eprintln!("╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝");
    eprintln!("     --- Upgrade to 0.9 ---         ");
    eprintln!();

    eprintln!("config: {}", pretty(&config)?);

    match config.cmd.as_str() {
        "print" => {
            print_table_meta(&config)?;
        }
        "upgrade" => {
            let p = GenericKVProcessor {
                process_pb: conv_serialized_table_meta,
            };
            rewrite::rewrite(&config, |x| p.process(x))?;
        }
        _ => {
            return Err(anyhow::anyhow!("invalid cmd: {:?}", config.cmd));
        }
    }

    Ok(())
}

/// Print TableMeta in protobuf message format that are found in log or state machine.
pub fn print_table_meta(config: &Config) -> anyhow::Result<()> {
    let p = GenericKVProcessor {
        process_pb: print_serialized_table_meta,
    };

    let raft_config = &config.raft_config;

    init_sled_db(raft_config.raft_dir.clone());

    for tree_iter_res in common_meta_sled_store::iter::<Vec<u8>>() {
        let (_tree_name, item_iter) = tree_iter_res?;

        for item_res in item_iter {
            let (k, v) = item_res?;
            let v1_ent = RaftStoreEntry::deserialize(&k, &v)?;

            p.process(v1_ent)?;
        }
    }

    Ok(())
}

fn conv_serialized_table_meta(key: &str, v: Vec<u8>) -> Result<Vec<u8>, anyhow::Error> {
    // Only convert the key space `table-by-id`
    if !key.starts_with(TableId::PREFIX) {
        return Ok(v);
    }

    let p: pb::TableMeta = common_protos::prost::Message::decode(v.as_slice())?;
    let v1: TableMeta = FromToProto::from_pb(p)?;

    let p_latest = FromToProto::to_pb(&v1)?;

    let mut buf = vec![];
    common_protos::prost::Message::encode(&p_latest, &mut buf)?;
    Ok(buf)
}

fn print_serialized_table_meta(key: &str, v: Vec<u8>) -> Result<Vec<u8>, anyhow::Error> {
    if !key.starts_with(TableId::PREFIX) {
        return Ok(v);
    }

    let p: pb::TableMeta = common_protos::prost::Message::decode(v.as_slice())?;

    println!("{:?}", p);

    Ok(v)
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

trait Process {
    type Inp;
    type Ret;

    /// It returns None if nothing has to be done.
    /// Otherwise it returns Some(converted_data).
    fn process(&self, input: Self::Inp) -> Result<Option<Self::Ret>, anyhow::Error>;
}

/// A processor that only processes GenericKV data.
///
/// GenericKV data will be found in state-machine, logs that operates a GenericKV or a Txn.
struct GenericKVProcessor<F>
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

impl<F> GenericKVProcessor<F>
where F: Fn(&str, Vec<u8>) -> Result<Vec<u8>, anyhow::Error>
{
    /// Convert log and state machine record in meta-service.
    fn proc_raft_store_entry(
        &self,
        input: RaftStoreEntry,
    ) -> Result<Option<RaftStoreEntry>, anyhow::Error> {
        match input {
            RaftStoreEntry::Logs { key, value } => {
                let x = RaftStoreEntry::Logs {
                    key,
                    value: unwrap_or_return!(self.proc_raft_entry(value)?),
                };
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
            RaftStoreEntry::ClientLastResps { .. } => Ok(None),
            RaftStoreEntry::LogMeta { .. } => Ok(None),
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
            Cmd::UpsertKV(ups) => {
                let x = LogEntry {
                    txid: log_entry.txid,
                    time_ms: log_entry.time_ms,
                    cmd: Cmd::UpsertKV(unwrap_or_return!(self.proc_upsert_kv(ups)?)),
                };
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

                Ok(Some(LogEntry {
                    txid: log_entry.txid,
                    time_ms: log_entry.time_ms,
                    cmd: Cmd::Transaction(TxnRequest {
                        condition,
                        if_then,
                        else_then,
                    }),
                }))
            }
        }
    }

    fn proc_upsert_kv(&self, ups: UpsertKV) -> Result<Option<UpsertKV>, anyhow::Error> {
        match ups.value {
            Operation::Update(v) => {
                let buf = (self.process_pb)(&ups.key, v)?;

                Ok(Some(UpsertKV {
                    key: ups.key,
                    seq: ups.seq,
                    value: Operation::Update(buf),
                    value_meta: ups.value_meta,
                }))
            }
            Operation::Delete => Ok(None),
            Operation::AsIs => Ok(None),
        }
    }

    fn proc_condition(&self, c: TxnCondition) -> TxnCondition {
        if let Some(Target::Value(_)) = &c.target {
            unreachable!("we has never used value");
        }
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
        }

        Ok(TxnOp { request: Some(req) })
    }

    fn proc_tx_put_request(&self, p: TxnPutRequest) -> Result<TxnPutRequest, anyhow::Error> {
        let value = (self.process_pb)(&p.key, p.value)?;

        let pr = TxnPutRequest {
            key: p.key,
            value,
            prev_value: p.prev_value,
            expire_at: p.expire_at,
        };

        Ok(pr)
    }
}

fn pretty<T>(v: &T) -> Result<String, serde_json::Error>
where T: Serialize {
    serde_json::to_string_pretty(v)
}
