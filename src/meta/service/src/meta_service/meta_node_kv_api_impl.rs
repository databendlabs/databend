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

use async_trait::async_trait;
use common_meta_client::MetaGrpcReadReq;
use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::GetKVReply;
use common_meta_kvapi::kvapi::GetKVReq;
use common_meta_kvapi::kvapi::KVStream;
use common_meta_kvapi::kvapi::ListKVReq;
use common_meta_kvapi::kvapi::MGetKVReply;
use common_meta_kvapi::kvapi::MGetKVReq;
use common_meta_kvapi::kvapi::UpsertKVReply;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::MetaAPIError;
use common_meta_types::MetaNetworkError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKV;
use futures::StreamExt;
use futures::TryStreamExt;
use log::info;

use crate::message::ForwardRequest;
use crate::meta_service::MetaNode;

/// Impl kvapi::KVApi for MetaNode.
///
/// Write through raft-log.
/// Read through local state machine, which may not be consistent.
/// E.g. Read is not guaranteed to see a write.
#[async_trait]
impl kvapi::KVApi for MetaNode {
    type Error = MetaAPIError;

    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        let ent = LogEntry::new(Cmd::UpsertKV(UpsertKV {
            key: act.key,
            seq: act.seq,
            value: act.value,
            value_meta: act.value_meta,
        }));
        let rst = self.write(ent).await?;

        match rst {
            AppliedState::KV(x) => Ok(x),
            _ => {
                unreachable!("expect type {}", "AppliedState::KV")
            }
        }
    }

    #[minitrace::trace]
    async fn get_kv(&self, key: &str) -> Result<GetKVReply, Self::Error> {
        let res = self
            .consistent_read(GetKVReq {
                key: key.to_string(),
            })
            .await?;

        Ok(res)
    }

    #[minitrace::trace]
    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, Self::Error> {
        let res = self
            .consistent_read(MGetKVReq {
                keys: keys.to_vec(),
            })
            .await?;

        Ok(res)
    }

    #[minitrace::trace]
    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        let req = ListKVReq {
            prefix: prefix.to_string(),
        };

        let strm = self
            .handle_forwardable_request(ForwardRequest {
                forward_to_leader: 1,
                body: MetaGrpcReadReq::ListKV(req),
            })
            .await?;

        let strm =
            strm.map_err(|status| MetaAPIError::NetworkError(MetaNetworkError::from(status)));
        Ok(strm.boxed())
    }

    #[minitrace::trace]
    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        info!("MetaNode::transaction(): {}", txn);
        let ent = LogEntry::new(Cmd::Transaction(txn));
        let rst = self.write(ent).await?;

        match rst {
            AppliedState::TxnReply(x) => Ok(x),
            _ => {
                unreachable!("expect type {}", "AppliedState::transaction",)
            }
        }
    }
}
