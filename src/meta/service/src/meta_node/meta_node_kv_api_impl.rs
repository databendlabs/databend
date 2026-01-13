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

use std::sync::Arc;

use async_trait::async_trait;
use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::ListKVReq;
use databend_common_meta_kvapi::kvapi::MGetKVReq;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::limit_stream;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::MetaAPIError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures::StreamExt;
use futures::TryStreamExt;
use log::info;

use crate::message::ForwardRequest;
use crate::meta_service::MetaNode;
use crate::metrics::server_metrics;

/// A wrapper of MetaNode that implements kvapi::KVApi.
pub struct MetaKVApi<'a> {
    inner: &'a MetaNode,
}

impl<'a> MetaKVApi<'a> {
    pub fn new(inner: &'a MetaNode) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<'a> kvapi::KVApi for MetaKVApi<'a> {
    type Error = MetaAPIError;

    async fn upsert_kv(&self, act: UpsertKV) -> Result<UpsertKVReply, Self::Error> {
        let ent = LogEntry::new(Cmd::UpsertKV(act));
        let rst = self.inner.write(ent).await?;

        match rst {
            AppliedState::KV(x) => Ok(x),
            _ => {
                unreachable!("expect type {}", "AppliedState::KV")
            }
        }
    }

    #[fastrace::trace]
    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        let req = MGetKVReq {
            keys: keys.to_vec(),
        };

        let res = self
            .inner
            .handle_forwardable_request(ForwardRequest::new(1, MetaGrpcReadReq::MGetKV(req)))
            .await;

        server_metrics::incr_read_result(&res);

        // TODO: enable returning endpoint
        let (_endpoint, strm) = res?;
        let strm = strm.map_err(MetaAPIError::from);

        Ok(strm.boxed())
    }

    #[fastrace::trace]
    async fn list_kv(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        let req = ListKVReq {
            prefix: prefix.to_string(),
        };

        let res = self
            .inner
            .handle_forwardable_request(ForwardRequest::new(1, MetaGrpcReadReq::ListKV(req)))
            .await;

        server_metrics::incr_read_result(&res);

        // TODO: enable returning endpoint
        let (_endpoint, strm) = res?;
        let strm = strm.map_err(MetaAPIError::from);
        Ok(limit_stream(strm, limit))
    }

    #[fastrace::trace]
    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        info!("MetaNode::transaction(): {}", txn);

        let ent = LogEntry::new(Cmd::Transaction(txn));
        let rst = self.inner.write(ent).await?;

        match rst {
            AppliedState::TxnReply(x) => Ok(x),
            _ => {
                unreachable!("expect type {}", "AppliedState::transaction",)
            }
        }
    }
}

/// A wrapper of MetaNode that implements kvapi::KVApi.
pub struct MetaKVApiOwned {
    inner: Arc<MetaNode>,
}

impl MetaKVApiOwned {
    pub fn new(inner: Arc<MetaNode>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl kvapi::KVApi for MetaKVApiOwned {
    type Error = MetaAPIError;

    async fn upsert_kv(&self, act: UpsertKV) -> Result<UpsertKVReply, Self::Error> {
        self.inner.kv_api().upsert_kv(act).await
    }

    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        self.inner.kv_api().get_kv_stream(keys).await
    }

    async fn list_kv(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        self.inner.kv_api().list_kv(prefix, limit).await
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        self.inner.kv_api().transaction(txn).await
    }
}
