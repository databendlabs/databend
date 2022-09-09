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

use std::sync::Arc;

use common_meta_api::KVApi;
use common_meta_client::MetaGrpcReq;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;

use crate::meta_service::MetaNode;
use crate::metrics::incr_meta_metrics_meta_request_result;

pub struct ActionHandler {
    /// The raft-based meta data entry.
    pub(crate) meta_node: Arc<MetaNode>,
}

impl ActionHandler {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        ActionHandler { meta_node }
    }

    pub async fn execute_kv_req(&self, req: MetaGrpcReq) -> RaftReply {
        // To keep the code IDE-friendly, we manually expand the enum variants and dispatch them one by one

        match req {
            MetaGrpcReq::UpsertKV(a) => {
                let res = self.meta_node.upsert_kv(a).await;
                incr_meta_metrics_meta_request_result(res.is_ok());
                RaftReply::from(res)
            }
            MetaGrpcReq::GetKV(a) => {
                let res = self.meta_node.get_kv(&a.key).await;
                incr_meta_metrics_meta_request_result(res.is_ok());
                RaftReply::from(res)
            }
            MetaGrpcReq::MGetKV(a) => {
                let res = self.meta_node.mget_kv(&a.keys).await;
                incr_meta_metrics_meta_request_result(res.is_ok());
                RaftReply::from(res)
            }
            MetaGrpcReq::ListKV(a) => {
                let res = self.meta_node.prefix_list_kv(&a.prefix).await;
                incr_meta_metrics_meta_request_result(res.is_ok());
                RaftReply::from(res)
            }
        }
    }

    pub async fn execute_txn(&self, req: TxnRequest) -> TxnReply {
        let ret = self.meta_node.transaction(req).await;
        incr_meta_metrics_meta_request_result(ret.is_ok());

        match ret {
            Ok(resp) => resp,
            Err(err) => TxnReply {
                success: false,
                error: serde_json::to_string(&err).expect("fail to serialize"),
                responses: vec![],
            },
        }
    }
}
