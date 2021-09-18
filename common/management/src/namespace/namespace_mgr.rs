// Copyright 2020 Datafuse Labs.
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
//

use std::sync::Arc;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_metatypes::MatchSeq;
use common_metatypes::SeqValue;
use common_store_api::KVApi;

use crate::namespace::NamespaceApi;
use crate::namespace::NodeInfo;

#[allow(dead_code)]
pub static NAMESPACE_API_KEY_PREFIX: &str = "__fd_namespaces";

#[allow(dead_code)]
pub struct NamespaceMgr {
    kv_api: Arc<dyn KVApi>,
}

impl NamespaceMgr {
    #[allow(dead_code)]
    pub fn new(kv_api: Arc<dyn KVApi>) -> Self {
        NamespaceMgr { kv_api }
    }

    pub fn key_prefix(&self, prefixes: &[String]) -> String {
        let mut res = NAMESPACE_API_KEY_PREFIX.to_string();
        for prefix in prefixes {
            res.push('/');
            res.push_str(prefix.as_str());
        }
        res
    }
}

#[async_trait]
impl NamespaceApi for NamespaceMgr {
    async fn add_node(
        &self,
        tenant_id: String,
        namespace_id: String,
        node: NodeInfo,
    ) -> Result<u64> {
        // Only when there are no record, i.e. seq=0
        let match_seq = MatchSeq::Exact(0);

        let key = self.key_prefix(&[tenant_id, namespace_id, node.id.clone()]);
        let value = serde_json::to_vec(&node)?;

        let res = self
            .kv_api
            .upsert_kv(&key, match_seq, Some(value), None)
            .await?;

        match (res.prev, res.result) {
            (None, Some((s, _))) => Ok(s), // do we need to check the seq returned?
            (Some((s, _)), None) => Err(ErrorCode::NamespaceNodeAlreadyExists(format!(
                "Namespace already exists, seq [{}]",
                s
            ))),
            r @ (_, _) => Err(ErrorCode::UnknownException(format!(
                "upsert result not expected (using version 0, got {:?})",
                r
            ))),
        }
    }

    async fn get_nodes(
        &self,
        tenant_id: String,
        namespace_id: String,
        _seq: Option<u64>,
    ) -> Result<Vec<SeqValue<NodeInfo>>> {
        let key = self.key_prefix(&[tenant_id, namespace_id]);
        let values = self.kv_api.prefix_list_kv(key.as_str()).await?;
        let mut r = vec![];
        for (_key, (s, val)) in values {
            let u = serde_json::from_slice::<NodeInfo>(&val.value)
                .map_err_to_code(ErrorCode::NamespaceIllegalNodeFormat, || "")?;

            r.push((s, u));
        }
        Ok(r)
    }

    async fn update_node(
        &self,
        tenant_id: String,
        namespace_id: String,
        node: NodeInfo,
        seq: Option<u64>,
    ) -> Result<Option<u64>> {
        let key = self.key_prefix(&[tenant_id, namespace_id, node.id.clone()]);
        let value = serde_json::to_vec(&node)?;

        let match_seq = match seq {
            None => MatchSeq::GE(1),
            Some(s) => MatchSeq::Exact(s),
        };
        let res = self
            .kv_api
            .upsert_kv(&key, match_seq, Some(value), None)
            .await?;
        match res.result {
            Some((s, _)) => Ok(Some(s)),
            None => Err(ErrorCode::NamespaceUnknownNode(format!(
                "unknown node, or seq not match {:?}",
                node
            ))),
        }
    }

    async fn drop_node(
        &self,
        tenant_id: String,
        namespace_id: String,
        node_id: String,
        seq: Option<u64>,
    ) -> Result<()> {
        let key = self.key_prefix(&[tenant_id, namespace_id, node_id.clone()]);
        let r = self.kv_api.upsert_kv(&key, seq.into(), None, None).await?;
        if r.prev.is_some() && r.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::NamespaceUnknownNode(format!(
                "unknown node {:?}",
                node_id
            )))
        }
    }
}
