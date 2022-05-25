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

use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use common_base::base::escape_for_key;
use common_base::base::unescape_for_key;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::KVApi;
use common_meta_types::KVMeta;
use common_meta_types::MatchSeq;
use common_meta_types::NodeInfo;
use common_meta_types::OkOrExist;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVReply;
use common_meta_types::UpsertKVReq;

use crate::cluster::ClusterApi;

pub static CLUSTER_API_KEY_PREFIX: &str = "__fd_clusters";

pub struct ClusterMgr {
    kv_api: Arc<dyn KVApi>,
    lift_time: Duration,
    cluster_prefix: String,
}

impl ClusterMgr {
    pub fn create(
        kv_api: Arc<dyn KVApi>,
        tenant: &str,
        cluster_id: &str,
        lift_time: Duration,
    ) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while cluster mgr create)",
            ));
        }

        Ok(ClusterMgr {
            kv_api,
            lift_time,
            cluster_prefix: format!(
                "{}/{}/{}/databend_query",
                CLUSTER_API_KEY_PREFIX,
                escape_for_key(tenant)?,
                escape_for_key(cluster_id)?
            ),
        })
    }

    fn new_lift_time(&self) -> KVMeta {
        let now = std::time::SystemTime::now();
        let expire_at = now
            .add(self.lift_time)
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        KVMeta {
            expire_at: Some(expire_at.as_secs()),
        }
    }
}

#[async_trait::async_trait]
impl ClusterApi for ClusterMgr {
    async fn add_node(&self, node: NodeInfo) -> Result<u64> {
        // Only when there are no record, i.e. seq=0
        let seq = MatchSeq::Exact(0);
        let meta = Some(self.new_lift_time());
        let value = Operation::Update(serde_json::to_vec(&node)?);
        let node_key = format!("{}/{}", self.cluster_prefix, escape_for_key(&node.id)?);
        let upsert_node = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&node_key, seq, value, meta));

        let res = upsert_node.await?.into_add_result()?;

        match res.res {
            OkOrExist::Ok(v) => Ok(v.seq),
            OkOrExist::Exists(v) => Err(ErrorCode::ClusterNodeAlreadyExists(format!(
                "Cluster ID already exists, seq [{}]",
                v.seq
            ))),
        }
    }

    async fn get_nodes(&self) -> Result<Vec<NodeInfo>> {
        let values = self.kv_api.prefix_list_kv(&self.cluster_prefix).await?;

        let mut nodes_info = Vec::with_capacity(values.len());
        for (node_key, value) in values {
            let mut node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;

            let node_key = unescape_for_key(&node_key)?;
            node_info.id = node_key[self.cluster_prefix.len() + 1..].to_string();
            nodes_info.push(node_info);
        }

        Ok(nodes_info)
    }

    async fn drop_node(&self, node_id: String, seq: Option<u64>) -> Result<()> {
        let node_key = format!("{}/{}", self.cluster_prefix, escape_for_key(&node_id)?);
        let upsert_node = self.kv_api.upsert_kv(UpsertKVReq::new(
            &node_key,
            seq.into(),
            Operation::Delete,
            None,
        ));

        match upsert_node.await? {
            UpsertKVReply {
                ident: None,
                prev: Some(_),
                result: None,
            } => Ok(()),
            UpsertKVReply { .. } => Err(ErrorCode::ClusterUnknownNode(format!(
                "unknown node {:?}",
                node_id
            ))),
        }
    }

    async fn heartbeat(&self, node: &NodeInfo, seq: Option<u64>) -> Result<u64> {
        let meta = Some(self.new_lift_time());
        let node_key = format!("{}/{}", self.cluster_prefix, escape_for_key(&node.id)?);
        let seq = match seq {
            None => MatchSeq::GE(1),
            Some(exact) => MatchSeq::Exact(exact),
        };

        let upsert_meta =
            self.kv_api
                .upsert_kv(UpsertKVReq::new(&node_key, seq, Operation::AsIs, meta));

        match upsert_meta.await? {
            UpsertKVReply {
                ident: None,
                prev: Some(_),
                result: Some(SeqV { seq: s, .. }),
            } => Ok(s),
            UpsertKVReply { .. } => self.add_node(node.clone()).await,
        }
    }
}
