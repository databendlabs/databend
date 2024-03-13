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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_app::principal::NetworkPolicy;
use databend_common_meta_app::principal::NetworkPolicyIdent;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::NonEmptyString;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::With;
use futures::TryStreamExt;

use crate::network_policy::network_policy_api::NetworkPolicyApi;

pub struct NetworkPolicyMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
}

impl NetworkPolicyMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
        tenant: &NonEmptyString,
    ) -> Self {
        NetworkPolicyMgr {
            kv_api,
            tenant: Tenant::new(tenant.as_str()),
        }
    }

    fn ident(&self, name: &str) -> NetworkPolicyIdent {
        NetworkPolicyIdent::new(self.tenant.clone(), name)
    }
}

#[async_trait::async_trait]
impl NetworkPolicyApi for NetworkPolicyMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add_network_policy(
        &self,
        network_policy: NetworkPolicy,
        create_option: &CreateOption,
    ) -> Result<()> {
        let ident = self.ident(network_policy.name.as_str());

        let seq = MatchSeq::from(*create_option);
        let upsert = UpsertPB::insert(ident, network_policy.clone()).with(seq);

        let res = self.kv_api.upsert_pb(&upsert).await?;

        if let CreateOption::None = create_option {
            if res.prev.is_some() {
                return Err(ErrorCode::NetworkPolicyAlreadyExists(format!(
                    "Network policy '{}' already exists.",
                    network_policy.name
                )));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn update_network_policy(
        &self,
        network_policy: NetworkPolicy,
        match_seq: MatchSeq,
    ) -> Result<u64> {
        let ident = self.ident(network_policy.name.as_str());
        let upsert = UpsertPB::update(ident, network_policy.clone()).with(match_seq);

        let res = self.kv_api.upsert_pb(&upsert).await?;

        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownNetworkPolicy(format!(
                "Network policy '{}' cannot be updated as it may not exist or the request is invalid.",
                network_policy.name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn drop_network_policy(&self, name: &str, seq: MatchSeq) -> Result<()> {
        let ident = self.ident(name);

        let upsert = UpsertPB::delete(ident).with(seq);

        let res = self.kv_api.upsert_pb(&upsert).await?;
        res.removed_or_else(|e| {
            ErrorCode::UnknownNetworkPolicy(format!(
                "Network policy '{}' does not exist. {:?}",
                name, e
            ))
        })?;

        Ok(())
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_network_policy(&self, name: &str, seq: MatchSeq) -> Result<SeqV<NetworkPolicy>> {
        let ident = self.ident(name);

        let res = self.kv_api.get_pb(&ident).await?;

        let seq_value = res.ok_or_else(|| {
            ErrorCode::UnknownNetworkPolicy(format!("Network policy '{}' does not exist.", name))
        })?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(seq_value),
            Err(_) => Err(ErrorCode::UnknownNetworkPolicy(format!(
                "Network policy '{}' does not exist.",
                name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_network_policies(&self) -> Result<Vec<NetworkPolicy>> {
        let dir_name = DirName::new(self.ident("dummy"));

        let values = self.kv_api.list_pb_values(&dir_name).await?;
        let values = values.try_collect().await?;

        Ok(values)
    }
}
