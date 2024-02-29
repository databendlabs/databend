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

use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::NetworkPolicyApi;
use databend_common_meta_app::principal::NetworkPolicy;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_types::MatchSeq;

use crate::UserApiProvider;

impl UserApiProvider {
    // Add a new network policy.
    #[async_backtrace::framed]
    pub async fn add_network_policy(
        &self,
        tenant: &str,
        network_policy: NetworkPolicy,
        create_option: &CreateOption,
    ) -> Result<()> {
        let client = self.get_network_policy_api_client(tenant)?;
        client
            .add_network_policy(network_policy, create_option)
            .await
    }

    // Update network policy.
    #[async_backtrace::framed]
    pub async fn update_network_policy(
        &self,
        tenant: &str,
        name: &str,
        allowed_ip_list: Option<Vec<String>>,
        blocked_ip_list: Option<Vec<String>>,
        comment: Option<String>,
        if_exists: bool,
    ) -> Result<Option<u64>> {
        let client = self.get_network_policy_api_client(tenant)?;
        let seq_network_policy = match client.get_network_policy(name, MatchSeq::GE(0)).await {
            Ok(seq_network_policy) => seq_network_policy,
            Err(e) => {
                if if_exists && e.code() == ErrorCode::UNKNOWN_NETWORK_POLICY {
                    return Ok(None);
                } else {
                    return Err(e.add_message_back(" (while alter network policy)"));
                }
            }
        };

        let seq = seq_network_policy.seq;
        let mut network_policy = seq_network_policy.data;
        if let Some(allowed_ip_list) = allowed_ip_list {
            network_policy.allowed_ip_list = allowed_ip_list;
        }
        if let Some(blocked_ip_list) = blocked_ip_list {
            network_policy.blocked_ip_list = blocked_ip_list;
        }
        if let Some(comment) = comment {
            network_policy.comment = comment;
        }
        network_policy.update_on = Some(Utc::now());

        match client
            .update_network_policy(network_policy, MatchSeq::Exact(seq))
            .await
        {
            Ok(res) => Ok(Some(res)),
            Err(e) => Err(e.add_message_back(" (while alter network policy).")),
        }
    }

    // Drop a network policy by name.
    #[async_backtrace::framed]
    pub async fn drop_network_policy(
        &self,
        tenant: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let user_infos = self.get_users(tenant).await?;
        for user_info in user_infos {
            if let Some(network_policy) = user_info.option.network_policy() {
                if network_policy == name {
                    return Err(ErrorCode::NetworkPolicyIsUsedByUser(format!(
                        "network policy `{}` is used by user",
                        name,
                    )));
                }
            }
        }

        let client = self.get_network_policy_api_client(tenant)?;
        match client.drop_network_policy(name, MatchSeq::GE(1)).await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists && e.code() == ErrorCode::UNKNOWN_NETWORK_POLICY {
                    Ok(())
                } else {
                    Err(e.add_message_back(" (while drop network policy)"))
                }
            }
        }
    }

    // Check whether a network policy is exist.
    #[async_backtrace::framed]
    pub async fn exists_network_policy(&self, tenant: &str, name: &str) -> Result<bool> {
        match self.get_network_policy(tenant, name).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_NETWORK_POLICY {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    // Get a network_policy by tenant.
    #[async_backtrace::framed]
    pub async fn get_network_policy(&self, tenant: &str, name: &str) -> Result<NetworkPolicy> {
        let client = self.get_network_policy_api_client(tenant)?;
        let network_policy = client.get_network_policy(name, MatchSeq::GE(0)).await?.data;
        Ok(network_policy)
    }

    // Get all network policies by tenant.
    #[async_backtrace::framed]
    pub async fn get_network_policies(&self, tenant: &str) -> Result<Vec<NetworkPolicy>> {
        let client = self.get_network_policy_api_client(tenant)?;
        let network_policies = client
            .get_network_policies()
            .await
            .map_err(|e| e.add_message_back(" (while get network policies)."))?;
        Ok(network_policies)
    }
}
