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

use databend_common_exception::Result;
use databend_common_meta_app::principal::NetworkPolicy;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;

#[async_trait::async_trait]
pub trait NetworkPolicyApi: Sync + Send {
    async fn add_network_policy(
        &self,
        network_policy: NetworkPolicy,
        create_option: &CreateOption,
    ) -> Result<()>;

    async fn update_network_policy(
        &self,
        network_policy: NetworkPolicy,
        seq: MatchSeq,
    ) -> Result<u64>;

    async fn drop_network_policy(&self, name: &str, seq: MatchSeq) -> Result<()>;

    async fn get_network_policy(&self, name: &str, seq: MatchSeq) -> Result<SeqV<NetworkPolicy>>;

    async fn get_network_policies(&self) -> Result<Vec<NetworkPolicy>>;
}
