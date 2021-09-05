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

use std::convert::TryFrom;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metatypes::SeqValue;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeInfo {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub cpu_nums: u32,
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub ip: String,
    #[serde(default)]
    pub port: u32,
}

impl TryFrom<Vec<u8>> for NodeInfo {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(user_info) => Ok(user_info),
            Err(serialize_error) => Err(ErrorCode::IllegalUserInfoFormat(format!(
                "Cannot deserialize namespace from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}

#[async_trait]
pub trait NamespaceApi {
    // Add a new node info to /tenant/namespace/node-name.
    async fn add_node(
        &mut self,
        tenant_id: String,
        namespace_id: String,
        node: NodeInfo,
    ) -> Result<u64>;

    // Get the tenant's namespace all nodes.
    async fn get_nodes(
        &mut self,
        tenant_id: String,
        namespace_id: String,
        seq: Option<u64>,
    ) -> Result<Vec<SeqValue<NodeInfo>>>;

    // Update the tenant's namespace node.
    async fn update_node(
        &mut self,
        tenant_id: String,
        namespace_id: String,
        node: NodeInfo,
        seq: Option<u64>,
    ) -> Result<Option<u64>>;

    // Drop the tenant's namespace one node by node.id.
    async fn drop_node(
        &mut self,
        tenant_id: String,
        namespace_id: String,
        node: NodeInfo,
        seq: Option<u64>,
    ) -> Result<()>;
}
