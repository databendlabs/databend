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

use common_exception::Result;
use common_metatypes::NodeInfo;

#[async_trait::async_trait]
pub trait NamespaceApi: Sync + Send {
    // Add a new node info to /tenant/namespace/node-name.
    async fn add_node(&self, node: NodeInfo) -> Result<u64>;

    // Get the tenant's namespace all nodes.
    async fn get_nodes(&self) -> Result<Vec<NodeInfo>>;

    // Drop the tenant's namespace one node by node.id.
    async fn drop_node(&self, node_id: String, seq: Option<u64>) -> Result<()>;

    // Keep the tenant's namespace node alive.
    async fn heartbeat(&self, node_id: String, seq: Option<u64>) -> Result<u64>;
}
