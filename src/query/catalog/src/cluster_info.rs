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
use databend_meta_types::NodeInfo;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Cluster {
    pub unassign: bool,
    pub local_id: String,

    pub nodes: Vec<Arc<NodeInfo>>,
}

impl Cluster {
    /// If this cluster is empty?
    ///
    /// # Note
    ///
    /// Cluster empty means:
    ///
    /// - There is no active node (is this possible?).
    /// - There is only one node (myself).
    ///
    /// # TODO
    ///
    /// From @Xuanwo
    ///
    /// Ideally, we should implement a cluster trait to replace `ClusterHelper`
    /// defined in `databend-query`.
    pub fn is_empty(&self) -> bool {
        self.nodes.len() <= 1
    }

    pub fn get_cluster_id(&self) -> Result<String> {
        for node in &self.nodes {
            if node.id == self.local_id {
                return Ok(node.cluster_id.clone());
            }
        }

        Err(ErrorCode::Internal("Cannot found local node in cluster"))
    }

    pub fn get_warehouse_id(&self) -> Result<String> {
        for node in &self.nodes {
            if node.id == self.local_id {
                return Ok(node.warehouse_id.clone());
            }
        }

        Err(ErrorCode::Internal("Cannot found local node in cluster"))
    }
}
