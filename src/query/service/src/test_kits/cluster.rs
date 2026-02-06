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

use databend_common_version::DATABEND_COMMIT_VERSION;
use databend_meta_types::NodeInfo;

/// A descriptor for a cluster.
#[derive(Clone)]
pub struct ClusterDescriptor {
    pub(crate) local_node_id: String,
    pub(crate) cluster_nodes_list: Vec<Arc<NodeInfo>>,
}

impl ClusterDescriptor {
    pub fn new() -> ClusterDescriptor {
        ClusterDescriptor {
            local_node_id: String::from(""),
            cluster_nodes_list: vec![],
        }
    }

    pub fn with_node(self, id: impl Into<String>, addr: impl Into<String>) -> ClusterDescriptor {
        let mut new_nodes = self.cluster_nodes_list.clone();
        let id = id.into();
        new_nodes.push(Arc::new(NodeInfo::create(
            id.clone(),
            "".to_string(),
            "".to_string(),
            addr.into(),
            "".to_string(),
            DATABEND_COMMIT_VERSION.to_string(),
            id,
        )));
        ClusterDescriptor {
            cluster_nodes_list: new_nodes,
            local_node_id: self.local_node_id,
        }
    }

    pub fn with_local_id(self, id: impl Into<String>) -> ClusterDescriptor {
        ClusterDescriptor {
            local_node_id: id.into(),
            cluster_nodes_list: self.cluster_nodes_list,
        }
    }
}

impl Default for ClusterDescriptor {
    fn default() -> Self {
        Self::new()
    }
}
