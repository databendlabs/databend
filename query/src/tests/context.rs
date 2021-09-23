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

use std::env;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_runtime::tokio::runtime::Runtime;

use crate::clusters::{ClusterDiscovery, Cluster};
use crate::configs::Config;
use crate::sessions::{DatabendQueryContextRef, DatabendQueryContext, DatabendQueryContextShared};
use crate::sessions::SessionManager;
use std::sync::Arc;
use common_management::NodeInfo;
use crate::tests::SessionManagerBuilder;

pub fn try_create_context() -> Result<DatabendQueryContextRef> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    Ok(DatabendQueryContext::from_shared(DatabendQueryContextShared::try_create(
        sessions.get_conf().clone(),
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::empty(),
    )))
}

pub fn try_create_context_with_config(config: Config) -> Result<DatabendQueryContextRef> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    Ok(DatabendQueryContext::from_shared(DatabendQueryContextShared::try_create(
        config,
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::empty(),
    )))
}

pub struct ClusterDescriptor {
    local_node_id: String,
    cluster_nodes_list: Vec<Arc<NodeInfo>>,
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
        new_nodes.push(Arc::new(NodeInfo::create(id.into(), 0, addr.into())));
        ClusterDescriptor {
            cluster_nodes_list: new_nodes,
            local_node_id: self.local_node_id.clone(),
        }
    }

    pub fn with_local_id(self, id: impl Into<String>) -> ClusterDescriptor {
        ClusterDescriptor {
            local_node_id: id.into(),
            cluster_nodes_list: self.cluster_nodes_list.clone(),
        }
    }
}

pub fn try_create_cluster_context(desc: ClusterDescriptor) -> Result<DatabendQueryContextRef> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    let local_id = desc.local_node_id;
    let nodes = desc.cluster_nodes_list.clone();

    Ok(DatabendQueryContext::from_shared(DatabendQueryContextShared::try_create(
        sessions.get_conf().clone(),
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::create(nodes, local_id),
    )))
}
