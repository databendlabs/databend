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

use std::sync::Arc;

use common_exception::Result;
use common_meta_embedded::MetaEmbedded;
use common_meta_types::NodeInfo;
use databend_query::clusters::Cluster;
use databend_query::configs::Config;
use databend_query::datasources::DataSourceContext;
use databend_query::datasources::DatabaseEngineRegistry;
use databend_query::datasources::TableEngineRegistry;
use databend_query::sessions::DatabendQueryContext;
use databend_query::sessions::DatabendQueryContextRef;
use databend_query::sessions::DatabendQueryContextShared;

use crate::tests::SessionManagerBuilder;

pub fn try_create_context() -> Result<DatabendQueryContextRef> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    let context = DatabendQueryContext::from_shared(DatabendQueryContextShared::try_create(
        sessions.get_conf().clone(),
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::empty(),
    ));

    context.get_settings().set_max_threads(8)?;
    Ok(context)
}

pub fn try_create_context_with_config(config: Config) -> Result<DatabendQueryContextRef> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    let context = DatabendQueryContext::from_shared(DatabendQueryContextShared::try_create(
        config,
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::empty(),
    ));

    context.get_settings().set_max_threads(8)?;
    Ok(context)
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

pub fn try_create_cluster_context(desc: ClusterDescriptor) -> Result<DatabendQueryContextRef> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    let local_id = desc.local_node_id;
    let nodes = desc.cluster_nodes_list;

    let context = DatabendQueryContext::from_shared(DatabendQueryContextShared::try_create(
        sessions.get_conf().clone(),
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::create(nodes, local_id),
    ));

    context.get_settings().set_max_threads(8)?;
    Ok(context)
}

pub fn try_create_datasource_context() -> Result<DataSourceContext> {
    let meta_embedded = MetaEmbedded::sync_new_temp().unwrap();
    let meta = Arc::new(meta_embedded);
    let table_engine_registry = Arc::new(TableEngineRegistry::default());
    let database_engine_registry = Arc::new(DatabaseEngineRegistry::default());

    Ok(DataSourceContext {
        meta,
        table_engine_registry,
        database_engine_registry,
        in_memory_data: Arc::new(Default::default()),
    })
}
