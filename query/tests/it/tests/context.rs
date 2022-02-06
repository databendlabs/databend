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

use std::sync::Arc;

use common_exception::Result;
use common_meta_embedded::MetaEmbedded;
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::NodeInfo;
use common_meta_types::PasswordHashMethod;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilegeSet;
use databend_query::catalogs::CatalogContext;
use databend_query::clusters::Cluster;
use databend_query::configs::Config;
use databend_query::databases::DatabaseFactory;
use databend_query::sessions::QueryContext;
use databend_query::sessions::QueryContextShared;
use databend_query::storages::StorageContext;
use databend_query::storages::StorageFactory;

use crate::tests::SessionManagerBuilder;

pub fn create_query_context() -> Result<Arc<QueryContext>> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    // Set user with all privileges
    let mut user_info = UserInfo::new(
        "root".to_string(),
        "127.0.0.1".to_string(),
        AuthInfo::Password {
            hash_method: PasswordHashMethod::Sha256,
            hash_value: Vec::from("pass"),
        },
    );
    user_info.grants.grant_privileges(
        "root",
        "127.0.0.1",
        &GrantObject::Global,
        UserPrivilegeSet::available_privileges_on_global(),
    );

    dummy_session.set_current_user(user_info);

    let context = QueryContext::create_from_shared(QueryContextShared::try_create(
        sessions.get_conf().clone(),
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::empty(),
    )?);

    context.get_settings().set_max_threads(8)?;
    Ok(context)
}

pub fn create_query_context_with_config(config: Config) -> Result<Arc<QueryContext>> {
    let sessions = SessionManagerBuilder::create_with_conf(config.clone()).build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    let mut user_info = UserInfo::new(
        "root".to_string(),
        "127.0.0.1".to_string(),
        AuthInfo::Password {
            hash_method: PasswordHashMethod::Sha256,
            hash_value: Vec::from("pass"),
        },
    );
    user_info.grants.grant_privileges(
        "root",
        "127.0.0.1",
        &GrantObject::Global,
        UserPrivilegeSet::available_privileges_on_global(),
    );
    dummy_session.set_current_user(user_info);

    let context = QueryContext::create_from_shared(QueryContextShared::try_create(
        config,
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::empty(),
    )?);

    context.get_settings().set_max_threads(8)?;
    Ok(context)
}

#[allow(dead_code)]
pub fn create_catalog_context() -> Result<CatalogContext> {
    let meta_embedded = futures::executor::block_on(MetaEmbedded::new_temp()).unwrap();
    let meta = meta_embedded;
    let storage_factory = StorageFactory::create(Config::default());
    let database_factory = DatabaseFactory::create(Config::default());

    Ok(CatalogContext {
        meta: Arc::new(meta),
        storage_factory: Arc::new(storage_factory),
        database_factory: Arc::new(database_factory),
        in_memory_data: Arc::new(Default::default()),
    })
}

pub fn create_storage_context() -> Result<StorageContext> {
    let meta_embedded = futures::executor::block_on(MetaEmbedded::new_temp()).unwrap();

    Ok(StorageContext {
        meta: Arc::new(meta_embedded),
        in_memory_data: Arc::new(Default::default()),
    })
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

pub fn create_query_context_with_cluster(desc: ClusterDescriptor) -> Result<Arc<QueryContext>> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dummy_session = sessions.create_session("TestSession")?;

    let local_id = desc.local_node_id;
    let nodes = desc.cluster_nodes_list;

    let context = QueryContext::create_from_shared(QueryContextShared::try_create(
        sessions.get_conf().clone(),
        Arc::new(dummy_session.as_ref().clone()),
        Cluster::create(nodes, local_id),
    )?);

    context.get_settings().set_max_threads(8)?;
    Ok(context)
}
