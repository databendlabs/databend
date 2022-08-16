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
use once_cell::sync::OnceCell;

use common_base::base::tokio::runtime::Runtime;
use common_base::base::{GlobalIORuntime, Thread};
use common_catalog::catalog::CatalogManager;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_storage::StorageOperator;
use common_tracing::QueryLogger;
use common_users::{RoleCacheManager, UserApiProvider};
use databend_query::api::DataExchangeManager;
use databend_query::catalogs::CatalogManagerHelper;
use databend_query::clusters::ClusterDiscovery;
use databend_query::sessions::SessionManager;
use databend_query::Config;
use databend_query::interpreters::AsyncInsertManager;
use databend_query::servers::http::v1::HttpQueryManager;

async fn async_create_sessions(config: Config) -> Result<Arc<SessionManager>> {
    ClusterDiscovery::init(config.clone()).await?;
    SessionManager::init(config.clone())?;

    let cluster_discovery = ClusterDiscovery::instance();
    cluster_discovery.register_to_metastore(&config).await?;
    Ok(SessionManager::instance())
}

fn sync_create_sessions(config: Config) -> Result<Arc<SessionManager>> {
    let runtime = Runtime::new()?;
    runtime.block_on(async_create_sessions(config))
}

pub struct GlobalServices {
    config: Config,
}

static INITIALIZED: OnceCell<bool> = OnceCell::new();

impl GlobalServices {
    pub async fn setup(config: Config) -> Result<()> {
        if let Some(_) = INITIALIZED.get() {
            return Ok(());
        }

        INITIALIZED.set(true).unwrap();
        // The order of initialization is very important
        let app_name_shuffle = format!("{}-{}", config.query.tenant_id, config.query.cluster_id);

        QueryLogger::init(app_name_shuffle, &config.log)?;
        GlobalIORuntime::init(config.query.num_cpus as usize)?;

        // Cluster discovery.
        ClusterDiscovery::init(config.clone()).await?;
        ClusterDiscovery::instance().register_to_metastore(&config).await?;

        StorageOperator::init(&config.storage).await?;
        AsyncInsertManager::init(&config)?;
        CacheManager::init(&config.query)?;
        CatalogManager::init(&config).await?;
        HttpQueryManager::init(&config).await?;
        DataExchangeManager::init(config.clone())?;
        SessionManager::init(config.clone())?;
        UserApiProvider::init(config.meta.to_meta_grpc_client_conf()).await?;
        RoleCacheManager::init()
    }

    pub fn create_with_conf(config: Config) -> GlobalServices {
        GlobalServices { config }
    }
}
