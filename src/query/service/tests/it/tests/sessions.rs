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

use std::time::Duration;

use common_base::base::Global;
use common_base::base::GlobalIORuntime;
use common_catalog::catalog::CatalogManager;
use common_config::Config;
use common_config::GlobalConfig;
use common_exception::Result;
use common_storage::CacheOperator;
use common_storage::DataOperator;
use common_storages_table_meta::caches::CacheManager;
use common_tracing::set_panic_hook;
use common_tracing::QueryLogger;
use common_users::RoleCacheManager;
use common_users::UserApiProvider;
use databend_query::api::DataExchangeManager;
use databend_query::catalogs::CatalogManagerHelper;
use databend_query::clusters::ClusterDiscovery;
use databend_query::servers::http::v1::HttpQueryManager;
use databend_query::sessions::SessionManager;
use time::Instant;

pub struct TestGlobalServices;

unsafe impl Send for TestGlobalServices {}

unsafe impl Sync for TestGlobalServices {}

impl TestGlobalServices {
    pub async fn setup(config: Config) -> Result<TestGuard> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");
        Global::init_testing();

        // The order of initialization is very important
        GlobalConfig::init(config.clone())?;

        let app_name_shuffle = format!("{}-{}", config.query.tenant_id, config.query.cluster_id);

        QueryLogger::init(app_name_shuffle, &config.log)?;
        GlobalIORuntime::init(config.query.num_cpus as usize)?;

        // Cluster discovery.
        ClusterDiscovery::init(config.clone()).await?;

        DataOperator::init(&config.storage).await?;
        CacheOperator::init(&config.storage.cache).await?;
        CacheManager::init(&config.query)?;
        CatalogManager::init(&config).await?;
        HttpQueryManager::init(&config).await?;
        DataExchangeManager::init()?;
        SessionManager::init(&config)?;
        UserApiProvider::init(
            config.meta.to_meta_grpc_client_conf(),
            config.query.idm.clone(),
            config.query.tenant_id.clone().as_str(),
            config.query.tenant_quota.clone(),
        )
        .await?;
        RoleCacheManager::init()?;

        ClusterDiscovery::instance()
            .register_to_metastore(&config)
            .await?;

        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(thread_name) => Ok(TestGuard {
                thread_name: thread_name.to_string(),
            }),
        }
    }
}

pub struct TestGuard {
    thread_name: String,
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        // Hack: The session may be referenced by other threads. Let's try to wait.
        let now = Instant::now();
        while !SessionManager::instance().processes_info().is_empty() {
            std::thread::sleep(Duration::from_millis(500));

            if now.elapsed() > Duration::from_secs(3) {
                break;
            }
        }

        Global::drop_testing(&self.thread_name)
    }
}
