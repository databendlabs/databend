// Copyright 2022 Datafuse Labs.
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

use std::cell::UnsafeCell;
use std::sync::Arc;

use base64::encode_config;
use base64::URL_SAFE;
use common_base::base::GlobalIORuntime;
use common_base::base::Runtime;
use common_base::base::SingletonImpl;
use common_catalog::catalog::CatalogManager;
use common_config::Config;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_storage::CacheOperator;
use common_storage::DataOperator;
use common_storage::ShareTableConfig;
use common_tracing::QueryLogger;
use common_users::RoleCacheManager;
use common_users::UserApiProvider;

use crate::api::DataExchangeManager;
use crate::catalogs::CatalogManagerHelper;
use crate::clusters::ClusterDiscovery;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::SessionManager;

pub struct GlobalServices {
    global_runtime: UnsafeCell<Option<Arc<Runtime>>>,
    query_logger: UnsafeCell<Option<Arc<QueryLogger>>>,
    cluster_discovery: UnsafeCell<Option<Arc<ClusterDiscovery>>>,
    storage_operator: UnsafeCell<Option<DataOperator>>,
    cache_operator: UnsafeCell<Option<CacheOperator>>,
    cache_manager: UnsafeCell<Option<Arc<CacheManager>>>,
    catalog_manager: UnsafeCell<Option<Arc<CatalogManager>>>,
    http_query_manager: UnsafeCell<Option<Arc<HttpQueryManager>>>,
    data_exchange_manager: UnsafeCell<Option<Arc<DataExchangeManager>>>,
    session_manager: UnsafeCell<Option<Arc<SessionManager>>>,
    users_manager: UnsafeCell<Option<Arc<UserApiProvider>>>,
    users_role_manager: UnsafeCell<Option<Arc<RoleCacheManager>>>,
    share_table_config: UnsafeCell<Option<ShareTableConfig>>,
}

unsafe impl Send for GlobalServices {}

unsafe impl Sync for GlobalServices {}

impl GlobalServices {
    pub async fn init(config: Config) -> Result<()> {
        let global_services = Arc::new(GlobalServices {
            query_logger: UnsafeCell::new(None),
            cluster_discovery: UnsafeCell::new(None),
            storage_operator: UnsafeCell::new(None),
            cache_operator: UnsafeCell::new(None),
            cache_manager: UnsafeCell::new(None),
            catalog_manager: UnsafeCell::new(None),
            http_query_manager: UnsafeCell::new(None),
            data_exchange_manager: UnsafeCell::new(None),
            session_manager: UnsafeCell::new(None),
            users_manager: UnsafeCell::new(None),
            global_runtime: UnsafeCell::new(None),
            users_role_manager: UnsafeCell::new(None),
            share_table_config: UnsafeCell::new(None),
        });

        // The order of initialization is very important
        let app_name_shuffle = format!("{}-{}", config.query.tenant_id, config.query.cluster_id);

        QueryLogger::init(app_name_shuffle, &config.log, global_services.clone())?;
        GlobalIORuntime::init(config.storage.num_cpus as usize, global_services.clone())?;

        // Cluster discovery.
        ClusterDiscovery::init(config.clone(), global_services.clone()).await?;

        DataOperator::init(&config.storage, global_services.clone()).await?;
        CacheOperator::init(&config.cache, global_services.clone()).await?;

        ShareTableConfig::init(
            &config.query.share_endpoint_address,
            &config.query.share_endpoint_auth_token_file,
            encode_config(config.query.tenant_id.clone(), URL_SAFE),
            global_services.clone(),
        )?;

        CacheManager::init(&config.query, global_services.clone())?;
        CatalogManager::init(&config, global_services.clone()).await?;
        HttpQueryManager::init(&config, global_services.clone()).await?;
        DataExchangeManager::init(config.clone(), global_services.clone())?;
        SessionManager::init(config.clone(), global_services.clone())?;
        UserApiProvider::init(
            config.meta.to_meta_grpc_client_conf(),
            config.query.idm,
            global_services.clone(),
        )
        .await?;
        RoleCacheManager::init(global_services.clone())
    }
}

impl SingletonImpl<Arc<Runtime>> for GlobalServices {
    fn get(&self) -> Arc<Runtime> {
        unsafe {
            match &*self.global_runtime.get() {
                None => panic!("GlobalRuntime is not init"),
                Some(global_runtime) => global_runtime.clone(),
            }
        }
    }

    fn init(&self, value: Arc<Runtime>) -> Result<()> {
        unsafe {
            *(self.global_runtime.get() as *mut Option<Arc<Runtime>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<QueryLogger>> for GlobalServices {
    fn get(&self) -> Arc<QueryLogger> {
        unsafe {
            match &*self.query_logger.get() {
                None => panic!("QueryLogger is not init"),
                Some(query_logger) => query_logger.clone(),
            }
        }
    }

    fn init(&self, value: Arc<QueryLogger>) -> Result<()> {
        unsafe {
            *(self.query_logger.get() as *mut Option<Arc<QueryLogger>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<ClusterDiscovery>> for GlobalServices {
    fn get(&self) -> Arc<ClusterDiscovery> {
        unsafe {
            match &*self.cluster_discovery.get() {
                None => panic!("ClusterDiscovery is not init"),
                Some(cluster_discovery) => cluster_discovery.clone(),
            }
        }
    }

    fn init(&self, value: Arc<ClusterDiscovery>) -> Result<()> {
        unsafe {
            *(self.cluster_discovery.get() as *mut Option<Arc<ClusterDiscovery>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<DataOperator> for GlobalServices {
    fn get(&self) -> DataOperator {
        unsafe {
            match &*self.storage_operator.get() {
                None => panic!("StorageOperator is not init"),
                Some(storage_operator) => storage_operator.clone(),
            }
        }
    }

    fn init(&self, value: DataOperator) -> Result<()> {
        unsafe {
            *(self.storage_operator.get() as *mut Option<DataOperator>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<CacheOperator> for GlobalServices {
    fn get(&self) -> CacheOperator {
        unsafe {
            match &*self.cache_operator.get() {
                None => panic!("CacheOperator is not init"),
                Some(op) => op.clone(),
            }
        }
    }

    fn init(&self, value: CacheOperator) -> Result<()> {
        unsafe {
            *(self.cache_operator.get() as *mut Option<CacheOperator>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<CacheManager>> for GlobalServices {
    fn get(&self) -> Arc<CacheManager> {
        unsafe {
            match &*self.cache_manager.get() {
                None => panic!("CacheManager is not init"),
                Some(cache_manager) => cache_manager.clone(),
            }
        }
    }

    fn init(&self, value: Arc<CacheManager>) -> Result<()> {
        unsafe {
            *(self.cache_manager.get() as *mut Option<Arc<CacheManager>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<CatalogManager>> for GlobalServices {
    fn get(&self) -> Arc<CatalogManager> {
        unsafe {
            match &*self.catalog_manager.get() {
                None => panic!("CatalogManager is not init"),
                Some(catalog_manager) => catalog_manager.clone(),
            }
        }
    }

    fn init(&self, value: Arc<CatalogManager>) -> Result<()> {
        unsafe {
            *(self.catalog_manager.get() as *mut Option<Arc<CatalogManager>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<HttpQueryManager>> for GlobalServices {
    fn get(&self) -> Arc<HttpQueryManager> {
        unsafe {
            match &*self.http_query_manager.get() {
                None => panic!("HttpQueryManager is not init"),
                Some(http_query_manager) => http_query_manager.clone(),
            }
        }
    }

    fn init(&self, value: Arc<HttpQueryManager>) -> Result<()> {
        unsafe {
            *(self.http_query_manager.get() as *mut Option<Arc<HttpQueryManager>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<DataExchangeManager>> for GlobalServices {
    fn get(&self) -> Arc<DataExchangeManager> {
        unsafe {
            match &*self.data_exchange_manager.get() {
                None => panic!("DataExchangeManager is not init"),
                Some(data_exchange_manager) => data_exchange_manager.clone(),
            }
        }
    }

    fn init(&self, value: Arc<DataExchangeManager>) -> Result<()> {
        unsafe {
            *(self.data_exchange_manager.get() as *mut Option<Arc<DataExchangeManager>>) =
                Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<SessionManager>> for GlobalServices {
    fn get(&self) -> Arc<SessionManager> {
        unsafe {
            match &*self.session_manager.get() {
                None => panic!("SessionManager is not init"),
                Some(session_manager) => session_manager.clone(),
            }
        }
    }

    fn init(&self, value: Arc<SessionManager>) -> Result<()> {
        unsafe {
            *(self.session_manager.get() as *mut Option<Arc<SessionManager>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<UserApiProvider>> for GlobalServices {
    fn get(&self) -> Arc<UserApiProvider> {
        unsafe {
            match &*self.users_manager.get() {
                None => panic!("UserApiProvider is not init"),
                Some(users_manager) => users_manager.clone(),
            }
        }
    }

    fn init(&self, value: Arc<UserApiProvider>) -> Result<()> {
        unsafe {
            *(self.users_manager.get() as *mut Option<Arc<UserApiProvider>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<RoleCacheManager>> for GlobalServices {
    fn get(&self) -> Arc<RoleCacheManager> {
        unsafe {
            match &*self.users_role_manager.get() {
                None => panic!("RoleCacheManager is not init"),
                Some(users_role_manager) => users_role_manager.clone(),
            }
        }
    }

    fn init(&self, value: Arc<RoleCacheManager>) -> Result<()> {
        unsafe {
            *(self.users_role_manager.get() as *mut Option<Arc<RoleCacheManager>>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<ShareTableConfig> for GlobalServices {
    fn get(&self) -> ShareTableConfig {
        unsafe {
            match &*self.share_table_config.get() {
                None => panic!("ShareTableConfig is not init"),
                Some(share_table_config) => share_table_config.clone(),
            }
        }
    }

    fn init(&self, value: ShareTableConfig) -> Result<()> {
        unsafe {
            *(self.share_table_config.get() as *mut Option<ShareTableConfig>) = Some(value);
            Ok(())
        }
    }
}
