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

use common_base::base::GlobalIORuntime;
use common_base::base::Runtime;
use common_base::base::SingletonImpl;
use common_catalog::catalog::CatalogManager;
use common_config::Config;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_storage::StorageOperator;
use common_tracing::QueryLogger;
use common_users::RoleCacheManager;
use common_users::UserApiProvider;
use opendal::Operator;

use crate::api::DataExchangeManager;
use crate::catalogs::CatalogManagerHelper;
use crate::clusters::ClusterDiscovery;
use crate::interpreters::AsyncInsertManager;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::SessionManager;

pub struct GlobalServices {
    global_runtime: UnsafeCell<Option<Arc<Runtime>>>,
    query_logger: UnsafeCell<Option<Arc<QueryLogger>>>,
    cluster_discovery: UnsafeCell<Option<Arc<ClusterDiscovery>>>,
    storage_operator: UnsafeCell<Option<Operator>>,
    async_insert_manager: UnsafeCell<Option<Arc<AsyncInsertManager>>>,
    cache_manager: UnsafeCell<Option<Arc<CacheManager>>>,
    catalog_manager: UnsafeCell<Option<Arc<CatalogManager>>>,
    http_query_manager: UnsafeCell<Option<Arc<HttpQueryManager>>>,
    data_exchange_manager: UnsafeCell<Option<Arc<DataExchangeManager>>>,
    session_manager: UnsafeCell<Option<Arc<SessionManager>>>,
    users_manager: UnsafeCell<Option<Arc<UserApiProvider>>>,
    users_role_manager: UnsafeCell<Option<Arc<RoleCacheManager>>>,
}

unsafe impl Send for GlobalServices {}

unsafe impl Sync for GlobalServices {}

impl GlobalServices {
    pub async fn init(config: Config) -> Result<()> {
        let global_services = Arc::new(GlobalServices {
            query_logger: UnsafeCell::new(None),
            cluster_discovery: UnsafeCell::new(None),
            storage_operator: UnsafeCell::new(None),
            async_insert_manager: UnsafeCell::new(None),
            cache_manager: UnsafeCell::new(None),
            catalog_manager: UnsafeCell::new(None),
            http_query_manager: UnsafeCell::new(None),
            data_exchange_manager: UnsafeCell::new(None),
            session_manager: UnsafeCell::new(None),
            users_manager: UnsafeCell::new(None),
            global_runtime: UnsafeCell::new(None),
            users_role_manager: UnsafeCell::new(None),
        });

        // The order of initialization is very important
        let app_name_shuffle = format!("{}-{}", config.query.tenant_id, config.query.cluster_id);

        QueryLogger::init(app_name_shuffle, &config.log, global_services.clone())?;
        GlobalIORuntime::init(config.query.num_cpus as usize, global_services.clone())?;

        // Cluster discovery.
        ClusterDiscovery::init(config.clone(), global_services.clone()).await?;

        StorageOperator::init(&config.storage, global_services.clone()).await?;
        AsyncInsertManager::init(&config, global_services.clone())?;
        CacheManager::init(&config.query, global_services.clone())?;
        CatalogManager::init(&config, global_services.clone()).await?;
        HttpQueryManager::init(&config, global_services.clone()).await?;
        DataExchangeManager::init(config.clone(), global_services.clone())?;
        SessionManager::init(config.clone(), global_services.clone())?;
        UserApiProvider::init(
            config.meta.to_meta_grpc_client_conf(),
            config.iam,
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

impl SingletonImpl<Operator> for GlobalServices {
    fn get(&self) -> Operator {
        unsafe {
            match &*self.storage_operator.get() {
                None => panic!("StorageOperator is not init"),
                Some(storage_operator) => storage_operator.clone(),
            }
        }
    }

    fn init(&self, value: Operator) -> Result<()> {
        unsafe {
            *(self.storage_operator.get() as *mut Option<Operator>) = Some(value);
            Ok(())
        }
    }
}

impl SingletonImpl<Arc<AsyncInsertManager>> for GlobalServices {
    fn get(&self) -> Arc<AsyncInsertManager> {
        unsafe {
            match &*self.async_insert_manager.get() {
                None => panic!("AsyncInsertManager is not init"),
                Some(async_insert_manager) => async_insert_manager.clone(),
            }
        }
    }

    fn init(&self, value: Arc<AsyncInsertManager>) -> Result<()> {
        unsafe {
            *(self.async_insert_manager.get() as *mut Option<Arc<AsyncInsertManager>>) =
                Some(value);
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
