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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_base::base::Runtime;
use common_base::base::SingletonImpl;
use common_catalog::catalog::CatalogManager;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_storage::StorageOperator;
use common_tracing::set_panic_hook;
use common_tracing::QueryLogger;
use common_users::RoleCacheManager;
use common_users::UserApiProvider;
use databend_query::api::DataExchangeManager;
use databend_query::catalogs::CatalogManagerHelper;
use databend_query::clusters::ClusterDiscovery;
use databend_query::interpreters::AsyncInsertManager;
use databend_query::servers::http::v1::HttpQueryManager;
use databend_query::sessions::SessionManager;
use databend_query::Config;
use once_cell::sync::OnceCell;
use opendal::Operator;
use parking_lot::Mutex;

/// Hard code, in order to make each test share the global service instance, we made some hack code
///   - We use thread names as key to store global service instances, because rust test passes the test name through the thread name
///   - We created an LRU queue to store the last ten global service instances, because the tests may run in parallel
///   - In the debug version, we enable the transfer of thread names by environment variables.
pub struct TestGlobalServices {
    global_runtime: Mutex<HashMap<String, Arc<Runtime>>>,
    query_logger: Mutex<HashMap<String, Arc<QueryLogger>>>,
    cluster_discovery: Mutex<HashMap<String, Arc<ClusterDiscovery>>>,
    storage_operator: Mutex<HashMap<String, Operator>>,
    async_insert_manager: Mutex<HashMap<String, Arc<AsyncInsertManager>>>,
    cache_manager: Mutex<HashMap<String, Arc<CacheManager>>>,
    catalog_manager: Mutex<HashMap<String, Arc<CatalogManager>>>,
    http_query_manager: Mutex<HashMap<String, Arc<HttpQueryManager>>>,
    data_exchange_manager: Mutex<HashMap<String, Arc<DataExchangeManager>>>,
    session_manager: Mutex<HashMap<String, Arc<SessionManager>>>,
    users_manager: Mutex<HashMap<String, Arc<UserApiProvider>>>,
    users_role_manager: Mutex<HashMap<String, Arc<RoleCacheManager>>>,

    lru_queue: Mutex<VecDeque<String>>,
}

unsafe impl Send for TestGlobalServices {}

unsafe impl Sync for TestGlobalServices {}

static GLOBAL: OnceCell<Arc<TestGlobalServices>> = OnceCell::new();

impl TestGlobalServices {
    pub async fn setup(config: Config) -> Result<()> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");
        let global_services = GLOBAL.get_or_init(|| {
            Arc::new(TestGlobalServices {
                global_runtime: Mutex::new(HashMap::new()),
                query_logger: Mutex::new(HashMap::new()),
                cluster_discovery: Mutex::new(HashMap::new()),
                storage_operator: Mutex::new(HashMap::new()),
                async_insert_manager: Mutex::new(HashMap::new()),
                cache_manager: Mutex::new(HashMap::new()),
                catalog_manager: Mutex::new(HashMap::new()),
                http_query_manager: Mutex::new(HashMap::new()),
                data_exchange_manager: Mutex::new(HashMap::new()),
                session_manager: Mutex::new(HashMap::new()),
                users_manager: Mutex::new(HashMap::new()),
                users_role_manager: Mutex::new(HashMap::new()),
                lru_queue: Mutex::new(VecDeque::new()),
            })
        });

        {
            match std::thread::current().name() {
                None => panic!("thread name is none"),
                Some(thread_name) => {
                    let mut lru_queue = global_services.lru_queue.lock();
                    lru_queue.push_back(thread_name.to_string());

                    if lru_queue.len() >= 10 {
                        let remove_id = lru_queue.pop_front().unwrap();

                        if !lru_queue.contains(&remove_id) {
                            if remove_id != thread_name {
                                // drop(lru_queue);
                                global_services.remove_services(&remove_id);
                            } else {
                                lru_queue.push_back(remove_id);
                            }
                        }
                    }
                }
            }
        }

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
            global_services.clone(),
        )
            .await?;
        RoleCacheManager::init(global_services.clone())?;

        ClusterDiscovery::instance()
            .register_to_metastore(&config)
            .await
    }

    pub fn remove_services(&self, key: &str) {
        {
            let mut global_runtime = self.global_runtime.lock();
            global_runtime.remove(key);
        }
        {
            let mut query_logger = self.query_logger.lock();
            query_logger.remove(key);
        }
        {
            let mut cluster_discovery = self.cluster_discovery.lock();
            cluster_discovery.remove(key);
        }
        {
            let mut storage_operator = self.storage_operator.lock();
            storage_operator.remove(key);
        }
        {
            let mut async_insert_manager = self.async_insert_manager.lock();
            if let Some(async_insert_manager) = async_insert_manager.remove(key) {
                async_insert_manager.shutdown();
            }
        }
        {
            let mut cache_manager = self.cache_manager.lock();
            cache_manager.remove(key);
        }
        {
            let mut catalog_manager = self.catalog_manager.lock();
            catalog_manager.remove(key);
        }
        {
            let mut http_query_manager = self.http_query_manager.lock();
            http_query_manager.remove(key);
        }
        {
            let mut data_exchange_manager = self.data_exchange_manager.lock();
            data_exchange_manager.remove(key);
        }
        {
            let mut session_manager = self.session_manager.lock();
            session_manager.remove(key);
        }
        {
            let mut users_role_manager = self.users_role_manager.lock();
            users_role_manager.remove(key);
        }
        {
            let mut users_manager = self.users_manager.lock();
            users_manager.remove(key);
        }
    }
}

impl SingletonImpl<Arc<Runtime>> for TestGlobalServices {
    fn get(&self) -> Arc<Runtime> {
        match std::thread::current().name() {
            None => panic!("Global runtime is not init"),
            Some(name) => match self.global_runtime.lock().get(name) {
                None => panic!("Global runtime is not init"),
                Some(global_runtime) => global_runtime.clone(),
            },
        }
    }

    fn init(&self, value: Arc<Runtime>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut global_runtime = self.global_runtime.lock();
                global_runtime.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<QueryLogger>> for TestGlobalServices {
    fn get(&self) -> Arc<QueryLogger> {
        match std::thread::current().name() {
            None => panic!("QueryLogger is not init"),
            Some(name) => match self.query_logger.lock().get(name) {
                None => panic!("QueryLogger is not init"),
                Some(query_logger) => query_logger.clone(),
            },
        }
    }

    fn init(&self, value: Arc<QueryLogger>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut query_logger = self.query_logger.lock();
                query_logger.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<ClusterDiscovery>> for TestGlobalServices {
    fn get(&self) -> Arc<ClusterDiscovery> {
        match std::thread::current().name() {
            None => panic!("ClusterDiscovery is not init"),
            Some(name) => match self.cluster_discovery.lock().get(name) {
                None => panic!("ClusterDiscovery is not init"),
                Some(cluster_discovery) => cluster_discovery.clone(),
            },
        }
    }

    fn init(&self, value: Arc<ClusterDiscovery>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut cluster_discovery = self.cluster_discovery.lock();
                cluster_discovery.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Operator> for TestGlobalServices {
    fn get(&self) -> Operator {
        match std::thread::current().name() {
            None => panic!("Operator is not init"),
            Some(name) => match self.storage_operator.lock().get(name) {
                None => panic!("Operator is not init"),
                Some(storage_operator) => storage_operator.clone(),
            },
        }
    }

    fn init(&self, value: Operator) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut storage_operator = self.storage_operator.lock();
                storage_operator.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<AsyncInsertManager>> for TestGlobalServices {
    fn get(&self) -> Arc<AsyncInsertManager> {
        match std::thread::current().name() {
            None => panic!("AsyncInsertManager is not init"),
            Some(name) => match self.async_insert_manager.lock().get(name) {
                None => panic!("AsyncInsertManager is not init"),
                Some(async_insert_manager) => async_insert_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<AsyncInsertManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut async_insert_manager = self.async_insert_manager.lock();
                async_insert_manager.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<CacheManager>> for TestGlobalServices {
    fn get(&self) -> Arc<CacheManager> {
        match std::thread::current().name() {
            None => panic!("CacheManager is not init"),
            Some(name) => match self.cache_manager.lock().get(name) {
                None => panic!(
                    "CacheManager is not init {:?}",
                    std::thread::current().name()
                ),
                Some(cache_manager) => cache_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<CacheManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut cache_manager = self.cache_manager.lock();
                cache_manager.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<CatalogManager>> for TestGlobalServices {
    fn get(&self) -> Arc<CatalogManager> {
        match std::thread::current().name() {
            None => panic!("CatalogManager is not init"),
            Some(name) => match self.catalog_manager.lock().get(name) {
                None => panic!("CatalogManager is not init"),
                Some(catalog_manager) => catalog_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<CatalogManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut catalog_manager = self.catalog_manager.lock();
                catalog_manager.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<HttpQueryManager>> for TestGlobalServices {
    fn get(&self) -> Arc<HttpQueryManager> {
        match std::thread::current().name() {
            None => panic!("HttpQueryManager is not init"),
            Some(name) => match self.http_query_manager.lock().get(name) {
                None => panic!("HttpQueryManager is not init"),
                Some(http_query_manager) => http_query_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<HttpQueryManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut http_query_manager = self.http_query_manager.lock();
                http_query_manager.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<DataExchangeManager>> for TestGlobalServices {
    fn get(&self) -> Arc<DataExchangeManager> {
        match std::thread::current().name() {
            None => panic!("DataExchangeManager is not init"),
            Some(name) => match self.data_exchange_manager.lock().get(name) {
                None => panic!("DataExchangeManager is not init"),
                Some(data_exchange_manager) => data_exchange_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<DataExchangeManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut data_exchange_manager = self.data_exchange_manager.lock();
                data_exchange_manager.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<SessionManager>> for TestGlobalServices {
    fn get(&self) -> Arc<SessionManager> {
        match std::thread::current().name() {
            None => panic!("SessionManager is not init"),
            Some(name) => match self.session_manager.lock().get(name) {
                None => panic!("SessionManager is not init"),
                Some(session_manager) => session_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<SessionManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut session_manager = self.session_manager.lock();
                session_manager.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<UserApiProvider>> for TestGlobalServices {
    fn get(&self) -> Arc<UserApiProvider> {
        match std::thread::current().name() {
            None => panic!("UserApiProvider is not init"),
            Some(name) => match self.users_manager.lock().get(name) {
                None => panic!("UserApiProvider is not init"),
                Some(users_manager) => users_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<UserApiProvider>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut users_manager = self.users_manager.lock();
                users_manager.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}

impl SingletonImpl<Arc<RoleCacheManager>> for TestGlobalServices {
    fn get(&self) -> Arc<RoleCacheManager> {
        match std::thread::current().name() {
            None => panic!("RoleCacheManager is not init"),
            Some(name) => match self.users_role_manager.lock().get(name) {
                None => panic!("RoleCacheManager is not init"),
                Some(users_role_manager) => users_role_manager.clone(),
            },
        }
    }

    fn init(&self, value: Arc<RoleCacheManager>) -> Result<()> {
        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(name) => {
                let mut users_role_manager = self.users_role_manager.lock();
                users_role_manager.insert(name.to_string(), value);
                Ok(())
            }
        }
    }
}
