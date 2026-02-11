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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GLOBAL_QUERIES_MANAGER;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::GlobalQueryRuntime;
use databend_common_catalog::catalog::CatalogCreator;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::StackTrace;
use databend_common_management::WorkloadGroupResourceManager;
use databend_common_management::WorkloadMgr;
use databend_common_meta_app::schema::CatalogType;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_storage::DataOperator;
use databend_common_storage::ShareTableConfig;
use databend_common_storages_hive::HiveCreator;
use databend_common_tracing::GlobalLogger;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_common_users::builtin::BuiltIn;
use databend_enterprise_resources_management::DummyResourcesManagement;
use databend_meta_runtime::DatabendRuntime;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::TempDirManager;

use crate::auth::AuthMgr;
use crate::builtin::BuiltinUDFs;
use crate::builtin::BuiltinUsers;
use crate::catalogs::DatabaseCatalog;
use crate::catalogs::IcebergCreator;
use crate::clusters::ClusterDiscovery;
use crate::history_tables::GlobalHistoryLog;
use crate::locks::LockManager;
use crate::pipelines::executor::GlobalQueriesExecutor;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::http::v1::ClientSessionManager;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::QueriesQueueManager;
use crate::sessions::SessionManager;
use crate::spillers::SpillsBufferPool;
use crate::task::service::TaskService;

pub struct GlobalServices;

impl GlobalServices {
    #[async_backtrace::framed]
    pub async fn init(config: &InnerConfig, version: BuildInfoRef, ee_mode: bool) -> Result<()> {
        GlobalInstance::init_production();
        GlobalServices::init_with(config, version, ee_mode).await
    }

    #[async_backtrace::framed]
    pub async fn init_with(
        config: &InnerConfig,
        version: BuildInfoRef,
        ee_mode: bool,
    ) -> Result<()> {
        StackTrace::pre_load_symbol();

        // app name format: node_id[0..7]@cluster_id
        let app_name_shuffle = format!("databend-query-{}", config.query.common.cluster_id);

        // The order of initialization is very important
        // 1. global config init.
        GlobalConfig::init(config, version)?;

        // 2. log init.
        let mut log_labels = BTreeMap::new();
        log_labels.insert("service".to_string(), "databend-query".to_string());
        log_labels.insert(
            "tenant_id".to_string(),
            config.query.tenant_id.tenant_name().to_string(),
        );
        log_labels.insert(
            "warehouse_id".to_string(),
            config.query.common.warehouse_id.clone(),
        );
        log_labels.insert(
            "cluster_id".to_string(),
            config.query.common.cluster_id.clone(),
        );
        log_labels.insert("node_id".to_string(), config.query.node_id.clone());
        GlobalLogger::init(&app_name_shuffle, &config.log, log_labels);

        // 3. runtime init.
        GlobalIORuntime::init(config.storage.num_cpus as usize)?;
        GlobalQueryRuntime::init(config.storage.num_cpus as usize)?;

        // 4. cluster discovery init.
        ClusterDiscovery::init(config, version).await?;

        SpillsBufferPool::init();
        // TODO(xuanwo):
        //
        // This part is a bit complex because catalog are used widely in different
        // crates that we don't have a good place for different kinds of creators.
        //
        // Maybe we can do some refactor to simplify the logic here.
        {
            // Init default catalog.
            let default_catalog =
                DatabaseCatalog::try_create_with_config(config.clone(), version).await?;

            let catalog_creator: Vec<(CatalogType, Arc<dyn CatalogCreator>)> = vec![
                (CatalogType::Iceberg, Arc::new(IcebergCreator)),
                (CatalogType::Hive, Arc::new(HiveCreator)),
            ];

            CatalogManager::init(config, Arc::new(default_catalog), catalog_creator, version)
                .await?;
        }

        QueriesQueueManager::init(config.query.common.max_running_queries as usize, config).await?;
        HttpQueryManager::init(config).await?;
        ClientSessionManager::init(config).await?;
        DataExchangeManager::init()?;
        SessionManager::init(config)?;
        LockManager::init()?;
        AuthMgr::init(config, version)?;

        // Init user manager.
        // Builtin users and udfs are created here.
        {
            let built_in_users = BuiltinUsers::create(config.query.builtin.users.clone());
            let built_in_udfs = BuiltinUDFs::create(config.query.builtin.udfs.clone());

            // We will ignore the error here, and just log a error.
            let builtin = BuiltIn {
                users: built_in_users.to_auth_infos(),
                udfs: built_in_udfs.to_udfs(),
            };
            UserApiProvider::init(
                config.meta.to_meta_grpc_client_conf(),
                &config.cache,
                builtin,
                &config.query.tenant_id,
                config.query.tenant_quota.clone(),
            )
            .await?;
        }
        RoleCacheManager::init()?;

        DataOperator::init(&config.storage, config.spill.storage_params.clone()).await?;
        ShareTableConfig::init(
            &config.query.common.share_endpoint_address,
            &config.query.common.share_endpoint_auth_token_file,
            config.query.tenant_id.tenant_name().to_string(),
        )?;
        CacheManager::init(
            &config.cache,
            &config.query.common.max_server_memory_usage,
            config.query.tenant_id.tenant_name().to_string(),
            ee_mode,
        )?;
        TempDirManager::init(&config.spill, config.query.tenant_id.tenant_name())?;

        if let Some(addr) = config
            .query
            .common
            .cloud_control_grpc_server_address
            .clone()
        {
            CloudControlApiProvider::init(addr, config.query.common.cloud_control_grpc_timeout)
                .await?;
        }

        if !ee_mode {
            DummyResourcesManagement::init()?;
        }

        if config.query.common.enable_queries_executor {
            GlobalQueriesExecutor::init()?;
        }

        Self::init_workload_mgr(config).await?;

        if config.log.history.on {
            GlobalHistoryLog::init(config, version).await?;
        }
        if config.task.on {
            if config
                .query
                .common
                .cloud_control_grpc_server_address
                .is_some()
            {
                return Err(ErrorCode::InvalidConfig(
                    "Private Task is enabled but `cloud_control_grpc_server_address` is not empty",
                ));
            }
            TaskService::init(config).await?;
        }

        GLOBAL_QUERIES_MANAGER.set_gc_handle(memory_gc_handle);

        Ok(())
    }

    async fn init_workload_mgr(config: &InnerConfig) -> Result<()> {
        let meta_api_provider = MetaStoreProvider::new(config.meta.to_meta_grpc_client_conf());
        let meta_store = match meta_api_provider
            .create_meta_store::<DatabendRuntime>()
            .await
        {
            Ok(meta_store) => Ok(meta_store),
            Err(cause) => Err(ErrorCode::MetaServiceError(format!(
                "Failed to create meta store: {}",
                cause
            ))),
        }?;

        let tenant = config.query.tenant_id.tenant_name().to_string();
        let workload_mgr = Arc::new(WorkloadMgr::create(meta_store, &tenant)?);
        GlobalInstance::set(workload_mgr.clone());
        GlobalInstance::set(WorkloadGroupResourceManager::new(workload_mgr.clone()));
        Ok(())
    }
}

pub fn memory_gc_handle(query_id: &String, _force: bool) -> bool {
    log::info!("memory_gc_handle {}", query_id);
    let sessions_manager = SessionManager::instance();
    // TODO: dealloc jemalloc dirty page?
    // TODO: page cache?
    // TODO: databend cache?
    // TODO: spill query?
    sessions_manager.kill_by_query_id(query_id);
    true
}
