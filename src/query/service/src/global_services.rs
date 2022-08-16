use common_base::base::GlobalIORuntime;
use common_catalog::catalog::CatalogManager;
use common_config::Config;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_storage::StorageOperator;
use common_tracing::QueryLogger;
use common_users::{RoleCacheManager, UserApiProvider};
use crate::api::DataExchangeManager;
use crate::catalogs::CatalogManagerHelper;
use crate::clusters::ClusterDiscovery;
use crate::interpreters::AsyncInsertManager;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::SessionManager;

pub struct GlobalServices;

impl GlobalServices {
    pub async fn init(config: Config) -> Result<()> {
        // The order of initialization is very important
        let app_name_shuffle = format!("{}-{}", config.query.tenant_id, config.query.cluster_id);

        QueryLogger::init(app_name_shuffle, &config.log)?;
        GlobalIORuntime::init(config.query.num_cpus as usize)?;

        // Cluster discovery.
        ClusterDiscovery::init(config.clone()).await?;

        StorageOperator::init(&config.storage).await?;
        AsyncInsertManager::init(&config)?;
        CacheManager::init(&config.query)?;
        CatalogManager::init(&config).await?;
        HttpQueryManager::init(&config).await?;
        DataExchangeManager::init(config.clone())?;
        SessionManager::init(config.clone())?;
        UserApiProvider::init(config.meta.to_meta_grpc_client_conf()).await?;
        RoleCacheManager::init()?;
        Ok(())
    }

    pub async fn init_with_guard(config: Config) -> Result<GlobalServices> {
        GlobalServices::init(config).await?;
        Ok(GlobalServices)
    }

    pub fn destroy() {
        RoleCacheManager::destroy();
        UserApiProvider::destroy();
        SessionManager::destroy();
        DataExchangeManager::destroy();
        HttpQueryManager::destroy();
        CatalogManager::destroy();
        CacheManager::destroy();
        AsyncInsertManager::destroy();
        StorageOperator::destroy();
        ClusterDiscovery::destroy();

        GlobalIORuntime::destroy();
        QueryLogger::destroy();
    }
}

impl Drop for GlobalServices {
    fn drop(&mut self) {
        Self::destroy();
    }
}

