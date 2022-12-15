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

use common_base::base::GlobalInstance;
use common_base::runtime::GlobalIORuntime;
use common_catalog::catalog::CatalogManager;
use common_config::Config;
use common_config::GlobalConfig;
use common_exception::Result;
use common_storage::CacheOperator;
use common_storage::DataOperator;
use common_storage::ShareTableConfig;
use common_storages_table_meta::caches::CacheManager;
use common_tracing::QueryLogger;
use common_users::RoleCacheManager;
use common_users::UserApiProvider;

use crate::api::DataExchangeManager;
use crate::catalogs::CatalogManagerHelper;
use crate::clusters::ClusterDiscovery;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::SessionManager;

pub struct GlobalServices;

impl GlobalServices {
    pub async fn init(config: Config) -> Result<()> {
        GlobalInstance::init_production();
        GlobalServices::init_with(config).await
    }

    pub async fn init_with(config: Config) -> Result<()> {
        // The order of initialization is very important
        GlobalConfig::init(config.clone())?;

        let app_name_shuffle = format!("{}-{}", config.query.tenant_id, config.query.cluster_id);

        QueryLogger::init(app_name_shuffle, &config.log)?;
        GlobalIORuntime::init(config.storage.num_cpus as usize)?;

        // Cluster discovery.
        ClusterDiscovery::init(config.clone()).await?;

        DataOperator::init(&config.storage).await?;
        CacheOperator::init(&config.storage.cache).await?;

        ShareTableConfig::init(
            &config.query.share_endpoint_address,
            &config.query.share_endpoint_auth_token_file,
            config.query.tenant_id.clone(),
        )?;

        CacheManager::init(&config.query)?;
        CatalogManager::init(&config).await?;
        HttpQueryManager::init(&config).await?;
        DataExchangeManager::init()?;
        SessionManager::init(&config)?;
        UserApiProvider::init(
            config.meta.to_meta_grpc_client_conf(),
            config.query.idm,
            config.query.tenant_id.as_str(),
            config.query.tenant_quota,
        )
        .await?;
        RoleCacheManager::init()?;

        Ok(())
    }
}
