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

use std::sync::Arc;
use std::sync::OnceLock;

use common_base::base::GlobalInstance;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::SchemaApi;
use common_meta_app::schema::CatalogType;
use common_meta_app::schema::CreateCatalogReq;
use common_meta_app::schema::DropCatalogReq;
use common_meta_app::schema::GetCatalogReq;
use common_meta_store::MetaStore;
use common_meta_store::MetaStoreProvider;
use dashmap::DashMap;

use super::Catalog;
use super::CatalogCreator;

pub const CATALOG_DEFAULT: &str = "default";

pub struct CatalogManager {
    pub meta: MetaStore,
    pub tenant: String,

    pub default_catalog: OnceLock<Arc<dyn Catalog>>,
    pub catalog_creators: DashMap<CatalogType, Arc<dyn CatalogCreator>>,
}

impl CatalogManager {
    /// Fetch catalog manager from global instance.
    pub fn instance() -> Arc<CatalogManager> {
        GlobalInstance::get()
    }

    /// Init the catalog manager in global instance.
    #[async_backtrace::framed]
    pub async fn init(conf: &InnerConfig) -> Result<()> {
        GlobalInstance::set(Self::try_create(conf).await?);

        Ok(())
    }

    /// Try to create a catalog manager via Config.
    #[async_backtrace::framed]
    async fn try_create(conf: &InnerConfig) -> Result<Arc<CatalogManager>> {
        let meta = {
            let provider = Arc::new(MetaStoreProvider::new(conf.meta.to_meta_grpc_client_conf()));

            provider.create_meta_store().await?
        };

        let tenant = conf.query.tenant_id.clone();

        let catalog_manager = Self {
            meta,
            tenant,
            default_catalog: OnceLock::new(),
            catalog_creators: DashMap::new(),
        };

        Ok(Arc::new(catalog_manager))
    }

    pub fn init_default_catalog(&self, default_catalog: Arc<dyn Catalog>) {
        self.default_catalog
            .set(default_catalog)
            .expect("init default catalog must succeed")
    }

    /// Get default catalog from manager.
    ///
    /// There are some place that we don't have async context, so we provide
    /// `get_default_catalog` to allow users fetch default catalog without async.
    pub fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.default_catalog
            .get()
            .cloned()
            .ok_or_else(|| ErrorCode::BadArguments("default catalog is not initiated".to_string()))
    }

    /// Register a catalog creator for given name.
    pub fn register_catalog(&self, typ: CatalogType, creator: Arc<dyn CatalogCreator>) {
        self.catalog_creators.insert(typ, creator);
    }

    /// Get a catalog from manager.
    ///
    /// # NOTES
    ///
    /// DEFAULT catalog is handled specially via `get_default_catalog`. Other catalogs
    /// will be fetched from metasrv.
    #[async_backtrace::framed]
    pub async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        if catalog_name == CATALOG_DEFAULT {
            return self.get_default_catalog();
        }

        // Get catalog from metasrv.
        let info = self
            .meta
            .get_catalog(GetCatalogReq::new(&self.tenant, catalog_name))
            .await?;

        let typ = info.meta.catalog_option.catalog_type();
        let creator = self.catalog_creators.get(&typ).ok_or_else(|| {
            ErrorCode::UnknownCatalogType(format!("unknown catalog type: {:?}", typ))
        })?;

        creator.try_create(info).await
    }

    /// Create a new catalog.
    ///
    /// # NOTES
    ///
    /// Trying to create default catalog will return an error.
    #[async_backtrace::framed]
    pub async fn create_catalog(&self, _: CreateCatalogReq) -> Result<()> {
        todo!()
    }

    /// Drop a catalog.
    ///
    /// # NOTES
    ///
    /// Trying to drop default catalog will return an error.
    #[async_backtrace::framed]
    pub async fn drop_catalog(&self, _: DropCatalogReq) -> Result<()> {
        todo!()
    }

    #[async_backtrace::framed]
    pub async fn list_catalogs(&self) -> Result<Vec<Arc<dyn Catalog>>> {
        todo!()
    }
}
