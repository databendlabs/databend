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
use std::sync::Arc;
use std::sync::Mutex;

use common_base::base::Singleton;
use common_catalog::catalog::Catalog;
pub use common_catalog::catalog::CatalogManager;
use common_catalog::catalog::CATALOG_DEFAULT;
use common_config::Config;
use common_exception::ErrorCode;
use common_exception::Result;
#[cfg(feature = "hive")]
use common_meta_app::schema::CatalogType;
#[cfg(feature = "hive")]
use common_meta_app::schema::CreateCatalogReq;
#[cfg(feature = "hive")]
use common_storages_hive::CATALOG_HIVE;

use crate::catalogs::DatabaseCatalog;

#[async_trait::async_trait]
pub trait CatalogManagerHelper {
    async fn init(conf: &Config, v: Singleton<Arc<CatalogManager>>) -> Result<()>;

    async fn try_create(conf: &Config) -> Result<Arc<CatalogManager>>;

    async fn register_build_in_catalogs(&self, conf: &Config) -> Result<()>;

    #[cfg(feature = "hive")]
    fn register_external_catalogs(&self, conf: &Config) -> Result<()>;

    #[cfg(feature = "hive")]
    async fn create_user_defined_catalog(&self, req: CreateCatalogReq) -> Result<()>;
}

#[async_trait::async_trait]
impl CatalogManagerHelper for CatalogManager {
    async fn init(conf: &Config, v: Singleton<Arc<CatalogManager>>) -> Result<()> {
        v.init(Self::try_create(conf).await?)?;
        CatalogManager::set_instance(v);
        Ok(())
    }

    async fn try_create(conf: &Config) -> Result<Arc<CatalogManager>> {
        let catalog_manager = CatalogManager {
            catalogs: Mutex::new(HashMap::new()),
        };

        catalog_manager.register_build_in_catalogs(conf).await?;

        #[cfg(feature = "hive")]
        {
            catalog_manager.register_external_catalogs(conf)?;
        }

        Ok(Arc::new(catalog_manager))
    }

    async fn register_build_in_catalogs(&self, conf: &Config) -> Result<()> {
        let default_catalog: Arc<dyn Catalog> =
            Arc::new(DatabaseCatalog::try_create_with_config(conf.clone()).await?);
        {
            self.catalogs
                .lock()
                .map_err(|e| {
                    ErrorCode::InternalError(format!("acquire catalog manager lock error: {:?}", e))
                })?
                .insert(CATALOG_DEFAULT.to_owned(), default_catalog);
        }
        Ok(())
    }

    #[cfg(feature = "hive")]
    fn register_external_catalogs(&self, conf: &Config) -> Result<()> {
        use crate::catalogs::hive::HiveCatalog;
        let hms_address = &conf.catalog.meta_store_address;
        if !hms_address.is_empty() {
            // register hive catalog
            let hive_catalog: Arc<dyn Catalog> = Arc::new(HiveCatalog::try_create(hms_address)?);
            {
                self.catalogs
                    .lock()
                    .map_err(|e| {
                        ErrorCode::InternalError(format!(
                            "acquire catalog manager lock error: {:?}",
                            e
                        ))
                    })?
                    .insert(CATALOG_HIVE.to_owned(), hive_catalog);
            }
        }
        Ok(())
    }

    #[cfg(feature = "hive")]
    // TODO: support more catalog types
    async fn create_user_defined_catalog(&self, req: CreateCatalogReq) -> Result<()> {
        use crate::catalogs::hive::HiveCatalog;
        if req.name_ident.tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant cannot be empty(while create catalog)",
            ));
        }
        tracing::info!("Creat user defined catalog from req: {:?}", req);

        // create catalog first and check conflict later
        let ctl_name = &req.name_ident.ctl_name;
        let ctl_type = &req.meta.catalog_type;
        let udc: Arc<dyn Catalog> = match ctl_type {
            CatalogType::Default => unreachable!(),
            CatalogType::Hive => {
                if let Some(hms_address) = req.meta.options.get("HMS_ADDRESS") {
                    Arc::new(HiveCatalog::try_create(hms_address)?)
                } else {
                    return Err(ErrorCode::UnknownCatalogType(format!(
                        "Hive catalog type must have HMS_ADDRESS in options, but got: {:?}",
                        req.meta.options
                    )));
                }
            }
        };

        let if_not_exists = req.if_not_exists;
        {
            let mut guard = self.catalogs.lock().map_err(|e| {
                ErrorCode::InternalError(format!("acquire catalog manager lock error: {:?}", e))
            })?;
            if guard.contains_key(ctl_name) && !if_not_exists {
                return Err(ErrorCode::CatalogAlreadyExists(format!(
                    "{} catalog exisits",
                    ctl_name
                )));
            }
            guard.insert(ctl_name.to_owned(), udc);
        }
        Ok(())
    }
}
