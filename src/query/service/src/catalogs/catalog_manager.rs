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

use common_catalog::catalog::Catalog;
pub use common_catalog::catalog::CatalogManager;
use common_catalog::catalog::CATALOG_DEFAULT;
use common_config::Config;
use common_exception::ErrorCode;
use common_exception::Result;
#[cfg(feature = "hive")]
use common_storages_hive::CATALOG_HIVE;
use once_cell::sync::OnceCell;

use crate::catalogs::DatabaseCatalog;

#[async_trait::async_trait]
pub trait CatalogManagerHelper {
    async fn init(conf: &Config) -> Result<()>;

    fn instance() -> Arc<CatalogManager>;

    fn destroy();

    async fn register_build_in_catalogs(&mut self, conf: &Config) -> Result<()>;

    #[cfg(feature = "hive")]
    fn register_external_catalogs(&mut self, conf: &Config) -> Result<()>;
}

static CATALOG_MANAGER: OnceCell<Arc<CatalogManager>> = OnceCell::new();

#[async_trait::async_trait]
impl CatalogManagerHelper for CatalogManager {
    async fn init(conf: &Config) -> Result<()> {
        let mut catalog_manager = CatalogManager {
            catalogs: HashMap::new(),
        };

        catalog_manager.register_build_in_catalogs(conf).await?;

        #[cfg(feature = "hive")]
        {
            catalog_manager.register_external_catalogs(conf)?;
        }

        match CATALOG_MANAGER.set(Arc::new(catalog_manager)) {
            Ok(_) => Ok(()),
            Err(_) => Err(ErrorCode::LogicalError("Cannot init SessionManager twice")),
        }
    }

    fn instance() -> Arc<CatalogManager> {
        match CATALOG_MANAGER.get() {
            None => panic!("CatalogManager is not init"),
            Some(catalog_manager) => catalog_manager.clone(),
        }
    }

    fn destroy() {
        unsafe {
            let const_ptr = &CATALOG_MANAGER as *const OnceCell<Arc<CatalogManager>>;
            let mut_ptr = const_ptr as *mut OnceCell<Arc<CatalogManager>>;

            if let Some(catalog_manager) = (*mut_ptr).take() {
                drop(catalog_manager);
            }
        }
    }

    async fn register_build_in_catalogs(&mut self, conf: &Config) -> Result<()> {
        let default_catalog: Arc<dyn Catalog> =
            Arc::new(DatabaseCatalog::try_create_with_config(conf.clone()).await?);
        self.catalogs
            .insert(CATALOG_DEFAULT.to_owned(), default_catalog);
        Ok(())
    }

    #[cfg(feature = "hive")]
    fn register_external_catalogs(&mut self, conf: &Config) -> Result<()> {
        use crate::catalogs::hive::HiveCatalog;
        let hms_address = &conf.catalog.meta_store_address;
        if !hms_address.is_empty() {
            // register hive catalog
            let hive_catalog: Arc<dyn Catalog> = Arc::new(HiveCatalog::try_create(hms_address)?);
            self.catalogs.insert(CATALOG_HIVE.to_owned(), hive_catalog);
        }
        Ok(())
    }
}
