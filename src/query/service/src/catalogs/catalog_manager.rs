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

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_catalog::catalog::Catalog;
pub use common_catalog::catalog::CatalogManager;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_config::CatalogConfig;
use common_config::Config;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CatalogType;
use common_meta_app::schema::CreateCatalogReq;
use common_meta_app::schema::DropCatalogReq;
#[cfg(feature = "hive")]
use common_storages_hive::HiveCatalog;
use dashmap::DashMap;

use crate::catalogs::DatabaseCatalog;

#[async_trait::async_trait]
pub trait CatalogManagerHelper {
    async fn init(conf: &Config) -> Result<()>;

    async fn try_create(conf: &Config) -> Result<Arc<CatalogManager>>;

    async fn register_build_in_catalogs(&self, conf: &Config) -> Result<()>;

    fn register_external_catalogs(&self, conf: &Config) -> Result<()>;

    fn create_user_defined_catalog(&self, req: CreateCatalogReq) -> Result<()>;

    fn drop_user_defined_catalog(&self, req: DropCatalogReq) -> Result<()>;
}

#[async_trait::async_trait]
impl CatalogManagerHelper for CatalogManager {
    async fn init(conf: &Config) -> Result<()> {
        GlobalInstance::set(Self::try_create(conf).await?);

        Ok(())
    }

    async fn try_create(conf: &Config) -> Result<Arc<CatalogManager>> {
        let catalog_manager = CatalogManager {
            catalogs: DashMap::new(),
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
        self.catalogs
            .insert(CATALOG_DEFAULT.to_owned(), default_catalog);
        Ok(())
    }

    fn register_external_catalogs(&self, conf: &Config) -> Result<()> {
        // currently, if the `hive` feature is not enabled
        // the loop will quit after the first iteration.
        // this is expected.
        #[allow(clippy::never_loop)]
        for (name, ctl) in conf.catalogs.iter() {
            match ctl {
                CatalogConfig::Hive(ctl) => {
                    // register hive catalog
                    #[cfg(not(feature = "hive"))]
                    {
                        return Err(ErrorCode::CatalogNotSupported(format!(
                            "Failed to create catalog {} to {}: Hive catalog is not enabled, please recompile with --features hive",
                            name, ctl.address
                        )));
                    }
                    #[cfg(feature = "hive")]
                    {
                        let hms_address = ctl.address.clone();
                        let hive_catalog = Arc::new(HiveCatalog::try_create(hms_address)?);
                        self.catalogs.insert(name.to_string(), hive_catalog);
                    }
                }
            }
        }
        Ok(())
    }

    fn create_user_defined_catalog(&self, req: CreateCatalogReq) -> Result<()> {
        let catalog_type = req.meta.catalog_type;

        // create catalog first
        match catalog_type {
            CatalogType::Default => Err(ErrorCode::CatalogNotSupported(
                "Creating a DEFAULT catalog is not allowed",
            )),
            CatalogType::Hive => {
                #[cfg(not(feature = "hive"))]
                {
                    Err(ErrorCode::CatalogNotSupported(
                        "Hive catalog is not enabled, please recompile with --features hive",
                    ))
                }
                #[cfg(feature = "hive")]
                {
                    let catalog_options = req.meta.options;
                    let address = catalog_options
                        .get("address")
                        .ok_or_else(|| ErrorCode::InvalidArgument("expected field: ADDRESS"))?;
                    let catalog: Arc<dyn Catalog> = Arc::new(HiveCatalog::try_create(address)?);
                    let ctl_name = &req.name_ident.catalog_name;
                    let if_not_exists = req.if_not_exists;

                    self.insert_catalog(ctl_name, catalog, if_not_exists)
                }
            }
        }
    }

    fn drop_user_defined_catalog(&self, req: DropCatalogReq) -> Result<()> {
        let name = req.name_ident.catalog_name;
        if name == CATALOG_DEFAULT {
            return Err(ErrorCode::CatalogNotSupported(
                "Dropping the DEFAULT catalog is not allowed",
            ));
        }

        match self.catalogs.remove(&name) {
            Some(_) => Ok(()),

            None if req.if_exists => Ok(()),

            None => Err(ErrorCode::CatalogNotFound(format!(
                "Catalog {} has to be exists",
                name
            ))),
        }
    }
}
