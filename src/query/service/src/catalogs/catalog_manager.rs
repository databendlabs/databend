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

use common_base::base::GlobalInstance;
use common_catalog::catalog::Catalog;
pub use common_catalog::catalog::CatalogManager;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_config::CatalogConfig;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CatalogOption;
use common_meta_app::schema::CreateCatalogReq;
use common_meta_app::schema::DropCatalogReq;
use common_meta_app::schema::IcebergCatalogOption;
use common_storage::DataOperator;
#[cfg(feature = "hive")]
use common_storages_hive::HiveCatalog;
use common_storages_iceberg::IcebergCatalog;
use dashmap::DashMap;

use crate::catalogs::DatabaseCatalog;

#[async_trait::async_trait]
pub trait CatalogManagerHelper {
    async fn init(conf: &InnerConfig) -> Result<()>;

    async fn try_create(conf: &InnerConfig) -> Result<Arc<CatalogManager>>;

    async fn register_build_in_catalogs(&self, conf: &InnerConfig) -> Result<()>;

    fn register_external_catalogs(&self, conf: &InnerConfig) -> Result<()>;

    /// build catalog from sql
    async fn create_user_defined_catalog(&self, req: CreateCatalogReq) -> Result<()>;

    fn drop_user_defined_catalog(&self, req: DropCatalogReq) -> Result<()>;
}

#[async_trait::async_trait]
impl CatalogManagerHelper for CatalogManager {
    #[async_backtrace::framed]
    async fn init(conf: &InnerConfig) -> Result<()> {
        GlobalInstance::set(Self::try_create(conf).await?);

        Ok(())
    }

    #[async_backtrace::framed]
    async fn try_create(conf: &InnerConfig) -> Result<Arc<CatalogManager>> {
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

    #[async_backtrace::framed]
    async fn register_build_in_catalogs(&self, conf: &InnerConfig) -> Result<()> {
        let default_catalog: Arc<dyn Catalog> =
            Arc::new(DatabaseCatalog::try_create_with_config(conf.clone()).await?);
        self.catalogs
            .insert(CATALOG_DEFAULT.to_owned(), default_catalog);
        Ok(())
    }

    fn register_external_catalogs(&self, conf: &InnerConfig) -> Result<()> {
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

    #[async_backtrace::framed]
    async fn create_user_defined_catalog(&self, req: CreateCatalogReq) -> Result<()> {
        let catalog_option = req.meta.catalog_option;

        // create catalog first
        match catalog_option {
            // NOTE:
            // when compiling without `hive` feature enabled
            // `address` will be seem as unused, which is not intentional
            #[allow(unused)]
            CatalogOption::Hive(address) => {
                #[cfg(not(feature = "hive"))]
                {
                    Err(ErrorCode::CatalogNotSupported(
                        "Hive catalog is not enabled, please recompile with --features hive",
                    ))
                }
                #[cfg(feature = "hive")]
                {
                    let catalog: Arc<dyn Catalog> = Arc::new(HiveCatalog::try_create(address)?);
                    let ctl_name = &req.name_ident.catalog_name;
                    let if_not_exists = req.if_not_exists;

                    self.insert_catalog(ctl_name, catalog, if_not_exists)
                }
            }
            CatalogOption::Iceberg(opt) => {
                let IcebergCatalogOption {
                    storage_params: sp,
                    flatten,
                } = opt;

                let data_operator = DataOperator::try_create(&sp).await?;
                let ctl_name = &req.name_ident.catalog_name;
                let catalog: Arc<dyn Catalog> = Arc::new(IcebergCatalog::try_create(
                    ctl_name,
                    flatten,
                    data_operator,
                )?);

                let if_not_exists = req.if_not_exists;
                self.insert_catalog(ctl_name, catalog, if_not_exists)
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
