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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::catalogs::default::DatabaseCatalog;
use crate::catalogs::hive::HiveCatalog;
use crate::catalogs::Catalog;
use crate::configs::Config;

// TODO catalogs are hard coded

pub const CATALOG_DEFAULT: &str = "default";
pub const CATALOG_HIVE: &str = "hive";

pub struct CatalogManager {
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl CatalogManager {
    pub async fn new(conf: &Config) -> Result<CatalogManager> {
        let catalogs = HashMap::new();
        let mut manager = CatalogManager { catalogs };

        manager.register_build_in_catalogs(conf).await?;
        manager.register_external_catalogs(conf)?;

        Ok(manager)
    }

    pub fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.catalogs
            .get(catalog_name)
            .cloned()
            .ok_or_else(|| ErrorCode::BadArguments(format!("not such catalog {}", catalog_name)))
    }

    async fn register_build_in_catalogs(&mut self, conf: &Config) -> Result<()> {
        let default_catalog: Arc<dyn Catalog> =
            Arc::new(DatabaseCatalog::try_create_with_config(conf.clone()).await?);
        self.catalogs
            .insert(CATALOG_DEFAULT.to_owned(), default_catalog);
        Ok(())
    }

    fn register_external_catalogs(&mut self, conf: &Config) -> Result<()> {
        let hms_address = &conf.catalog.meta_store_address;
        if !hms_address.is_empty() {
            // register hive catalog
            let hive_catalog: Arc<dyn Catalog> =
                Arc::new(HiveCatalog::try_create_with_config(hms_address)?);
            self.catalogs.insert(CATALOG_HIVE.to_owned(), hive_catalog);
        }
        Ok(())
    }
}
