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
use common_exception::ErrorCode;
use common_exception::Result;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use super::Catalog;

pub const CATALOG_DEFAULT: &str = "default";

pub struct CatalogManager {
    pub catalogs: DashMap<String, Arc<dyn Catalog>>,
}

impl CatalogManager {
    pub fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.catalogs
            .get(catalog_name)
            .as_deref()
            .cloned()
            .ok_or_else(|| ErrorCode::BadArguments(format!("no such catalog {}", catalog_name)))
    }

    pub fn instance() -> Arc<CatalogManager> {
        GlobalInstance::get()
    }

    pub fn insert_catalog(
        &self,
        catalog_name: &str,
        catalog: Arc<dyn Catalog>,
        if_not_exists: bool,
    ) -> Result<()> {
        // NOTE:
        //
        // Concurrent write may happen here, should be carefully dealt with.
        // The problem occurs when the entry is vacant:
        //
        // Using `DashMap::entry` can occupy the write lock on the entry,
        // ensuring a safe concurrent writing.
        //
        // If getting with `DashMap::get_mut`, it will unlock the entry and return `None` directly.
        // This makes a safe concurrent write hard to implement.
        match self.catalogs.entry(catalog_name.to_string()) {
            Entry::Occupied(_) => {
                if if_not_exists {
                    Ok(())
                } else {
                    Err(ErrorCode::CatalogAlreadyExists(format!(
                        "Catalog {} already exists",
                        catalog_name
                    )))
                }
            }
            Entry::Vacant(v) => {
                v.insert(catalog);
                Ok(())
            }
        }
    }
}
