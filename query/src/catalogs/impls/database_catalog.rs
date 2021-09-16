//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_exception::Result;

use crate::catalogs::impls::sub_cats::metastore_catalog::MetaStoreCatalog;
use crate::catalogs::impls::sub_cats::overlaid_catalog::OverlaidCatalog;
use crate::catalogs::impls::sub_cats::system_catalog::SystemCatalog;
use crate::configs::Config;

/// DatabaseCatalog is the Catalog exports to other query components
pub type DatabaseCatalog = OverlaidCatalog;

impl DatabaseCatalog {
    pub fn try_create_with_config(conf: Config) -> Result<DatabaseCatalog> {
        let system_catalog = SystemCatalog::try_create_with_config(&conf)?;
        let metastore_catalog = MetaStoreCatalog::try_create_with_config(conf)?;
        let res = DatabaseCatalog::create(Arc::new(system_catalog), Arc::new(metastore_catalog));
        Ok(res)
    }
}
