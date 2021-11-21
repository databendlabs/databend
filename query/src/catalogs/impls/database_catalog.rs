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

use crate::catalogs::impls::ImmutableCatalog;
use crate::catalogs::impls::MutableCatalog;
use crate::catalogs::impls::OverlaidCatalog1;
use crate::configs::Config;
use crate::datasources;

/// DatabaseCatalog is the Catalog exports to other query components
pub type DatabaseCatalog = OverlaidCatalog1;

impl DatabaseCatalog {
    pub async fn try_create_with_config(conf: Config) -> Result<DatabaseCatalog> {
        let immutable_catalog = ImmutableCatalog::try_create_with_config(&conf).await?;
        let mutable_catalog = MutableCatalog::try_create_with_config(conf).await?;
        let func_engine_registry = datasources::table_func::prelude::prelude_func_engines();
        let res = DatabaseCatalog::create(
            Arc::new(immutable_catalog),
            Arc::new(mutable_catalog),
            func_engine_registry,
        );
        Ok(res)
    }
}
