// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::catalogs::impls::DatabaseCatalog;
use crate::catalogs::Catalog;
use crate::configs::Config;
use crate::datasources::local::LocalDatabases;
use crate::datasources::remote::RemoteDatabases;
use crate::datasources::system::SystemDatabases;

pub fn try_create_catalog() -> Result<DatabaseCatalog> {
    let conf = Config::default();
    let catalog = DatabaseCatalog::try_create_with_config(conf.clone())?;
    // Register local/system and remote database engine.
    if conf.store.disable_local_database_engine == "0" {
        catalog.register_db_engine("local", Arc::new(LocalDatabases::create(conf.clone())))?;
    }
    catalog.register_db_engine("system", Arc::new(SystemDatabases::create(conf.clone())))?;
    catalog.register_db_engine("remote", Arc::new(RemoteDatabases::create(conf.clone())))?;

    Ok(catalog)
}
