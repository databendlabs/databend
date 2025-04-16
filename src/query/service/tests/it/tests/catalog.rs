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

use databend_common_exception::Result;
use databend_query::catalogs::DatabaseCatalog;
use databend_query::table_functions::TableFunctionFactoryRegistry;

pub async fn create_catalog() -> Result<DatabaseCatalog> {
    let conf = databend_query::test_kits::ConfigBuilder::create().config();
    DatabaseCatalog::try_create_with_config(conf, Arc::new(TableFunctionFactoryRegistry::create()))
        .await
}
