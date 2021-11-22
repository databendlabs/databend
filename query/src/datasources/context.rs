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

use common_dal::InMemoryData;
use common_infallible::RwLock;
use common_meta_api::MetaApi;

use crate::datasources::database_engine_registry::DatabaseEngineRegistry;
use crate::datasources::table_engine_registry::TableEngineRegistry;

/// Datasource Context.
#[derive(Clone)]
pub struct DataSourceContext {
    pub meta: Arc<dyn MetaApi>,
    pub in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
    pub table_engine_registry: Arc<TableEngineRegistry>,
    pub database_engine_registry: Arc<DatabaseEngineRegistry>,
}
