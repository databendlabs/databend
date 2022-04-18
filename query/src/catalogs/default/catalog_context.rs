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

use common_datablocks::InMemoryData;
use common_infallible::RwLock;
use common_meta_api::MetaApi;

use crate::databases::DatabaseFactory;
use crate::storages::StorageFactory;

#[derive(Clone)]
pub struct CatalogContext {
    pub meta: Arc<dyn MetaApi>,
    pub storage_factory: Arc<StorageFactory>,
    pub database_factory: Arc<DatabaseFactory>,
    pub in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
}
