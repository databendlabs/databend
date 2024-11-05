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

use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;

use crate::storages::StorageFactory;

/// Database Context.
#[derive(Clone)]
pub struct DatabaseContext {
    pub meta: MetaStore,
    pub storage_factory: Arc<StorageFactory>,
    pub tenant: Tenant,
    pub disable_table_info_refresh: bool,
}

impl DatabaseContext {
    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }
}
