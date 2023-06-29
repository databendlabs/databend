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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::LazyLock;

use common_catalog::table::Table;
use dashmap::DashMap;

/// FIXME
///
/// Maintains a global context for iceberg tables is definitely not a good
/// idea. We use this to make the iceberg table work with the current
/// codebase.
pub static ICEBERG_CONTEXT: LazyLock<IcebergContext> = LazyLock::new(|| IcebergContext::default());

#[derive(Default, Clone)]
pub struct IcebergContext {
    /// table.info.desc -> Table instance
    tables: DashMap<String, Arc<dyn Table>>,
}

impl Debug for IcebergContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergContext").finish_non_exhaustive()
    }
}

impl IcebergContext {
    pub fn insert(&self, id: &str, table: Arc<dyn Table>) {
        self.tables.insert(id.to_string(), table);
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn Table>> {
        self.tables.get(id).map(|v| v.value().clone())
    }
}
