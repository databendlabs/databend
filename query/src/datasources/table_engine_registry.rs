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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;

use crate::datasources::table_engine::TableFactory;

/// Registry of Table Providers
pub struct TableEngineRegistry {
    engines: RwLock<HashMap<String, Arc<dyn TableFactory>>>,
}

impl TableEngineRegistry {
    pub fn new() -> Self {
        Self {
            engines: Default::default(),
        }
    }

    pub fn register(
        &self,
        engine: impl Into<String>,
        provider: Arc<dyn TableFactory>,
    ) -> Result<()> {
        let engine_name = engine.into().to_uppercase();
        let mut w = self.engines.write();

        if let Entry::Vacant(e) = w.entry(engine_name.clone()) {
            e.insert(provider);
            Ok(())
        } else {
            Err(ErrorCode::DuplicatedTableEngineProvider(format!(
                "table engine provider {} already exist",
                engine_name
            )))
        }
    }

    pub fn engine_provider(&self, table_engine: impl AsRef<str>) -> Option<Arc<dyn TableFactory>> {
        let name = table_engine.as_ref().to_uppercase();
        self.engines.read().get(&name).cloned()
    }
}
