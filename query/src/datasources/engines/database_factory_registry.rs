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

use crate::datasources::engines::database_factory::DatabaseFactory;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct EngineDescription {
    pub name: String,
    pub desc: String,
}

/// Registry of Table Providers
pub struct DatabaseEngineRegistry {
    engines: RwLock<HashMap<String, Arc<dyn DatabaseFactory>>>,
}

impl DatabaseEngineRegistry {
    pub fn new() -> Self {
        Self {
            engines: Default::default(),
        }
    }

    pub fn engine_names(&self) -> Vec<String> {
        self.engines
            .read()
            .iter()
            .map(|(k, _v)| k.to_string())
            .collect::<Vec<_>>()
    }

    pub fn contains(&self, engine: &str) -> bool {
        self.engines.read().contains_key(engine)
    }

    pub fn register(
        &self,
        engine: impl Into<String>,
        provider: Arc<dyn DatabaseFactory>,
    ) -> Result<()> {
        let engine_name = engine.into();
        let mut w = self.engines.write();

        if let Entry::Vacant(e) = w.entry(engine_name.clone()) {
            e.insert(provider);
            Ok(())
        } else {
            Err(ErrorCode::DuplicatedDatabaseEngineProvider(format!(
                "database engine provider {} already exist",
                engine_name
            )))
        }
    }

    pub fn engine_provider(
        &self,
        table_engine: impl AsRef<str>,
    ) -> Option<Arc<dyn DatabaseFactory>> {
        self.engines.read().get(table_engine.as_ref()).cloned()
    }
    pub fn descriptions(&self) -> Vec<EngineDescription> {
        self.engines
            .read()
            .iter()
            .map(|(name, item)| EngineDescription {
                name: name.clone(),
                desc: item.description(),
            })
            .collect::<Vec<_>>()
    }
}
