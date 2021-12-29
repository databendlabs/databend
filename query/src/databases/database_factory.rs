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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::DatabaseInfo;

use crate::configs::Config;
use crate::databases::default::DefaultDatabase;
use crate::databases::github::GithubDatabase;
use crate::databases::Database;
use crate::databases::DatabaseContext;

pub trait DatabaseCreator: Send + Sync {
    fn try_create(&self, ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>>;
}

impl<T> DatabaseCreator for T
where
    T: Fn(DatabaseContext, DatabaseInfo) -> Result<Box<dyn Database>>,
    T: Send + Sync,
{
    fn try_create(&self, ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>> {
        self(ctx, db_info)
    }
}

#[derive(Default)]
pub struct DatabaseFactory {
    creators: RwLock<HashMap<String, Arc<dyn DatabaseCreator>>>,
}

impl DatabaseFactory {
    pub fn create(conf: Config) -> Self {
        let mut creators: HashMap<String, Arc<dyn DatabaseCreator>> = Default::default();
        creators.insert("DEFAULT".to_string(), Arc::new(DefaultDatabase::try_create));
        if conf.query.database_engine_github_enabled {
            creators.insert("GITHUB".to_string(), Arc::new(GithubDatabase::try_create));
        }

        DatabaseFactory {
            creators: RwLock::new(creators),
        }
    }

    pub fn get_database(
        &self,
        ctx: DatabaseContext,
        db_info: &DatabaseInfo,
    ) -> Result<Arc<dyn Database>> {
        let db_engine = &db_info.engine();
        let engine = if db_engine.is_empty() {
            "DEFAULT".to_string()
        } else {
            db_engine.to_uppercase()
        };

        let lock = self.creators.read();
        let factory = lock.get(&engine).ok_or_else(|| {
            ErrorCode::UnknownDatabaseEngine(format!("Unknown database engine {}", engine))
        })?;

        let db: Arc<dyn Database> = factory.try_create(ctx, db_info.clone())?.into();
        Ok(db)
    }
}
