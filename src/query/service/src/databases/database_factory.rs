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

use dashmap::DashMap;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DatabaseInfo;

use crate::databases::default::DefaultDatabase;
use crate::databases::share::ShareDatabase;
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
    creators: DashMap<String, Arc<dyn DatabaseCreator>>,
}

impl DatabaseFactory {
    pub fn create(_: InnerConfig) -> Self {
        let creators: DashMap<String, Arc<dyn DatabaseCreator>> = DashMap::new();
        creators.insert(
            DefaultDatabase::NAME.to_string(),
            Arc::new(DefaultDatabase::try_create),
        );
        creators.insert(
            ShareDatabase::NAME.to_string(),
            Arc::new(ShareDatabase::try_create),
        );

        DatabaseFactory { creators }
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

        let factory = self.creators.get(&engine).ok_or_else(|| {
            ErrorCode::UnknownDatabaseEngine(format!("Unknown database engine {}", engine))
        })?;

        let db: Arc<dyn Database> = factory.try_create(ctx, db_info.clone())?.into();
        Ok(db)
    }
}
