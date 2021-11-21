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

use std::sync::Arc;

use common_dal::InMemoryData;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_api::MetaApi;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::DatabaseInfo;
use common_meta_types::DropDatabaseReq;

use crate::catalogs::catalog::Catalog1;
use crate::catalogs::database::Database1;
use crate::datasources::database::fuse::database::FuseDatabase;
use crate::datasources::table_engine_registry::TableEngineRegistry;

/// Catalog based on MetaStore
/// - System Database NOT included
/// - Meta data of databases are saved in meta store
/// - Instances of `Database` are created by using database factories according to the engine
/// - Database engines are free to save table meta in metastore or not
#[derive(Clone)]
pub struct MutableCatalog {
    meta: Arc<dyn MetaApi>,
    table_engine_registry: Arc<TableEngineRegistry>,
    in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
}

impl MutableCatalog {
    pub fn create(
        meta: Arc<dyn MetaApi>,
        in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
        table_engine_registry: Arc<TableEngineRegistry>,
    ) -> Self {
        MutableCatalog {
            meta,
            table_engine_registry,
            in_memory_data,
        }
    }

    fn build_db_instance(&self, db_info: &Arc<DatabaseInfo>) -> Result<Arc<dyn Database1>> {
        // TODO(bohu): Add the database engine match, now we set only one fuse database here.
        let db = FuseDatabase::new(
            &db_info.db,
            self.meta.clone(),
            self.in_memory_data.clone(),
            self.table_engine_registry.clone(),
        );
        let db = Arc::new(db);
        Ok(db)
    }
}

#[async_trait::async_trait]
impl Catalog1 for MutableCatalog {
    async fn get_databases(&self) -> Result<Vec<Arc<dyn Database1>>> {
        let dbs = self.meta.get_databases().await?;

        dbs.iter().try_fold(vec![], |mut acc, item| {
            let db = self.build_db_instance(item)?;
            acc.push(db);
            Ok(acc)
        })
    }

    async fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database1>> {
        let db_info = self.meta.get_database(db_name).await?;
        self.build_db_instance(&db_info)
    }

    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        self.meta.create_database(req).await
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<()> {
        self.meta.drop_database(req).await?;
        Ok(())
    }
}
