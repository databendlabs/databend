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
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_api::MetaApi;
use common_meta_types::CreateTableReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;

use crate::catalogs::Database1;
use crate::catalogs::Table;
use crate::datasources::context::TableContext;
use crate::datasources::table_engine_registry::TableEngineRegistry;

#[derive(Clone)]
pub struct FuseDatabase {
    db_name: String,
    meta: Arc<dyn MetaApi>,
    in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
    table_engine_registry: Arc<TableEngineRegistry>,
}

impl FuseDatabase {
    pub fn new(
        db_name: impl Into<String>,
        meta: Arc<dyn MetaApi>,
        in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
        table_engine_registry: Arc<TableEngineRegistry>,
    ) -> Self {
        Self {
            db_name: db_name.into(),
            meta,
            in_memory_data,
            table_engine_registry,
        }
    }

    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let engine = table_info.engine();
        let factory = self
            .table_engine_registry
            .get_table_factory(engine)
            .ok_or_else(|| {
                ErrorCode::UnknownTableEngine(format!("unknown table engine {}", engine))
            })?;

        let tbl: Arc<dyn Table> = factory
            .try_create(table_info.clone(), TableContext {
                in_memory_data: self.in_memory_data.clone(),
            })?
            .into();

        Ok(tbl)
    }
}

#[async_trait::async_trait]
impl Database1 for FuseDatabase {
    fn name(&self) -> &str {
        &self.db_name
    }

    async fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>> {
        let table_info = self.meta.get_table(db_name, table_name).await?;
        self.build_table(table_info.as_ref())
    }

    async fn get_tables(&self, db_name: &str) -> common_exception::Result<Vec<Arc<dyn Table>>> {
        let table_infos = self.meta.get_tables(db_name).await?;

        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.build_table(item.as_ref())?;
            acc.push(tbl);
            Ok(acc)
        })
    }

    async fn get_table_meta_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        self.meta.get_table_by_id(table_id).await
    }

    async fn create_table(&self, req: CreateTableReq) -> common_exception::Result<()> {
        self.meta.create_table(req).await?;
        Ok(())
    }

    async fn drop_table(&self, req: DropTableReq) -> common_exception::Result<DropTableReply> {
        self.meta.drop_table(req).await
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> common_exception::Result<UpsertTableOptionReply> {
        self.meta.upsert_table_option(req).await
    }
}
