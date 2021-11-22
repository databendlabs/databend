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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListTableReq;
use common_meta_types::TableInfo;

use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::datasources::context::DataSourceContext;

#[derive(Clone)]
pub struct FuseDatabase {
    db_name: String,
    ctx: DataSourceContext,
}

impl FuseDatabase {
    pub fn new(db_name: impl Into<String>, ctx: DataSourceContext) -> Self {
        Self {
            db_name: db_name.into(),
            ctx,
        }
    }

    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let engine = table_info.engine();
        let factory = self
            .ctx
            .table_engine_registry
            .get_table_factory(engine)
            .ok_or_else(|| {
                ErrorCode::UnknownTableEngine(format!("unknown table engine {}", engine))
            })?;

        let tbl: Arc<dyn Table> = factory
            .try_create(table_info.clone(), self.ctx.clone())?
            .into();

        Ok(tbl)
    }
}

#[async_trait::async_trait]
impl Database for FuseDatabase {
    fn name(&self) -> &str {
        &self.db_name
    }

    async fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>> {
        let table_info = self
            .ctx
            .meta
            .get_table(GetTableReq::new(db_name, table_name))
            .await?;
        self.build_table(table_info.as_ref())
    }

    async fn list_tables(&self, db_name: &str) -> common_exception::Result<Vec<Arc<dyn Table>>> {
        let table_infos = self
            .ctx
            .meta
            .list_tables(ListTableReq::new(db_name))
            .await?;

        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.build_table(item.as_ref())?;
            acc.push(tbl);
            Ok(acc)
        })
    }

    async fn create_table(&self, req: CreateTableReq) -> common_exception::Result<()> {
        self.ctx.meta.create_table(req).await?;
        Ok(())
    }

    async fn drop_table(&self, req: DropTableReq) -> common_exception::Result<DropTableReply> {
        self.ctx.meta.drop_table(req).await
    }
}
