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

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::TableApi;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;

use super::shared_table::SharedTable;
use crate::databases::Database;
use crate::databases::DatabaseContext;
use crate::meta_service_error;
use crate::share::ShareDatabaseBinding;
use crate::share::ShareTableContext;
use crate::share::share_service;

#[derive(Clone)]
pub struct SharedDatabase {
    ctx: DatabaseContext,
    db_info: DatabaseInfo,
}

impl SharedDatabase {
    pub const NAME: &'static str = "SHARE";

    pub fn try_create(ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>> {
        ShareDatabaseBinding::from_engine_options(&db_info.meta.engine_options)?;
        Ok(Box::new(Self { ctx, db_info }))
    }

    fn binding(&self) -> Result<ShareDatabaseBinding> {
        ShareDatabaseBinding::from_engine_options(&self.db_info.meta.engine_options)
    }

    async fn table_info(&self, context: &ShareTableContext) -> Result<TableInfo> {
        let name_ident = DBIdTableName::new(context.provider_database_id, &context.provider_table);
        let table_niv = self
            .ctx
            .meta
            .get_table_in_db(&name_ident)
            .await
            .map_err(meta_service_error)?;

        let Some(table_niv) = table_niv else {
            return Err(AppError::from(UnknownTable::new(
                &context.provider_table,
                format!(
                    "shared table '{}'.'{}'",
                    context.provider_database, context.provider_table
                ),
            ))
            .into());
        };

        let (_name, id, seq_meta) = table_niv.unpack();
        if id.table_id != context.provider_table_id {
            return Err(ErrorCode::InvalidOperation(format!(
                "Shared table binding is stale: expected provider table id {}, got {}",
                context.provider_table_id, id.table_id
            )));
        }

        Ok(TableInfo {
            ident: TableIdent {
                table_id: id.table_id,
                seq: seq_meta.seq,
            },
            desc: format!(
                "'{}'.'{}'",
                context.provider_database, context.provider_table
            ),
            name: context.provider_table.clone(),
            meta: seq_meta.data,
            db_type: DatabaseType::NormalDB,
            catalog_info: Default::default(),
        })
    }

    async fn shared_table(&self, context: ShareTableContext) -> Result<Arc<dyn Table>> {
        let table_info = self.table_info(&context).await?;
        SharedTable::try_create(self.ctx.clone(), self.get_db_name(), &context, table_info).await
    }
}

#[async_trait::async_trait]
impl Database for SharedDatabase {
    fn name(&self) -> &str {
        self.db_info.name_ident.database_name()
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }

    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let binding = self.binding()?;
        let context = share_service()
            .resolve_shared_table(self.get_tenant(), &binding, table_name)
            .await?;
        self.shared_table(context).await
    }

    async fn mget_tables(&self, table_names: &[String]) -> Result<Vec<Arc<dyn Table>>> {
        let mut tables = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            match self.get_table(table_name).await {
                Ok(table) => tables.push(table),
                Err(err) if err.code() == ErrorCode::UnknownTable("").code() => {}
                Err(err) => return Err(err),
            }
        }
        Ok(tables)
    }

    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let binding = self.binding()?;
        let contexts = share_service()
            .list_shared_tables(self.get_tenant(), &binding)
            .await?;
        let mut tables = Vec::with_capacity(contexts.len());
        for context in contexts {
            tables.push(self.shared_table(context).await?);
        }
        Ok(tables)
    }

    async fn list_tables_names(&self) -> Result<Vec<String>> {
        let binding = self.binding()?;
        let contexts = share_service()
            .list_shared_tables(self.get_tenant(), &binding)
            .await?;
        Ok(contexts
            .into_iter()
            .map(|context| context.provider_table)
            .collect())
    }
}
