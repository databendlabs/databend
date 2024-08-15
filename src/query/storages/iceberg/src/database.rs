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

//! Wrapping of the parent directory containing iceberg tables

use std::sync::Arc;

use async_trait::async_trait;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::DatabaseIdent;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::tenant::Tenant;

use crate::table::IcebergTable;
use crate::IcebergCatalog;

#[derive(Clone, Debug)]
pub struct IcebergDatabase {
    ctl: IcebergCatalog,

    info: DatabaseInfo,
    ident: iceberg::NamespaceIdent,
}

impl IcebergDatabase {
    pub fn create(ctl: IcebergCatalog, name: &str) -> Self {
        let ident = iceberg::NamespaceIdent::new(name.to_string());
        let info = DatabaseInfo {
            ident: DatabaseIdent { db_id: 0, seq: 0 },
            name_ident: DatabaseNameIdent::new(Tenant::new_literal("dummy"), name),
            meta: DatabaseMeta {
                engine: "iceberg".to_string(),
                created_on: chrono::Utc::now(),
                updated_on: chrono::Utc::now(),
                ..Default::default()
            },
        };

        Self { ctl, info, ident }
    }
}

#[async_trait]
impl Database for IcebergDatabase {
    fn name(&self) -> &str {
        self.info.name_ident.database_name()
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.info
    }

    #[async_backtrace::framed]
    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let tbl = IcebergTable::try_create_from_iceberg_catalog(
            self.ctl.clone(),
            self.info.name_ident.database_name(),
            table_name,
        )
        .await?;
        let tbl = Arc::new(tbl) as Arc<dyn Table>;

        Ok(tbl)
    }

    #[async_backtrace::framed]
    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let table_names = self
            .ctl
            .iceberg_catalog()
            .list_tables(&self.ident)
            .await
            .map_err(|err| {
                ErrorCode::UnknownException(format!("Iceberg list tables failed: {err:?}"))
            })?;

        let mut tables = vec![];

        for table_name in table_names {
            let table = self.get_table(&table_name.name).await?;
            tables.push(table);
        }
        Ok(tables)
    }
}
