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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::seq_value::SeqV;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::TableCreation;
use iceberg::TableIdent;

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
            database_id: DatabaseId::new(0),
            name_ident: DatabaseNameIdent::new(Tenant::new_literal("dummy"), name),
            meta: SeqV::new(0, DatabaseMeta {
                engine: "iceberg".to_string(),
                created_on: chrono::Utc::now(),
                updated_on: chrono::Utc::now(),
                ..Default::default()
            }),
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

    #[async_backtrace::framed]
    async fn list_tables_names(&self) -> Result<Vec<String>> {
        let table_names = self
            .ctl
            .iceberg_catalog()
            .list_tables(&self.ident)
            .await
            .map_err(|err| {
                ErrorCode::UnknownException(format!("Iceberg list tables failed: {err:?}"))
            })?;

        Ok(table_names
            .into_iter()
            .map(|table_name| table_name.name.to_string())
            .collect())
    }

    // create iceberg_catalog.db.t (col_name type);
    #[async_backtrace::framed]
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        let table_name = TableIdent::new(self.ident.clone(), req.table_name().to_string());

        match req.create_option {
            CreateOption::Create => {}
            CreateOption::CreateIfNotExists => {
                if self
                    .ctl
                    .iceberg_catalog()
                    .table_exists(&table_name)
                    .await
                    .map_err(|err| {
                        ErrorCode::Internal(format!(
                            "Iceberg create table {}.{} failed: {err:?}",
                            req.db_name(),
                            req.table_name()
                        ))
                    })?
                {
                    return Ok(CreateTableReply {
                        table_id: 0,
                        table_id_seq: None,
                        db_id: 0,
                        new_table: true,
                        spec_vec: None,
                        prev_table_id: None,
                        orphan_table_name: None,
                    });
                }
            }
            CreateOption::CreateOrReplace => {
                self.drop_table_by_id(DropTableByIdReq {
                    if_exists: true,
                    tenant: req.tenant().clone(),
                    tb_id: 0,
                    table_name: req.table_name().to_string(),
                    db_id: 0,
                    db_name: req.db_name().to_string(),
                    engine: req.table_meta.engine.to_string(),
                    session_id: "".to_string(),
                })
                .await?;
            }
        }

        let arrow_schema: arrow_schema::Schema = req.table_meta.schema.as_ref().into();
        let table_create_option = TableCreation::builder()
            .name(req.table_name().to_string())
            .properties(HashMap::new())
            .schema(arrow_schema_to_schema(&arrow_schema).map_err(|err| {
                ErrorCode::Internal(format!(
                    "Iceberg create table {}.{} failed: {err:?}",
                    req.db_name(),
                    req.table_name()
                ))
            })?)
            .build();

        let _ = self
            .ctl
            .iceberg_catalog()
            .create_table(&self.ident, table_create_option)
            .await
            .map_err(|err| {
                ErrorCode::Internal(format!(
                    "Iceberg create table {}.{} failed: {err:?}",
                    req.db_name(),
                    req.table_name()
                ))
            })?;
        Ok(CreateTableReply {
            table_id: 0,
            table_id_seq: None,
            db_id: 0,
            new_table: true,
            spec_vec: None,
            prev_table_id: None,
            orphan_table_name: None,
        })
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        let table_name = TableIdent::new(self.ident.clone(), req.table_name.to_string());
        if let Err(err) = self
            .ctl
            .iceberg_catalog()
            .drop_table(&table_name)
            .await
            .map_err(|err| {
                ErrorCode::Internal(format!(
                    "Iceberg drop table {}.{} failed: {err:?}",
                    req.db_name, req.table_name
                ))
            })
        {
            if req.if_exists {
                Ok(DropTableReply {})
            } else {
                Err(err)
            }
        } else {
            Ok(DropTableReply {})
        }
    }
}
