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

use arrow_schema::Field;
use arrow_schema::Schema;
use async_trait::async_trait;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableDataType::Null;
use databend_common_expression::TableDataType::Timestamp;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType::Float32;
use databend_common_expression::types::NumberDataType::Float64;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::TablePartition;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_types::SeqV;
use databend_storages_common_cache::LoadParams;
use educe::Educe;
use iceberg::TableCreation;
use iceberg::TableIdent;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::spec::PartitionSpec;
use iceberg::spec::Schema as IceSchema;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::spec::Transform;
use iceberg::spec::UnboundPartitionField;

use crate::IcebergMutableCatalog;
use crate::cache;

const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct IcebergDatabase {
    ctl: IcebergMutableCatalog,

    info: DatabaseInfo,
    ident: iceberg::NamespaceIdent,
}

impl IcebergDatabase {
    pub fn create(ctl: IcebergMutableCatalog, name: &str) -> Self {
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
        let params = LoadParams {
            location: format!("{}{}{}", self.name(), cache::SEP_STR, table_name),
            len_hint: None,
            ver: 0,
            put_cache: true,
        };
        let reader =
            super::cache::iceberg_table_cache_reader(self.ctl.iceberg_catalog(), self.ctl.info());
        reader.read(&params).await.map(|v| v.0.clone())
    }

    // trigger use will refresh the table meta cache in current databases
    #[async_backtrace::framed]
    async fn trigger_use(&self) -> Result<()> {
        let _ = self.list_tables().await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn refresh_table(&self, table_name: &str) -> Result<()> {
        let params = LoadParams {
            location: format!("{}{}{}", self.name(), cache::SEP_STR, table_name),
            len_hint: None,
            ver: 0,
            put_cache: true,
        };
        let reader =
            super::cache::iceberg_table_cache_reader(self.ctl.iceberg_catalog(), self.ctl.info());
        let _ = reader.refresh(&params).await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn refresh_database(&self) -> Result<()> {
        let table_names = self
            .ctl
            .iceberg_catalog()
            .list_tables(&self.ident)
            .await
            .map_err(|err| {
                ErrorCode::UnknownException(format!("Iceberg list tables failed: {err:?}"))
            })?;

        for name in table_names {
            let _ = self.refresh_table(&name.name).await?;
        }
        Ok(())
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
    async fn mget_tables(&self, table_names: &[String]) -> Result<Vec<Arc<dyn Table>>> {
        let mut tables = vec![];
        for table_name in table_names {
            if let Ok(table) = self.get_table(table_name).await {
                tables.push(table);
            }
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
                if let Ok(exists) = self.ctl.iceberg_catalog().table_exists(&table_name).await {
                    if exists {
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
                    temp_prefix: "".to_string(),
                })
                .await?;
            }
        }

        let schema = convert_table_schema(
            req.table_meta.schema.as_ref(),
            req.db_name(),
            req.table_name(),
        )?;

        let properties = if let Some(table_properties) = &req.table_properties {
            table_properties.clone().into_iter().collect()
        } else {
            HashMap::new()
        };

        let table_create_option = build_table_creation_option(&req, properties, schema)?;

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
            let params = LoadParams {
                location: format!("{}{}{}", self.name(), cache::SEP_STR, table_name),
                len_hint: None,
                ver: 0,
                put_cache: true,
            };
            let reader = super::cache::iceberg_table_cache_reader(
                self.ctl.iceberg_catalog(),
                self.ctl.info(),
            );

            reader.remove(&params);
            Ok(DropTableReply {})
        }
    }
}

fn convert_table_schema(
    schema: &TableSchema,
    db_name: &str,
    table_name: &str,
) -> Result<IcebergSchema> {
    let mut fields = vec![];
    for f in schema.fields() {
        let mut metadata = HashMap::new();
        metadata.insert(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            f.column_id.to_string(),
        );
        fields.push(Field::from(f).with_metadata(metadata));
    }
    let metadata = schema
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let arrow_schema = Schema::new(fields).with_metadata(metadata);
    let schema = arrow_schema_to_schema(&arrow_schema).map_err(|err| {
        ErrorCode::Internal(format!(
            "Iceberg create table {}.{} failed: {err:?}",
            db_name, table_name
        ))
    })?;

    Ok(schema) // Return the converted Iceberg schema
}

fn is_invalid_partition_type(ty: &TableDataType) -> bool {
    match ty {
        TableDataType::Number(Float32)
        | TableDataType::Number(Float64)
        | TableDataType::Decimal(_)
        | Timestamp
        | Null => true,
        TableDataType::Nullable(inner_ty) => is_invalid_partition_type(inner_ty),
        _ => false,
    }
}

fn build_identity_partition_spec(
    columns: &[String],
    req_schema: &TableSchema,
    ice_schema: &IceSchema,
) -> Result<PartitionSpec> {
    let mut fields = vec![];

    for (i, col) in columns.iter().enumerate() {
        // Use req_schema for type validation
        let origin_ty = &req_schema.field_with_name(col)?.data_type;
        if is_invalid_partition_type(origin_ty) {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "Partition key {} is {:?} type. Cannot set FLOAT, DOUBLE, DECIMAL, DATETIME as partition field",
                col, origin_ty
            )));
        }

        let field = ice_schema.field_by_name(col.as_str());
        if let Some(field) = field {
            fields.push(UnboundPartitionField {
                source_id: field.id,
                name: field.name.to_string(),
                field_id: Some(i as i32),
                transform: Transform::Identity,
            });
        } else {
            return Err(ErrorCode::Internal(format!(
                "Can not get partition field '{}' in Iceberg schema.",
                col
            )));
        }
    }

    Ok(PartitionSpec::builder(ice_schema.clone())
        .with_spec_id(1)
        .add_unbound_fields(fields)
        .unwrap()
        .build()
        .unwrap())
}

fn build_table_creation_option(
    req: &CreateTableReq,
    properties: HashMap<String, String>,
    schema: IceSchema,
) -> Result<TableCreation> {
    let builder = TableCreation::builder()
        .name(req.table_name().to_string())
        .properties(properties)
        .schema(schema.clone());

    if let Some(ref partition) = req.table_partition {
        match partition {
            TablePartition::Identity { columns } => {
                let spec = build_identity_partition_spec(columns, &req.table_meta.schema, &schema)?;
                Ok(builder.partition_spec(spec).build())
            }
        }
    } else {
        Ok(builder.build())
    }
}
