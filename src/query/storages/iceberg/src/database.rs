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

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use async_trait::async_trait;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
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
use databend_meta_client::types::SeqV;
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
        let cache_key =
            cache::table_cache_key(self.ctl.info().catalog_name(), self.name(), table_name);
        let reader =
            super::cache::iceberg_table_cache_reader(self.ctl.iceberg_catalog(), self.ctl.info());
        reader.read(&cache_key).await.map(|v| v.table())
    }

    // trigger use will refresh the table meta cache in current databases
    #[async_backtrace::framed]
    async fn trigger_use(&self) -> Result<()> {
        let _ = self.list_tables().await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn refresh_table(&self, table_name: &str) -> Result<()> {
        let cache_key =
            cache::table_cache_key(self.ctl.info().catalog_name(), self.name(), table_name);
        let reader =
            super::cache::iceberg_table_cache_reader(self.ctl.iceberg_catalog(), self.ctl.info());
        reader.refresh(&cache_key).await?;
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
                // `Catalog::table_exists` asks the REST catalog with a HEAD request, which older
                // REST servers (iceberg-rest 0.10 answers 400 to any HEAD) reject, so the table
                // is looked up instead: a GET works everywhere, and a failed lookup means there
                // is nothing to do.
                if self
                    .ctl
                    .iceberg_catalog()
                    .load_table(&table_name)
                    .await
                    .is_ok()
                {
                    return Ok(CreateTableReply {
                        table_id: 0,
                        table_id_seq: Some(0),
                        db_id: 0,
                        // The table is already there, so `CREATE TABLE IF NOT EXISTS` has
                        // nothing to do. Callers such as CTAS only skip their write when the
                        // reply says the table is not new; reporting a new table here made them
                        // append to the existing one.
                        new_table: false,
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
            // Iceberg tables are not managed by the meta service, but callers such as CTAS
            // expect the sequence number of the created table to be present.
            table_id_seq: Some(0),
            db_id: 0,
            new_table: true,
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
            let cache_key = cache::table_cache_key(
                self.ctl.info().catalog_name(),
                self.name(),
                &req.table_name,
            );
            let reader = super::cache::iceberg_table_cache_reader(
                self.ctl.iceberg_catalog(),
                self.ctl.info(),
            );

            reader.remove(&cache_key);
            Ok(DropTableReply {})
        }
    }
}

fn convert_table_schema(
    schema: &TableSchema,
    db_name: &str,
    table_name: &str,
) -> Result<IcebergSchema> {
    // Nested fields have no column id of their own, but iceberg requires a unique id on every
    // field of the schema, including struct members, list elements and map keys/values.
    let mut next_field_id = schema
        .fields()
        .iter()
        .map(|f| f.column_id)
        .max()
        .unwrap_or(0)
        + 1;
    let mut fields = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let field =
            databend_field_to_arrow(&f.name, &f.data_type, f.column_id, &mut next_field_id)?;
        fields.push(field.with_nullable(f.is_nullable() || f.data_type.is_nullable()));
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

/// Convert a databend field to an arrow field that [`arrow_schema_to_schema`] accepts.
///
/// Decimals are restricted to `Decimal128` and every field carries the field id that iceberg
/// expects in its metadata.
fn databend_field_to_arrow(
    name: &str,
    ty: &TableDataType,
    field_id: ColumnId,
    next_field_id: &mut ColumnId,
) -> Result<Field> {
    let arrow_ty = databend_type_to_arrow(ty, next_field_id)?;
    let metadata = HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())]);
    Ok(Field::new(name, arrow_ty, ty.is_nullable()).with_metadata(metadata))
}

fn next_nested_field(
    name: &str,
    ty: &TableDataType,
    next_field_id: &mut ColumnId,
) -> Result<Field> {
    let field_id = *next_field_id;
    *next_field_id += 1;
    databend_field_to_arrow(name, ty, field_id, next_field_id)
}

fn databend_type_to_arrow(
    ty: &TableDataType,
    next_field_id: &mut ColumnId,
) -> Result<ArrowDataType> {
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::NumberDataType;

    let arrow_ty = match ty {
        TableDataType::Boolean => ArrowDataType::Boolean,
        TableDataType::String => ArrowDataType::Utf8,
        TableDataType::Binary => ArrowDataType::Binary,
        TableDataType::Date => ArrowDataType::Date32,
        TableDataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        TableDataType::TimestampTz => {
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        TableDataType::Number(number) => match number {
            NumberDataType::Int8 | NumberDataType::Int16 | NumberDataType::Int32 => {
                ArrowDataType::Int32
            }
            NumberDataType::Int64 => ArrowDataType::Int64,
            NumberDataType::UInt8 | NumberDataType::UInt16 => ArrowDataType::Int32,
            NumberDataType::UInt32 => ArrowDataType::Int64,
            // Iceberg has no unsigned integer type; the full uint64 range fits this decimal.
            NumberDataType::UInt64 => ArrowDataType::Decimal128(20, 0),
            NumberDataType::Float32 => ArrowDataType::Float32,
            NumberDataType::Float64 => ArrowDataType::Float64,
        },
        TableDataType::Decimal(decimal) => {
            let (precision, scale) = match decimal {
                DecimalDataType::Decimal64(size) => (size.precision(), size.scale() as i8),
                DecimalDataType::Decimal128(size) => (size.precision(), size.scale() as i8),
                // Iceberg only supports decimals with a precision of at most 38.
                DecimalDataType::Decimal256(size) => (size.precision(), size.scale() as i8),
            };
            if precision > 38 {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "Iceberg does not support DECIMAL({precision}, {scale}): the precision must not exceed 38"
                )));
            }
            ArrowDataType::Decimal128(precision, scale)
        }
        TableDataType::Nullable(inner) => databend_type_to_arrow(inner, next_field_id)?,
        TableDataType::Array(inner) => {
            let element = next_nested_field("element", inner, next_field_id)?;
            ArrowDataType::List(Arc::new(element))
        }
        TableDataType::Map(inner) => {
            let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = inner.as_ref()
            else {
                return Err(ErrorCode::TableEngineNotSupported(
                    "Iceberg map type must have a key/value tuple".to_string(),
                ));
            };
            let key = next_nested_field(&fields_name[0], &fields_type[0], next_field_id)?;
            // Map keys are required in iceberg, and databend only allows non-null keys.
            let key = key.with_nullable(false);
            let value = next_nested_field(&fields_name[1], &fields_type[1], next_field_id)?;
            let entries = Field::new(
                "key_value",
                ArrowDataType::Struct(vec![key, value].into()),
                false,
            );
            ArrowDataType::Map(Arc::new(entries), false)
        }
        TableDataType::Tuple {
            fields_name,
            fields_type,
        } => {
            let mut fields = Vec::with_capacity(fields_name.len());
            for (name, ty) in fields_name.iter().zip(fields_type) {
                fields.push(next_nested_field(name, ty, next_field_id)?);
            }
            ArrowDataType::Struct(fields.into())
        }
        // Types without an iceberg equivalent keep the previous lossy mapping instead of
        // failing DDL that worked before.
        _ => {
            let field = Field::from(&databend_common_expression::TableField::new("", ty.clone()));
            field.data_type().clone()
        }
    };
    Ok(arrow_ty)
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

#[cfg(test)]
mod tests {
    use databend_common_expression::TableField;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;
    use iceberg::spec::PrimitiveType;
    use iceberg::spec::Type;

    use super::*;

    fn convert(fields: Vec<TableField>) -> IcebergSchema {
        convert_table_schema(&TableSchema::new(fields), "db", "t").unwrap()
    }

    #[test]
    fn test_decimal_columns_become_iceberg_decimals() {
        // Databend stores a decimal(10, 2) as Decimal64 in arrow, which iceberg does not map.
        for (decimal, precision, scale) in [
            (
                DecimalDataType::Decimal64(DecimalSize::new_unchecked(10, 2)),
                10,
                2,
            ),
            (
                DecimalDataType::Decimal128(DecimalSize::new_unchecked(20, 4)),
                20,
                4,
            ),
            (
                DecimalDataType::Decimal256(DecimalSize::new_unchecked(38, 6)),
                38,
                6,
            ),
        ] {
            let schema = convert(vec![TableField::new("x", TableDataType::Decimal(decimal))]);
            assert_eq!(
                schema.field_by_name("x").unwrap().field_type.as_ref(),
                &Type::Primitive(PrimitiveType::Decimal { precision, scale })
            );
        }
    }

    #[test]
    fn test_decimal_above_iceberg_precision_is_rejected() {
        let schema = TableSchema::new(vec![TableField::new(
            "x",
            TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize::new_unchecked(
                39, 2,
            ))),
        )]);
        let err = convert_table_schema(&schema, "db", "t").unwrap_err();
        assert!(
            err.message().contains("precision must not exceed 38"),
            "{err}"
        );
    }

    #[test]
    fn test_nested_columns_get_unique_field_ids() {
        let schema = convert(vec![
            TableField::new("st", TableDataType::Tuple {
                fields_name: vec!["a".to_string(), "b".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int64),
                    TableDataType::String,
                ],
            }),
            TableField::new(
                "li",
                TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::Int64))),
            ),
            TableField::new(
                "mp",
                TableDataType::Map(Box::new(TableDataType::Tuple {
                    fields_name: vec!["key".to_string(), "value".to_string()],
                    fields_type: vec![
                        TableDataType::String,
                        TableDataType::Number(NumberDataType::Int64),
                    ],
                })),
            ),
        ]);

        let Type::Struct(st) = schema.field_by_name("st").unwrap().field_type.as_ref() else {
            panic!("expected a struct column");
        };
        assert_eq!(st.fields().len(), 2);
        assert_eq!(
            st.fields()[0].field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );

        let Type::List(li) = schema.field_by_name("li").unwrap().field_type.as_ref() else {
            panic!("expected a list column");
        };
        assert_eq!(
            li.element_field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );

        let Type::Map(mp) = schema.field_by_name("mp").unwrap().field_type.as_ref() else {
            panic!("expected a map column");
        };
        assert!(mp.key_field.required, "iceberg map keys are required");
        assert_eq!(
            mp.value_field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );

        // `arrow_schema_to_schema` rejects duplicate ids, so a conversion that assigned the
        // same id to two nested fields would already have failed above. Check the ids are
        // distinct anyway, including against the top-level columns.
        let mut ids: Vec<i32> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.id)
            .chain(std::iter::once(st.fields()[0].id))
            .chain(std::iter::once(st.fields()[1].id))
            .chain(std::iter::once(li.element_field.id))
            .chain(std::iter::once(mp.key_field.id))
            .chain(std::iter::once(mp.value_field.id))
            .collect();
        let total = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), total, "nested field ids must be unique");
    }
}
