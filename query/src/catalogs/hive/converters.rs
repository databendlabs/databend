// Copyright 2022 Datafuse Labs.
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

use common_datavalues::chrono::Utc;
use common_datavalues::type_primitive::Float32Type;
use common_datavalues::type_primitive::Float64Type;
use common_datavalues::type_primitive::Int16Type;
use common_datavalues::type_primitive::Int32Type;
use common_datavalues::type_primitive::Int64Type;
use common_datavalues::type_primitive::Int8Type;
use common_datavalues::type_string::StringType;
use common_datavalues::ArrayType;
use common_datavalues::BooleanType;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataTypeImpl;
use common_datavalues::DateType;
use common_datavalues::NullableType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hive_meta_store as hms;
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use super::hive_database::HiveDatabase;
use super::hive_database::HIVE_DATABASE_ENGIE;
use super::hive_table::HIVE_TABLE_ENGIE;
use super::hive_table_options::HiveTableOptions;

/// ! Skeleton of mappers
impl From<hms::Database> for HiveDatabase {
    fn from(hms_database: hms::Database) -> Self {
        HiveDatabase {
            database_info: DatabaseInfo {
                ident: DatabaseIdent { db_id: 0, seq: 0 },
                name_ident: DatabaseNameIdent {
                    tenant: "TODO".to_owned(),
                    db_name: hms_database.name.unwrap_or_default(),
                },
                meta: DatabaseMeta {
                    engine: HIVE_DATABASE_ENGIE.to_owned(),
                    created_on: Utc::now(),
                    ..Default::default()
                },
            },
        }
    }
}

pub fn try_into_table_info(
    hms_table: hms::Table,
    fields: Vec<hms::FieldSchema>,
) -> Result<TableInfo> {
    let schema = Arc::new(try_into_schema(fields)?);
    let partition_keys = if let Some(partitions) = &hms_table.partition_keys {
        let r = partitions
            .iter()
            .filter_map(|field| field.name.clone())
            .collect();
        Some(r)
    } else {
        None
    };

    let location = if let Some(storage) = &hms_table.sd {
        storage
            .location
            .as_ref()
            .map(|location| location.to_string())
    } else {
        None
    };

    let table_options = HiveTableOptions {
        partition_keys,
        location,
    };

    let meta = TableMeta {
        schema,
        engine: HIVE_TABLE_ENGIE.to_owned(),
        engine_options: table_options.into(),
        created_on: Utc::now(),
        ..Default::default()
    };

    let table_info = TableInfo {
        ident: TableIdent {
            table_id: 0,
            seq: 0,
        },
        desc: "".to_owned(),
        name: hms_table.table_name.unwrap_or_default(),
        meta,
    };

    Ok(table_info)
}

fn try_into_schema(hive_fields: Vec<hms::FieldSchema>) -> Result<DataSchema> {
    let mut fields = Vec::new();
    for field in hive_fields {
        let name = field.name.unwrap_or_default();
        let type_name = field.type_.unwrap_or_default();
        let null_field = NullableType::new_impl(try_from_filed_type_name(type_name)?);
        let field = DataField::new(&name, null_field);
        fields.push(field);
    }
    Ok(DataSchema::new(fields))
}

fn try_from_filed_type_name(type_name: impl AsRef<str>) -> Result<DataTypeImpl> {
    let name = type_name.as_ref().to_uppercase();
    // TODO more mappings goes here
    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types

    // Hive string data type could be varchar(n), where n is the maximum number of characters
    if name.starts_with("VARCHAR") {
        Ok(DataTypeImpl::String(StringType::default()))
    } else if name.starts_with("ARRAY<") {
        let sub_type = &name["ARRAY<".len()..name.len() - 1];
        let sub_type = try_from_filed_type_name(sub_type)?;
        Ok(DataTypeImpl::Array(ArrayType::create(
            NullableType::new_impl(sub_type),
        )))
    } else {
        match name.as_str() {
            "TINYINT" => Ok(DataTypeImpl::Int8(Int8Type::default())),
            "SMALLINT" => Ok(DataTypeImpl::Int16(Int16Type::default())),
            "INT" => Ok(DataTypeImpl::Int32(Int32Type::default())),
            "BIGINT" => Ok(DataTypeImpl::Int64(Int64Type::default())),

            "BINARY" | "STRING" => Ok(DataTypeImpl::String(StringType::default())),
            // boolean
            "BOOLEAN" => Ok(DataTypeImpl::Boolean(BooleanType::default())),

            //"DECIMAL", "NUMERIC" type not supported
            "FLOAT" => Ok(DataTypeImpl::Float32(Float32Type::default())),
            "DOUBLE" => Ok(DataTypeImpl::Float64(Float64Type::default())),
            "DOUBLE PRECISION" => Ok(DataTypeImpl::Float64(Float64Type::default())),

            // timestamp
            // "TIMESTAMP" => Ok(DataTypeImpl::Timestamp(TimestampType::create(3))),
            "DATE" => Ok(DataTypeImpl::Date(DateType::default())),

            _ => Err(ErrorCode::IllegalDataType(format!(
                "unknown hive data type [{}]",
                name
            ))),
        }
    }
}
