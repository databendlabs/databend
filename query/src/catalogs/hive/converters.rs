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
use common_datavalues::type_primitive::Int32Type;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hive_meta_store as hms;
use common_meta_types::*;

use super::hive_database::HiveDatabase;
use super::hive_database::HIVE_DATABASE_ENGIE;
use super::hive_table::HIVE_TABLE_ENGIE;

///! Skeleton of mappers
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
    let meta = TableMeta {
        schema,
        engine: HIVE_TABLE_ENGIE.to_owned(),
        created_on: Utc::now(),
        ..Default::default()
    };

    let table_info = TableInfo {
        ident: TableIdent {
            table_id: 0,
            version: 0,
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
        let field = DataField::new(&name, try_from_filed_type_name(type_name)?);
        fields.push(field);
    }
    Ok(DataSchema::new(fields))
}

fn try_from_filed_type_name(type_name: impl AsRef<str>) -> Result<DataTypeImpl> {
    let name = type_name.as_ref();
    // TODO more mappings goes here
    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
    match name.to_uppercase().as_str() {
        "INT" => Ok(DataTypeImpl::Int32(Int32Type::default())),
        _ => Err(ErrorCode::IllegalDataType(format!(
            "unknown hive data type [{}]",
            name
        ))),
    }
}
