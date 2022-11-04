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

use chrono::Utc;
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
use common_datavalues::TimestampType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::NumberType;
use common_expression::with_number_type;
use common_hive_meta_store as hms;
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::hive_catalog::HIVE_CATALOG;
use crate::hive_database::HiveDatabase;
use crate::hive_database::HIVE_DATABASE_ENGIE;
use crate::hive_table::HIVE_TABLE_ENGIE;
use crate::hive_table_options::HiveTableOptions;

pub(crate) fn str_field_to_scalar(str: &str, data_type: &DataType) -> Result<Scalar> {
    with_number_type!(|NUM_TYPE| match field.data_type() {
        DataType::Number(NumberDataType::NUM_TYPE) => Ok(Scalar::NUM_TYPE(value.parse().unwrap())),
        DataType::String(_) => Ok(Scalar::String(value.as_bytes().to_vec())),
        _ => Err(ErrorCode::UnImplement(format!(
            "generate column failed, {:?}",
            field
        ))),
    })
}
