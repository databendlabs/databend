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

//! this module is used for converting iceberg data types, shemas and other metadata
//! to databend

use common_datavalues::chrono;
use common_datavalues::create_primitive_datatype;
use common_datavalues::BooleanType;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataTypeImpl;
use common_datavalues::DateType;
use common_datavalues::StringType;
use common_datavalues::StructType;
use common_datavalues::TimestampType;
use common_datavalues::VariantArrayType;
use common_datavalues::VariantObjectType;
use common_meta_app::schema::TableMeta;
use iceberg_rs::model::schema::AllType;
use iceberg_rs::model::schema::SchemaV2;
use iceberg_rs::model::schema::StructField;
use iceberg_rs::model::table::TableMetadataV2;
use itertools::Itertools;

/// generate TableMeta from Iceberg table meta
pub(super) fn meta_iceberg_to_databend(catalog: &str, meta: &TableMetadataV2) -> TableMeta {
    let schema = match meta.schemas.last() {
        Some(scm) => schema_iceberg_to_databend(scm),
        // empty schema
        None => DataSchema::new(vec![]),
    } // into Arc<DataSchema>
    .into();

    TableMeta {
        schema,
        catalog: catalog.to_string(),
        engine: "iceberg".to_string(),
        created_on: chrono::Utc::now(),
        ..Default::default()
    }
}

/// generate databend DataSchema from Iceberg
pub(super) fn schema_iceberg_to_databend(schema: &SchemaV2) -> DataSchema {
    let fields = schema
        .struct_fields
        .fields
        .iter()
        .sorted_by_key(|f| f.id)
        .map(struct_field_iceberg_to_databend)
        .collect();
    DataSchema::new(fields)
}

fn struct_field_iceberg_to_databend(sf: &StructField) -> DataField {
    let name = &sf.name;
    let ty = primitive_iceberg_to_databend(&sf.field_type);

    if sf.required {
        DataField::new(name, ty)
    } else {
        DataField::new_nullable(name, ty)
    }
}

fn primitive_iceberg_to_databend(prim: &AllType) -> DataTypeImpl {
    match prim {
        iceberg_rs::model::schema::AllType::Primitive(p) => match p {
            iceberg_rs::model::schema::PrimitiveType::Boolean => {
                DataTypeImpl::Boolean(BooleanType {})
            }
            iceberg_rs::model::schema::PrimitiveType::Int => create_primitive_datatype::<i32>(),
            iceberg_rs::model::schema::PrimitiveType::Long => create_primitive_datatype::<i64>(),
            iceberg_rs::model::schema::PrimitiveType::Float => create_primitive_datatype::<f32>(),
            iceberg_rs::model::schema::PrimitiveType::Double => create_primitive_datatype::<f64>(),
            iceberg_rs::model::schema::PrimitiveType::Decimal { .. } => {
                // not supported
                unimplemented!()
            }
            iceberg_rs::model::schema::PrimitiveType::Date => {
                // 4 bytes date type
                DataTypeImpl::Date(DateType {})
            }
            iceberg_rs::model::schema::PrimitiveType::Time => {
                // not supported, time without date
                unimplemented!()
            }
            iceberg_rs::model::schema::PrimitiveType::Timestamp => {
                // not supported, timestamp without timezone
                unimplemented!()
            }
            iceberg_rs::model::schema::PrimitiveType::Timestampz => TimestampType::new_impl(),
            iceberg_rs::model::schema::PrimitiveType::String => StringType::new_impl(),
            iceberg_rs::model::schema::PrimitiveType::Uuid => StringType::new_impl(),
            iceberg_rs::model::schema::PrimitiveType::Fixed(_) => StringType::new_impl(),
            iceberg_rs::model::schema::PrimitiveType::Binary => StringType::new_impl(),
        },
        iceberg_rs::model::schema::AllType::Struct(s) => {
            let (names, fields): (Vec<String>, Vec<DataTypeImpl>) = s
                .fields
                .iter()
                .sorted_by_key(|f| f.id)
                .map(|field| {
                    (
                        field.name.clone(),
                        primitive_iceberg_to_databend(&field.field_type),
                    )
                })
                .unzip();

            StructType::new_impl(Some(names), fields)
        }
        iceberg_rs::model::schema::AllType::List(_) => VariantArrayType::new_impl(),
        iceberg_rs::model::schema::AllType::Map(_) => VariantObjectType::new_impl(),
    }
}

// todo: add stateless tests here
