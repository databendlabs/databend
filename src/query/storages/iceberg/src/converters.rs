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

//! this module is used for converting iceberg data types, schemas and other metadata
//! to databend

use common_expression::types::decimal::DecimalSize;
use common_expression::types::DecimalDataType;
use common_expression::types::NumberDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use icelake::types;
use itertools::Itertools;

/// generate databend DataSchema from Iceberg
pub(super) fn schema_iceberg_to_databend(schema: &types::Schema) -> TableSchema {
    let fields = schema
        .fields
        .iter()
        .sorted_by_key(|f| f.id)
        .map(struct_field_iceberg_to_databend)
        .collect();
    TableSchema::new(fields)
}

fn struct_field_iceberg_to_databend(sf: &types::Field) -> TableField {
    let name = &sf.name;
    let ty = primitive_iceberg_to_databend(&sf.field_type);

    if sf.required {
        TableField::new(name, ty)
    } else {
        TableField::new(name, ty.wrap_nullable())
    }
}

// TODO: reject nested Struct
fn primitive_iceberg_to_databend(prim: &types::Any) -> TableDataType {
    match prim {
        types::Any::Primitive(p) => match p {
            types::Primitive::Boolean => TableDataType::Boolean,
            types::Primitive::Int => TableDataType::Number(NumberDataType::UInt32),
            types::Primitive::Long => TableDataType::Number(NumberDataType::Int64),
            types::Primitive::Float => TableDataType::Number(NumberDataType::Float32),
            types::Primitive::Double => TableDataType::Number(NumberDataType::Float64),
            types::Primitive::Decimal { precision, scale } => TableDataType::Decimal(
                DecimalDataType::from_size(DecimalSize {
                    precision: *precision,
                    scale: *scale,
                })
                .unwrap(),
            ),
            types::Primitive::Date => {
                // 4 bytes date type
                TableDataType::Date
            }
            types::Primitive::Time => {
                // not supported, time without date
                unimplemented!()
            }
            types::Primitive::Timestamp => TableDataType::Timestamp,
            types::Primitive::Timestampz => TableDataType::Timestamp,
            types::Primitive::String => TableDataType::String,
            types::Primitive::Uuid => TableDataType::String,
            types::Primitive::Fixed(_) => TableDataType::String,
            types::Primitive::Binary => TableDataType::String,
        },
        types::Any::Struct(s) => {
            let (names, fields): (Vec<String>, Vec<TableDataType>) = s
                .fields
                .iter()
                // reading as is?
                .sorted_by_key(|f| f.id)
                .map(|field| {
                    (
                        field.name.clone(),
                        primitive_iceberg_to_databend(&field.field_type),
                    )
                })
                .unzip();

            TableDataType::Tuple {
                fields_name: names,
                fields_type: fields,
            }
        }
        types::Any::List(types::List {
            element_required,
            element_type,
            ..
        }) => {
            let element_type = primitive_iceberg_to_databend(element_type);
            if *element_required {
                TableDataType::Array(Box::new(element_type))
            } else {
                TableDataType::Array(Box::new(TableDataType::Nullable(Box::new(element_type))))
            }
        }
        types::Any::Map(_) => {
            // wait for new expression support to complete
            unimplemented!()
        }
    }
}
