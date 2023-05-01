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

use chrono::Utc;
use common_expression::types::decimal::DecimalSize;
use common_expression::types::DecimalDataType;
use common_expression::types::NumberDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_meta_app::schema::TableMeta;
use common_meta_app::storage::StorageParams;
use iceberg_rs::model::schema::AllType;
use iceberg_rs::model::schema::List as IcebergList;
use iceberg_rs::model::schema::SchemaV2;
use iceberg_rs::model::schema::StructField;
use iceberg_rs::model::table::TableMetadata;
use itertools::Itertools;

/// generate TableMeta from Iceberg table meta
pub(crate) fn meta_iceberg_to_databend(
    catalog: &str,
    storage_params: &StorageParams,
    meta: &TableMetadata,
) -> TableMeta {
    let meta = meta.clone().to_latest();
    let schema = match meta.schemas.last() {
        Some(scm) => schema_iceberg_to_databend(scm),
        // empty schema
        None => TableSchema::empty(),
    }
    .into();

    TableMeta {
        schema,
        catalog: catalog.to_string(),
        engine: "iceberg".to_string(),
        created_on: Utc::now(),
        storage_params: Some(storage_params.clone()),
        ..Default::default()
    }
}

/// generate databend DataSchema from Iceberg
pub(super) fn schema_iceberg_to_databend(schema: &SchemaV2) -> TableSchema {
    let fields = schema
        .struct_fields
        .fields
        .iter()
        .sorted_by_key(|f| f.id)
        .map(struct_field_iceberg_to_databend)
        .collect();
    TableSchema::new(fields)
}

fn struct_field_iceberg_to_databend(sf: &StructField) -> TableField {
    let name = &sf.name;
    let ty = primitive_iceberg_to_databend(&sf.field_type);

    if sf.required {
        TableField::new(name, ty)
    } else {
        TableField::new(name, ty.wrap_nullable())
    }
}

// TODO: reject nested Struct
fn primitive_iceberg_to_databend(prim: &AllType) -> TableDataType {
    match prim {
        iceberg_rs::model::schema::AllType::Primitive(p) => match p {
            iceberg_rs::model::schema::PrimitiveType::Boolean => TableDataType::Boolean,
            iceberg_rs::model::schema::PrimitiveType::Int => {
                TableDataType::Number(NumberDataType::UInt32)
            }
            iceberg_rs::model::schema::PrimitiveType::Long => {
                TableDataType::Number(NumberDataType::Int64)
            }
            iceberg_rs::model::schema::PrimitiveType::Float => {
                TableDataType::Number(NumberDataType::Float32)
            }
            iceberg_rs::model::schema::PrimitiveType::Double => {
                TableDataType::Number(NumberDataType::Float64)
            }
            iceberg_rs::model::schema::PrimitiveType::Decimal { precision, scale } => {
                TableDataType::Decimal(
                    DecimalDataType::from_size(DecimalSize {
                        precision: *precision as u8,
                        scale: *scale,
                    })
                    .unwrap(),
                )
            }
            iceberg_rs::model::schema::PrimitiveType::Date => {
                // 4 bytes date type
                TableDataType::Date
            }
            iceberg_rs::model::schema::PrimitiveType::Time => {
                // not supported, time without date
                unimplemented!()
            }
            iceberg_rs::model::schema::PrimitiveType::Timestamp => TableDataType::Timestamp,
            iceberg_rs::model::schema::PrimitiveType::Timestampz => TableDataType::Timestamp,
            iceberg_rs::model::schema::PrimitiveType::String => TableDataType::String,
            iceberg_rs::model::schema::PrimitiveType::Uuid => TableDataType::String,
            iceberg_rs::model::schema::PrimitiveType::Fixed(_) => TableDataType::String,
            iceberg_rs::model::schema::PrimitiveType::Binary => TableDataType::String,
        },
        iceberg_rs::model::schema::AllType::Struct(s) => {
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
        iceberg_rs::model::schema::AllType::List(IcebergList {
            element_required,
            element,
            ..
        }) => {
            let element_type = primitive_iceberg_to_databend(element);
            if *element_required {
                TableDataType::Array(Box::new(element_type))
            } else {
                TableDataType::Array(Box::new(TableDataType::Nullable(Box::new(element_type))))
            }
        }
        iceberg_rs::model::schema::AllType::Map(_) => {
            // wait for new expression support to complete
            unimplemented!()
        }
    }
}

#[cfg(test)]
mod convert_test {
    use common_meta_app::storage::StorageFsConfig;
    use common_meta_app::storage::StorageParams;
    use iceberg_rs::model::table::TableMetadata;

    use super::meta_iceberg_to_databend;

    /// example metadata file
    const METADATA_FILE: &str = r#"
    {
        "format-version" : 2,
        "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
        "location": "s3://b/wh/data.db/table",
        "last-sequence-number" : 1,
        "last-updated-ms": 1515100955770,
        "last-column-id": 1,
        "schemas": [
            {
                "schema-id" : 1,
                "type" : "struct",
                "fields" :[
                    {
                        "id": 1,
                        "name": "struct_name",
                        "required": true,
                        "field_type": "fixed[1]"
                    }
                ]
            }
        ],
        "current-schema-id" : 1,
        "partition-specs": [
            {
                "spec-id": 1,
                "fields": [
                    {
                        "source-id": 4,
                        "field-id": 1000,
                        "name": "ts_day",
                        "transform": "day"
                    }
                ]
            }
        ],
        "default-spec-id": 1,
        "last-partition-id": 1,
        "properties": {
            "commit.retry.num-retries": "1"
        },
        "metadata-log": [
            {
                "metadata-file": "s3://bucket/.../v1.json",
                "timestamp-ms": 1515100
            }
        ],
        "sort-orders": [],
        "default-sort-order-id": 0
    }
"#;

    fn gen_iceberg_meta() -> TableMetadata {
        serde_json::de::from_str(METADATA_FILE).unwrap()
    }

    #[test]
    fn test_parse_metadata() {
        let metadata = gen_iceberg_meta();
        let mock_sp = StorageParams::Fs(StorageFsConfig {
            root: "/".to_string(),
        });

        let converted = meta_iceberg_to_databend("ctl", &mock_sp, &metadata);

        assert_eq!(converted.engine, "iceberg");
        assert_eq!(converted.catalog, "ctl");
    }
}
