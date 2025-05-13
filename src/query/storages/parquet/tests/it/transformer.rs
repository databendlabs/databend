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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Float32Array;
use arrow_array::Float64Array;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_array::StringViewArray;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::Schema;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_storages_parquet::RecordBatchTransformer;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[test]
fn build_field_id_to_source_schema_map_works() {
    let arrow_schema = arrow_schema_already_same_as_target();

    let result = RecordBatchTransformer::build_field_id_to_arrow_schema_map(&arrow_schema).unwrap();

    let expected = HashMap::from_iter([
        (10, (arrow_schema.fields()[0].clone(), 0)),
        (11, (arrow_schema.fields()[1].clone(), 1)),
        (12, (arrow_schema.fields()[2].clone(), 2)),
        (14, (arrow_schema.fields()[3].clone(), 3)),
    ]);

    assert!(result.eq(&expected));
}

#[test]
fn processor_returns_properly_shaped_record_batch_when_no_schema_migration_required() {
    let table_schema = Arc::new(table_schema().project(&[3, 4]));

    let mut inst = RecordBatchTransformer::build(table_schema);

    let result = inst
        .process_record_batch(source_record_batch_no_migration_required())
        .unwrap();

    let expected = clean_fields_meta(source_record_batch_no_migration_required());

    assert_eq!(result, expected);
}

#[test]
fn processor_returns_properly_shaped_record_batch_when_schema_migration_required() {
    let table_schema = Arc::new(table_schema().project(&[1, 2, 4])); // b, c, e

    let mut inst = RecordBatchTransformer::build(table_schema);

    let result = inst.process_record_batch(source_record_batch()).unwrap();

    let expected = clean_fields_meta(expected_record_batch_migration_required());

    assert_eq!(result, expected);
}

fn clean_fields_meta(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();

    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Field::clone(field)
                .with_nullable(false)
                .with_metadata(HashMap::new())
        })
        .collect::<Vec<_>>();
    let schema = Schema::new(Fields::from(fields));
    RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).unwrap()
}

pub fn source_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema_promotion_addition_and_renaming_required(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1001), Some(1002), Some(1003)])), // b
            Arc::new(Float32Array::from(vec![
                Some(12.125),
                Some(23.375),
                Some(34.875),
            ])), // c
            Arc::new(Int32Array::from(vec![Some(2001), Some(2002), Some(2003)])), // d
            Arc::new(StringViewArray::from(vec![
                Some("Apache"),
                Some("Iceberg"),
                Some("Rocks"),
            ])), // e
        ],
    )
    .unwrap()
}

pub fn source_record_batch_no_migration_required() -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema_no_promotion_addition_or_renaming_required(),
        vec![
            Arc::new(Int32Array::from(vec![Some(2001), Some(2002), Some(2003)])), // d
            Arc::new(StringViewArray::from(vec![
                Some("Apache"),
                Some("Iceberg"),
                Some("Rocks"),
            ])), // e
        ],
    )
    .unwrap()
}

pub fn expected_record_batch_migration_required() -> RecordBatch {
    RecordBatch::try_new(arrow_schema_already_same_as_target(), vec![
        Arc::new(Int64Array::from(vec![Some(1001), Some(1002), Some(1003)])), // b
        Arc::new(Float64Array::from(vec![
            Some(12.125),
            Some(23.375),
            Some(34.875),
        ])), // c
        Arc::new(StringViewArray::from(vec![
            Some("Apache"),
            Some("Iceberg"),
            Some("Rocks"),
        ])), /* e (d skipped by projection) */
    ])
    .unwrap()
}

pub fn table_schema() -> TableSchema {
    TableSchema {
        fields: vec![
            TableField::new_from_column_id("a", TableDataType::String, 10),
            TableField::new_from_column_id("b", TableDataType::Number(NumberDataType::Int64), 11),
            TableField::new_from_column_id("c", TableDataType::Number(NumberDataType::Float64), 12),
            TableField::new_from_column_id("d", TableDataType::Number(NumberDataType::Int32), 13),
            TableField::new_from_column_id("e", TableDataType::String, 14),
        ],
        metadata: Default::default(),
        next_column_id: 0,
    }
}

fn arrow_schema_already_same_as_target() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        simple_field("b", DataType::Int64, false, "11"),
        simple_field("c", DataType::Float64, false, "12"),
        simple_field("e", DataType::Utf8View, true, "14"),
    ]))
}

fn arrow_schema_promotion_addition_and_renaming_required() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        simple_field("b", DataType::Int32, false, "11"),
        simple_field("c", DataType::Float32, false, "12"),
        simple_field("d", DataType::Int32, false, "13"),
        simple_field("e_old", DataType::Utf8View, true, "14"),
    ]))
}

fn arrow_schema_no_promotion_addition_or_renaming_required() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        simple_field("d", DataType::Int32, false, "13"),
        simple_field("e", DataType::Utf8View, true, "14"),
    ]))
}

fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
    Field::new(name, ty, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        value.to_string(),
    )]))
}
