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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_cast::cast;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use databend_common_exception::ErrorCode;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchemaRef;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[derive(Debug)]
enum SchemaComparison {
    Equivalent,
    NameChangesOnly,
    Different,
}

#[derive(Debug, Clone)]
enum ColumnSource {
    PassThrough {
        source_index: usize,
    },
    Promote {
        target_type: arrow_schema::DataType,
        source_index: usize,
    },
}

#[derive(Debug, Clone)]
enum BatchTransform {
    PassThrough,
    Modify {
        target_schema: Arc<Schema>,
        operations: Vec<ColumnSource>,
    },
    ModifySchema {
        target_schema: Arc<Schema>,
    },
}

#[derive(Clone)]
pub struct RecordBatchTransformer {
    target_schema: SchemaRef,
    table_field_mapping: BTreeMap<ColumnId, usize>,

    transforms: Option<BatchTransform>,
}

impl RecordBatchTransformer {
    pub fn build(table_schema: TableSchemaRef) -> Self {
        let target_schema: Schema = table_schema.as_ref().into();

        let mut table_field_mapping = BTreeMap::new();
        for (i, field) in table_schema.fields().iter().enumerate() {
            table_field_mapping.insert(field.column_id(), i);
        }

        RecordBatchTransformer {
            target_schema: Arc::new(target_schema),
            table_field_mapping,
            transforms: None,
        }
    }

    pub fn process_record_batch(
        &mut self,
        record_batch: RecordBatch,
    ) -> databend_common_exception::Result<RecordBatch> {
        let batch = match &self.transforms {
            Some(BatchTransform::PassThrough) => record_batch,
            Some(BatchTransform::Modify {
                target_schema,
                operations,
            }) => {
                let options = RecordBatchOptions::default()
                    .with_match_field_names(false)
                    .with_row_count(Some(record_batch.num_rows()));
                let columns = self.transform_columns(record_batch.columns(), operations)?;
                RecordBatch::try_new_with_options(target_schema.clone(), columns, &options)?
            }
            Some(BatchTransform::ModifySchema { target_schema }) => {
                RecordBatch::try_new(target_schema.clone(), record_batch.columns().to_vec())?
            }
            None => {
                self.transforms = Some(Self::generate_batch_transform(
                    record_batch.schema_ref(),
                    &self.target_schema,
                    &self.table_field_mapping,
                )?);

                self.process_record_batch(record_batch)?
            }
        };
        Ok(batch)
    }

    fn compare_schemas(source: &SchemaRef, target: &SchemaRef) -> SchemaComparison {
        if source.fields().len() != target.fields().len() {
            return SchemaComparison::Different;
        }
        let mut names_changed = false;

        for (source_field, target_field) in source.fields().iter().zip(target.fields().iter()) {
            if source_field.data_type() != target_field.data_type()
                || source_field.is_nullable() != target_field.is_nullable()
            {
                return SchemaComparison::Different;
            }

            if source_field.name() != target_field.name() {
                names_changed = true;
            }
        }
        if names_changed {
            SchemaComparison::NameChangesOnly
        } else {
            SchemaComparison::Equivalent
        }
    }

    fn transform_columns(
        &self,
        columns: &[Arc<dyn Array>],
        operations: &[ColumnSource],
    ) -> databend_common_exception::Result<Vec<Arc<dyn Array>>> {
        if columns.is_empty() {
            return Ok(columns.to_vec());
        }
        operations
            .iter()
            .map(|op| {
                Ok(match op {
                    ColumnSource::PassThrough { source_index } => columns[*source_index].clone(),

                    ColumnSource::Promote {
                        target_type,
                        source_index,
                    } => cast(&*columns[*source_index], target_type)?,
                })
            })
            .collect()
    }

    pub fn build_field_id_to_arrow_schema_map(
        source_schema: &SchemaRef,
    ) -> databend_common_exception::Result<HashMap<ColumnId, (FieldRef, usize)>> {
        let mut field_id_to_source_schema = HashMap::new();
        for (source_field_idx, source_field) in source_schema.fields.iter().enumerate() {
            let this_field_id = source_field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .ok_or_else(|| ErrorCode::InvalidDate("field id not present in parquet metadata"))?
                .parse()
                .map_err(|e| {
                    ErrorCode::InvalidDate(format!("field id not parseable as an column id: {}", e))
                })?;

            field_id_to_source_schema
                .insert(this_field_id, (source_field.clone(), source_field_idx));
        }
        Ok(field_id_to_source_schema)
    }

    fn generate_batch_transform(
        source: &SchemaRef,
        target: &SchemaRef,
        table_field_mapping: &BTreeMap<ColumnId, usize>,
    ) -> databend_common_exception::Result<BatchTransform> {
        match Self::compare_schemas(source, target) {
            SchemaComparison::Equivalent => Ok(BatchTransform::PassThrough),
            SchemaComparison::NameChangesOnly => Ok(BatchTransform::ModifySchema {
                target_schema: target.clone(),
            }),
            SchemaComparison::Different => Ok(BatchTransform::Modify {
                operations: Self::generate_transform_operations(
                    source,
                    target,
                    table_field_mapping,
                )?,
                target_schema: target.clone(),
            }),
        }
    }

    fn generate_transform_operations(
        source: &SchemaRef,
        target: &SchemaRef,
        table_field_mapping: &BTreeMap<ColumnId, usize>,
    ) -> databend_common_exception::Result<Vec<ColumnSource>> {
        let mut sources = Vec::with_capacity(table_field_mapping.len());

        let source_map = Self::build_field_id_to_arrow_schema_map(source)?;

        for (field_id, target_index) in table_field_mapping.iter() {
            let target_field = target.field(*target_index);
            let target_type = target_field.data_type();

            let Some((source_field, source_index)) = source_map.get(field_id) else {
                return Err(ErrorCode::TableSchemaMismatch(format!("The field with field_id: {field_id} does not exist in the source schema: {:#?}.", source)));
            };

            sources.push(
                if source_field
                    .data_type()
                    .equals_datatype(target_field.data_type())
                {
                    ColumnSource::PassThrough {
                        source_index: *source_index,
                    }
                } else {
                    ColumnSource::Promote {
                        target_type: target_type.clone(),
                        source_index: *source_index,
                    }
                },
            )
        }
        Ok(sources)
    }
}

#[cfg(test)]
mod test {
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
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::transformer::RecordBatchTransformer;

    #[test]
    fn build_field_id_to_source_schema_map_works() {
        let arrow_schema = arrow_schema_already_same_as_target();

        let result =
            RecordBatchTransformer::build_field_id_to_arrow_schema_map(&arrow_schema).unwrap();

        let expected = HashMap::from_iter([
            (11, (arrow_schema.fields()[0].clone(), 0)),
            (12, (arrow_schema.fields()[1].clone(), 1)),
            (14, (arrow_schema.fields()[2].clone(), 2)),
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
                TableField::new_from_column_id(
                    "b",
                    TableDataType::Number(NumberDataType::Int64),
                    11,
                ),
                TableField::new_from_column_id(
                    "c",
                    TableDataType::Number(NumberDataType::Float64),
                    12,
                ),
                TableField::new_from_column_id(
                    "d",
                    TableDataType::Number(NumberDataType::Int32),
                    13,
                ),
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
}
