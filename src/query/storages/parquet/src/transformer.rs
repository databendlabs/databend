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

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::StructArray;
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use databend_common_exception::ErrorCode;
use databend_common_expression::ColumnId;
use databend_common_expression::ParquetFieldId;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::is_internal_column_id;
use databend_common_expression::parquet_field_id_from_column_id;
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
        nested_indices: Vec<usize>,
    },
    Promote {
        target_type: arrow_schema::DataType,
        source_index: usize,
        nested_indices: Vec<usize>,
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
    table_field_mappings: Vec<TableFieldMapping>,

    transforms: Option<BatchTransform>,
}

#[derive(Debug, Clone)]
struct TableFieldMapping {
    column_id: ColumnId,
    target_index: usize,
    column_path: Vec<String>,
}

type ParquetFieldIdToArrowSchemaMap = HashMap<ParquetFieldId, (FieldRef, usize, Vec<usize>)>;

impl RecordBatchTransformer {
    pub fn build(table_schema: TableSchemaRef) -> Self {
        let target_schema: Schema = table_schema.as_ref().into();

        let mut table_field_mappings = Vec::new();
        for (i, field) in table_schema.fields().iter().enumerate() {
            if is_internal_column_id(field.column_id()) {
                continue;
            }
            let column_path = field
                .name()
                .split(':')
                .map(|segment| segment.to_string())
                .collect();
            table_field_mappings.push(TableFieldMapping {
                column_id: field.column_id(),
                target_index: i,
                column_path,
            });
        }

        RecordBatchTransformer {
            target_schema: Arc::new(target_schema),
            table_field_mappings,
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
                    &self.table_field_mappings,
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
                    ColumnSource::PassThrough {
                        source_index,
                        nested_indices,
                    } => Self::extract_nested_array(&columns[*source_index], nested_indices)?,

                    ColumnSource::Promote {
                        target_type,
                        source_index,
                        nested_indices,
                    } => {
                        let array =
                            Self::extract_nested_array(&columns[*source_index], nested_indices)?;
                        cast(&*array, target_type)?
                    }
                })
            })
            .collect()
    }

    fn extract_nested_array(
        column: &Arc<dyn Array>,
        nested_indices: &[usize],
    ) -> databend_common_exception::Result<Arc<dyn Array>> {
        if nested_indices.is_empty() {
            return Ok(column.clone());
        }

        let mut current = column.clone();
        for &child_idx in nested_indices {
            let struct_array = current
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Expected Struct array when traversing nested column, but found {}",
                        current.data_type()
                    ))
                })?;
            current = struct_array.column(child_idx).clone();
        }
        Ok(current)
    }

    pub fn build_field_id_to_arrow_schema_map(
        source_schema: &SchemaRef,
    ) -> databend_common_exception::Result<ParquetFieldIdToArrowSchemaMap> {
        let mut field_id_to_source_schema = HashMap::new();
        for (source_field_idx, source_field) in source_schema.fields.iter().enumerate() {
            let mut current_index_path = Vec::new();
            Self::insert_field_id_mapping(
                source_field,
                source_field_idx,
                &mut current_index_path,
                &mut field_id_to_source_schema,
            )?;
        }
        Ok(field_id_to_source_schema)
    }

    fn insert_field_id_mapping(
        field: &FieldRef,
        source_field_idx: usize,
        current_index_path: &mut Vec<usize>,
        field_id_to_source_schema: &mut HashMap<ParquetFieldId, (FieldRef, usize, Vec<usize>)>,
    ) -> databend_common_exception::Result<()> {
        let this_field_id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .ok_or_else(|| ErrorCode::InvalidDate("field id not present in parquet metadata"))?
            .parse::<u32>()
            .map(ParquetFieldId::new)
            .map_err(|e| {
                ErrorCode::InvalidDate(format!(
                    "field id not parseable as a parquet field id: {}",
                    e
                ))
            })?;

        field_id_to_source_schema.insert(
            this_field_id,
            (field.clone(), source_field_idx, current_index_path.clone()),
        );

        if let DataType::Struct(fields) = field.data_type() {
            for (child_idx, nested_field) in fields.iter().enumerate() {
                current_index_path.push(child_idx);
                Self::insert_field_id_mapping(
                    nested_field,
                    source_field_idx,
                    current_index_path,
                    field_id_to_source_schema,
                )?;
                current_index_path.pop();
            }
        }

        Ok(())
    }

    fn build_field_path_to_arrow_schema_map(
        source_schema: &SchemaRef,
    ) -> HashMap<Vec<String>, (FieldRef, usize, Vec<usize>)> {
        let mut field_path_map = HashMap::new();
        for (source_index, source_field) in source_schema.fields.iter().enumerate() {
            let mut current_path = vec![source_field.name().clone()];
            let mut current_index_path = Vec::new();
            Self::insert_field_path_mapping(
                source_field,
                source_index,
                &mut current_path,
                &mut current_index_path,
                &mut field_path_map,
            );
        }
        field_path_map
    }

    fn insert_field_path_mapping(
        field: &FieldRef,
        source_field_idx: usize,
        current_path: &mut Vec<String>,
        current_index_path: &mut Vec<usize>,
        field_path_map: &mut HashMap<Vec<String>, (FieldRef, usize, Vec<usize>)>,
    ) {
        field_path_map.insert(
            current_path.clone(),
            (field.clone(), source_field_idx, current_index_path.clone()),
        );

        if let DataType::Struct(fields) = field.data_type() {
            for (child_idx, nested_field) in fields.iter().enumerate() {
                current_path.push(nested_field.name().clone());
                current_index_path.push(child_idx);
                Self::insert_field_path_mapping(
                    nested_field,
                    source_field_idx,
                    current_path,
                    current_index_path,
                    field_path_map,
                );
                current_path.pop();
                current_index_path.pop();
            }
        }
    }

    fn generate_batch_transform(
        source: &SchemaRef,
        target: &SchemaRef,
        table_field_mappings: &[TableFieldMapping],
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
                    table_field_mappings,
                )?,
                target_schema: target.clone(),
            }),
        }
    }

    fn generate_transform_operations(
        source: &SchemaRef,
        target: &SchemaRef,
        table_field_mappings: &[TableFieldMapping],
    ) -> databend_common_exception::Result<Vec<ColumnSource>> {
        let mut sources = Vec::with_capacity(table_field_mappings.len());

        let source_map = Self::build_field_id_to_arrow_schema_map(source)?;
        let source_path_map = Self::build_field_path_to_arrow_schema_map(source);

        for mapping in table_field_mappings.iter() {
            let target_field = target.field(mapping.target_index);
            let target_type = target_field.data_type();

            let source_entry = source_path_map
                .get(&mapping.column_path)
                .cloned()
                .or_else(|| {
                    source_map
                        .get(&parquet_field_id_from_column_id(mapping.column_id))
                        .cloned()
                });

            let Some((source_field, source_index, nested_indices)) = source_entry else {
                let path = if mapping.column_path.is_empty() {
                    "unknown".to_string()
                } else {
                    mapping.column_path.join(":")
                };
                return Err(ErrorCode::TableSchemaMismatch(format!(
                    "The field with column_id: {} (parquet_field_id: {}) (path: {path}) does not exist in the source schema: {:#?}.",
                    mapping.column_id,
                    parquet_field_id_from_column_id(mapping.column_id).as_u32(),
                    source
                )));
            };

            sources.push(
                if source_field
                    .data_type()
                    .equals_datatype(target_field.data_type())
                {
                    ColumnSource::PassThrough {
                        source_index,
                        nested_indices,
                    }
                } else {
                    ColumnSource::Promote {
                        target_type: target_type.clone(),
                        source_index,
                        nested_indices,
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
    use arrow_schema::FieldRef;
    use arrow_schema::Fields;
    use arrow_schema::Schema;
    use databend_common_expression::ParquetFieldId;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::transformer::ColumnSource;
    use crate::transformer::RecordBatchTransformer;
    use crate::transformer::TableFieldMapping;

    #[test]
    fn build_field_id_to_source_schema_map_works() {
        let arrow_schema = arrow_schema_already_same_as_target();

        let result =
            RecordBatchTransformer::build_field_id_to_arrow_schema_map(&arrow_schema).unwrap();

        let expected = HashMap::from_iter([
            (
                ParquetFieldId::new(11),
                (arrow_schema.fields()[0].clone(), 0, vec![]),
            ),
            (
                ParquetFieldId::new(12),
                (arrow_schema.fields()[1].clone(), 1, vec![]),
            ),
            (
                ParquetFieldId::new(14),
                (arrow_schema.fields()[2].clone(), 2, vec![]),
            ),
        ]);

        assert!(result.eq(&expected));
    }

    #[test]
    fn build_field_id_to_source_schema_map_handles_struct_fields() {
        let arrow_schema = arrow_schema_with_struct();

        let result =
            RecordBatchTransformer::build_field_id_to_arrow_schema_map(&arrow_schema).unwrap();

        let struct_field = arrow_schema.fields()[1].clone();
        let DataType::Struct(struct_children) = struct_field.data_type() else {
            unreachable!();
        };
        let struct_child_fields = struct_children.iter().cloned().collect::<Vec<FieldRef>>();
        let nested_struct_field = struct_child_fields[1].clone();
        let DataType::Struct(nested_children) = nested_struct_field.data_type() else {
            unreachable!();
        };
        let nested_child_fields = nested_children.iter().cloned().collect::<Vec<FieldRef>>();

        let expected = HashMap::from_iter([
            (
                ParquetFieldId::new(30),
                (arrow_schema.fields()[0].clone(), 0, vec![]),
            ),
            (ParquetFieldId::new(40), (struct_field.clone(), 1, vec![])),
            (
                ParquetFieldId::new(41),
                (struct_child_fields[0].clone(), 1, vec![0]),
            ),
            (
                ParquetFieldId::new(42),
                (nested_struct_field.clone(), 1, vec![1]),
            ),
            (
                ParquetFieldId::new(43),
                (nested_child_fields[0].clone(), 1, vec![1, 0]),
            ),
        ]);

        assert!(result.eq(&expected));
    }

    #[test]
    fn generate_transform_operations_prefers_path_over_field_ids() {
        let source_schema = Arc::new(Schema::new(vec![struct_field(
            "item",
            vec![simple_field("age", DataType::Int32, true, "5")],
            true,
            "3",
        )]));
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "item:age",
            DataType::Int32,
            true,
        )]));

        let table_field_mapping = vec![TableFieldMapping {
            column_id: 3,
            target_index: 0,
            column_path: vec!["item".to_string(), "age".to_string()],
        }];

        let operations = RecordBatchTransformer::generate_transform_operations(
            &source_schema,
            &target_schema,
            &table_field_mapping,
        )
        .unwrap();

        assert_eq!(operations.len(), 1);
        assert!(matches!(
            operations[0],
            ColumnSource::PassThrough {
                source_index: 0,
                ref nested_indices,
            } if nested_indices.as_slice() == [0]
        ));
    }

    #[test]
    fn generate_transform_operations_tracks_nested_indices() {
        let source_schema = arrow_schema_with_struct();
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "struct_col:nested_struct:deep_leaf",
            DataType::Utf8View,
            false,
        )]));

        let table_field_mapping = vec![TableFieldMapping {
            column_id: 43,
            target_index: 0,
            column_path: vec![
                "struct_col".to_string(),
                "nested_struct".to_string(),
                "deep_leaf".to_string(),
            ],
        }];

        let operations = RecordBatchTransformer::generate_transform_operations(
            &source_schema,
            &target_schema,
            &table_field_mapping,
        )
        .unwrap();

        assert_eq!(operations.len(), 1);
        assert!(matches!(
            operations[0],
            ColumnSource::PassThrough {
                source_index: 1,
                ref nested_indices,
            } if nested_indices.as_slice() == [1, 0]
        ));
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

    fn arrow_schema_with_struct() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            simple_field("plain", DataType::Int32, false, "30"),
            struct_field(
                "struct_col",
                vec![
                    simple_field("leaf", DataType::Float64, true, "41"),
                    struct_field(
                        "nested_struct",
                        vec![simple_field("deep_leaf", DataType::Utf8View, false, "43")],
                        true,
                        "42",
                    ),
                ],
                false,
                "40",
            ),
        ]))
    }

    fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
        Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value.to_string(),
        )]))
    }

    fn struct_field(name: &str, fields: Vec<Field>, nullable: bool, value: &str) -> Field {
        Field::new(name, DataType::Struct(Fields::from(fields)), nullable).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), value.to_string())]),
        )
    }
}
