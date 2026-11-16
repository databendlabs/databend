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
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::LargeListArray;
use arrow_array::MapArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::StructArray;
use arrow_array::new_null_array;
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_schema::FieldRef;
use arrow_schema::Fields;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::parquet_reader::utils::parquet_field_name_matches_path_name;
use crate::read_schema::parquet_field_id;

/// The caller selects identity semantics explicitly. Missing IDs are valid in
/// path matching; ID matching never falls back to names or table/file positions.
#[derive(Clone, Copy, Debug)]
pub enum FieldMatching {
    Path,
    FieldId,
}

#[derive(Debug, Clone)]
struct ColumnSource {
    source_index: usize,
    nested_indices: Vec<usize>,
    target_type: Option<DataType>,
}

type FieldIdMap = HashMap<i32, (FieldRef, usize, Vec<usize>)>;

/// A conversion compiled once for an exact projected batch schema. This holds
/// only file-format IDs and Arrow types, never Databend's internal ColumnIds.
#[derive(Clone, Debug)]
pub struct RecordBatchTransformer {
    target_schema: SchemaRef,
    operations: Vec<ColumnSource>,
    matching: FieldMatching,
}

impl RecordBatchTransformer {
    pub fn try_create(
        source: SchemaRef,
        target: SchemaRef,
        matching: FieldMatching,
    ) -> Result<Self> {
        let id_map = match matching {
            FieldMatching::FieldId => Self::build_field_id_map(&source)?,
            FieldMatching::Path => HashMap::new(),
        };
        let mut operations = Vec::with_capacity(target.fields().len());
        for field in target.fields() {
            let entry = match matching {
                FieldMatching::FieldId => {
                    parquet_field_id(field)?.and_then(|id| id_map.get(&id).cloned())
                }
                FieldMatching::Path => Self::find_field_by_path(&source, field.name()),
            };
            let Some((source_field, source_index, nested_indices)) = entry else {
                return Err(ErrorCode::TableSchemaMismatch(format!(
                    "Field {} cannot be resolved with {matching:?} matching in the source batch schema",
                    field.name()
                )));
            };
            let target_type =
                (source_field.data_type() != field.data_type()).then(|| field.data_type().clone());
            operations.push(ColumnSource {
                source_index,
                nested_indices,
                target_type,
            });
        }
        Ok(Self {
            target_schema: target,
            operations,
            matching,
        })
    }

    pub fn transform_record_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .operations
            .iter()
            .map(|operation| {
                let array = Self::extract_nested_array(
                    batch.column(operation.source_index),
                    &operation.nested_indices,
                )?;
                match &operation.target_type {
                    Some(target) => align_array(array, target, self.matching),
                    None => Ok(array),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        // Field identities and child order were resolved by the plan. File-only
        // nested metadata (e.g. optional IDs in path mode) need not equal output metadata.
        let options = RecordBatchOptions::default()
            .with_match_field_names(false)
            .with_row_count(Some(batch.num_rows()));
        Ok(RecordBatch::try_new_with_options(
            self.target_schema.clone(),
            columns,
            &options,
        )?)
    }

    fn find_field_by_path(source: &Schema, path: &str) -> Option<(FieldRef, usize, Vec<usize>)> {
        // A quoted/flat Parquet column may itself contain ':'. Prefer the full
        // name before treating the separator as a nested path.
        if let Some(index) = matching_field_index(source.fields(), path) {
            return Some((source.fields()[index].clone(), index, vec![]));
        }
        let mut fields = source.fields();
        let mut indices = Vec::new();
        let mut segments = path.split(':').peekable();
        while let Some(segment) = segments.next() {
            let index = matching_field_index(fields, segment)?;
            indices.push(index);
            let field = &fields[index];
            if segments.peek().is_none() {
                return Some((field.clone(), indices[0], indices[1..].to_vec()));
            }
            fields = match field.data_type() {
                DataType::Struct(fields) => fields,
                _ => return None,
            };
        }
        None
    }

    fn build_field_id_map(source: &Schema) -> Result<FieldIdMap> {
        let mut map = HashMap::new();
        for (index, field) in source.fields().iter().enumerate() {
            Self::insert_field_id(field, index, &mut vec![], &mut map)?;
        }
        Ok(map)
    }

    fn insert_field_id(
        field: &FieldRef,
        index: usize,
        path: &mut Vec<usize>,
        map: &mut FieldIdMap,
    ) -> Result<()> {
        // IDs are optional in Parquet. An unmapped parent must not hide children
        // that have IDs, and irrelevant ID-less columns must not block the read.
        if let Some(id) = parquet_field_id(field)? {
            if map
                .insert(id, (field.clone(), index, path.clone()))
                .is_some()
            {
                return Err(ErrorCode::TableSchemaMismatch(format!(
                    "Duplicate Parquet field ID {id}"
                )));
            }
        }
        if let DataType::Struct(fields) = field.data_type() {
            for (child, field) in fields.iter().enumerate() {
                path.push(child);
                Self::insert_field_id(field, index, path, map)?;
                path.pop();
            }
        }
        Ok(())
    }

    fn extract_nested_array(column: &ArrayRef, indices: &[usize]) -> Result<ArrayRef> {
        let mut array = column.clone();
        for &index in indices {
            let parent = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    ErrorCode::TableSchemaMismatch(format!(
                        "Expected struct while traversing nested column, found {}",
                        array.data_type()
                    ))
                })?;
            let child = parent.column(index);
            let nulls = arrow_buffer::NullBuffer::union(parent.nulls(), child.nulls());
            array = arrow_array::make_array(child.to_data().into_builder().nulls(nulls).build()?);
        }
        Ok(array)
    }
}

fn matching_field_index(fields: &Fields, name: &str) -> Option<usize> {
    // Preserve exact names for case-sensitive files before using the generic
    // case-insensitive/quoted-path fallback.
    fields
        .iter()
        .position(|field| field.name() == name)
        .or_else(|| {
            let quoted = QuotedIdent::<String>::from_str(name).ok()?;
            fields.iter().position(|field| field.name() == &quoted.0)
        })
        .or_else(|| {
            fields
                .iter()
                .position(|field| parquet_field_name_matches_path_name(field.name(), name))
        })
}

/// Align named nested types without Arrow's positional fallback for an absent
/// struct child. The engine has already resolved names when matching by ID.
fn align_array(array: ArrayRef, target: &DataType, matching: FieldMatching) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(array);
    }
    match target {
        DataType::Struct(fields) => {
            let source = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ErrorCode::TableSchemaMismatch("Expected struct array"))?;
            let columns = fields
                .iter()
                .map(|field| {
                    let column = match matching {
                        FieldMatching::FieldId => {
                            let id = parquet_field_id(field)?.ok_or_else(|| {
                                ErrorCode::TableSchemaMismatch(format!(
                                    "Missing file field ID for child {}",
                                    field.name()
                                ))
                            })?;
                            let mut column = None;
                            for (index, child) in source.fields().iter().enumerate() {
                                if parquet_field_id(child)? == Some(id) {
                                    if column.is_some() {
                                        return Err(ErrorCode::TableSchemaMismatch(format!(
                                            "Duplicate Parquet child field ID {id}"
                                        )));
                                    }
                                    column = Some(source.column(index));
                                }
                            }
                            column
                        }
                        FieldMatching::Path => matching_field_index(source.fields(), field.name())
                            .map(|index| source.column(index)),
                    };
                    if let Some(column) = column {
                        align_array(column.clone(), field.data_type(), matching)
                    } else if field.is_nullable() {
                        Ok(new_null_array(field.data_type(), array.len()))
                    } else {
                        Err(ErrorCode::TableSchemaMismatch(format!(
                            "Missing required field {}",
                            field.name()
                        )))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                source.nulls().cloned(),
            )?))
        }
        DataType::LargeList(element) => {
            let source_type = match array.data_type() {
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => DataType::LargeList(field.clone()),
                _ => return Err(ErrorCode::TableSchemaMismatch("Expected list array")),
            };
            let array = cast(array.as_ref(), &source_type)?;
            let source = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = align_array(source.values().clone(), element.data_type(), matching)?;
            Ok(Arc::new(LargeListArray::try_new(
                element.clone(),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        DataType::Map(entries, ordered) => {
            let source = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| ErrorCode::TableSchemaMismatch("Expected map array"))?;
            let values = match matching {
                FieldMatching::Path => {
                    // MAP entries have structural roles (key, value), not table
                    // column names. Metastore labels may differ from file labels.
                    // Positional matching applies only to these two entry fields;
                    // named structs nested inside their values still match by path.
                    let DataType::Struct(fields) = entries.data_type() else {
                        return Err(ErrorCode::TableSchemaMismatch("Expected MAP entry struct"));
                    };
                    if fields.len() != 2 || source.entries().num_columns() != 2 {
                        return Err(ErrorCode::TableSchemaMismatch(
                            "MAP entries require key and value",
                        ));
                    }
                    let columns = fields
                        .iter()
                        .enumerate()
                        .map(|(index, field)| {
                            align_array(
                                source.entries().column(index).clone(),
                                field.data_type(),
                                matching,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    StructArray::try_new(
                        fields.clone(),
                        columns,
                        source.entries().nulls().cloned(),
                    )?
                }
                FieldMatching::FieldId => {
                    let values = align_array(
                        Arc::new(source.entries().clone()),
                        entries.data_type(),
                        matching,
                    )?;
                    values
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap()
                        .clone()
                }
            };
            Ok(Arc::new(MapArray::try_new(
                entries.clone(),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
                *ordered,
            )?))
        }
        _ => Ok(cast(array.as_ref(), target)?),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use arrow_array::Int64Array;
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;

    fn field(name: &str, ty: DataType, id: i32) -> Field {
        Field::new(name, ty, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }
    fn transformer(
        batch: &RecordBatch,
        fields: Vec<Field>,
        matching: FieldMatching,
    ) -> RecordBatchTransformer {
        RecordBatchTransformer::try_create(batch.schema(), Arc::new(Schema::new(fields)), matching)
            .unwrap()
    }
    fn int64(batch: &RecordBatch, column: usize, row: usize) -> i64 {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
    }

    #[test]
    fn idless_parquet_promotes_by_path_without_table_format_metadata() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let plan = transformer(
            &batch,
            vec![Field::new("k", DataType::Int64, false)],
            FieldMatching::Path,
        );
        for _ in 0..2 {
            let output = plan.transform_record_batch(&batch).unwrap();
            assert_eq!(int64(&output, 0, 1), 2);
        }
    }
    #[test]
    fn same_types_reordered_by_path_are_not_treated_as_name_changes_only() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("b", DataType::Int64, false),
                Field::new("a", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![20])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        let output = transformer(
            &batch,
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ],
            FieldMatching::Path,
        )
        .transform_record_batch(&batch)
        .unwrap();
        assert_eq!(int64(&output, 0, 0), 10);
        assert_eq!(int64(&output, 1, 0), 20);
    }
    #[test]
    fn ids_override_names_and_positions_even_when_types_match() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                field("a", DataType::Int64, 2),
                field("b", DataType::Int64, 1),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![20])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        let output = transformer(
            &batch,
            vec![
                field("a", DataType::Int64, 1),
                field("b", DataType::Int64, 2),
            ],
            FieldMatching::FieldId,
        )
        .transform_record_batch(&batch)
        .unwrap();
        assert_eq!(int64(&output, 0, 0), 10);
        assert_eq!(int64(&output, 1, 0), 20);
    }
    #[test]
    fn exact_path_names_take_precedence_over_case_insensitive_fallback() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("A", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![20])),
            ],
        )
        .unwrap();
        let output = transformer(
            &batch,
            vec![
                Field::new("\"A\"", DataType::Int64, false),
                Field::new("a", DataType::Int64, false),
            ],
            FieldMatching::Path,
        )
        .transform_record_batch(&batch)
        .unwrap();
        assert_eq!(int64(&output, 0, 0), 20);
        assert_eq!(int64(&output, 1, 0), 10);
    }

    #[test]
    fn id_matching_never_guesses_current_names() {
        let source = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let target = Arc::new(Schema::new(vec![field("k", DataType::Int64, 1)]));
        assert!(
            RecordBatchTransformer::try_create(
                source.clone(),
                target.clone(),
                FieldMatching::FieldId
            )
            .is_err()
        );
        assert!(RecordBatchTransformer::try_create(source, target, FieldMatching::Path).is_ok());
    }
    #[test]
    fn missing_parent_id_does_not_hide_nested_ids_or_parent_nulls() {
        let fields = vec![field("value", DataType::Int64, 30)].into();
        let array = StructArray::new(
            fields,
            vec![Arc::new(Int64Array::from(vec![5, 99]))],
            Some(NullBuffer::from(vec![true, false])),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "item",
                array.data_type().clone(),
                true,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();
        let output = transformer(
            &batch,
            vec![field("renamed:value", DataType::Int64, 30)],
            FieldMatching::FieldId,
        )
        .transform_record_batch(&batch)
        .unwrap();
        assert_eq!(int64(&output, 0, 0), 5);
        assert!(output.column(0).is_null(1));
        let output = transformer(
            &batch,
            vec![Field::new("item:value", DataType::Int64, true)],
            FieldMatching::Path,
        )
        .transform_record_batch(&batch)
        .unwrap();
        assert!(output.column(0).is_null(1));
    }
    #[test]
    fn malformed_ids_are_irrelevant_to_path_matching_and_duplicates_fail_id_matching() {
        let bad = Field::new("k", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "bad".to_string(),
        )]));
        let source = Arc::new(Schema::new(vec![bad]));
        let target = Arc::new(Schema::new(vec![field("k", DataType::Int64, 1)]));
        assert!(
            RecordBatchTransformer::try_create(source.clone(), target.clone(), FieldMatching::Path)
                .is_ok()
        );
        assert!(
            RecordBatchTransformer::try_create(source, target.clone(), FieldMatching::FieldId)
                .is_err()
        );
        let duplicate = Arc::new(Schema::new(vec![
            field("k", DataType::Int64, 1),
            field("other", DataType::Int64, 1),
        ]));
        assert!(
            RecordBatchTransformer::try_create(duplicate, target, FieldMatching::FieldId).is_err()
        );
    }
    #[test]
    fn whole_struct_alignment_keeps_names_and_never_falls_back_to_child_position() {
        let source = StructArray::new(
            vec![
                Field::new("b", DataType::Int64, true),
                Field::new("a", DataType::Int64, true),
            ]
            .into(),
            vec![
                Arc::new(Int64Array::from(vec![20])),
                Arc::new(Int64Array::from(vec![10])),
            ],
            None,
        );
        let target = DataType::Struct(
            vec![
                Field::new("a", DataType::Int64, true),
                Field::new("new_child", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ]
            .into(),
        );
        let array = align_array(Arc::new(source.clone()), &target, FieldMatching::Path).unwrap();
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(array.column(1).null_count(), 1);
        assert_eq!(
            array
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
        assert!(
            align_array(
                Arc::new(source),
                &DataType::Struct(vec![Field::new("missing", DataType::Int64, false)].into()),
                FieldMatching::Path
            )
            .is_err()
        );
    }
    #[test]
    fn whole_struct_uses_child_ids_not_same_names_or_positions() {
        let array = StructArray::new(
            vec![
                field("a", DataType::Int64, 2),
                field("b", DataType::Int64, 1),
            ]
            .into(),
            vec![
                Arc::new(Int64Array::from(vec![20])),
                Arc::new(Int64Array::from(vec![10])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field(
                "item",
                array.data_type().clone(),
                3,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();
        let target = field(
            "item",
            DataType::Struct(
                vec![
                    field("a", DataType::Int64, 1),
                    field("b", DataType::Int64, 2),
                ]
                .into(),
            ),
            3,
        );
        let output = transformer(&batch, vec![target], FieldMatching::FieldId)
            .transform_record_batch(&batch)
            .unwrap();
        let array = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            array
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
    }

    #[test]
    fn struct_path_matching_preserves_generic_case_insensitive_behavior() {
        let array = StructArray::new(
            vec![Field::new("VALUE", DataType::Int32, false)].into(),
            vec![Arc::new(Int32Array::from(vec![5]))],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ITEM",
                array.data_type().clone(),
                false,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();
        let output = transformer(
            &batch,
            vec![Field::new(
                "item",
                DataType::Struct(vec![Field::new("value", DataType::Int64, false)].into()),
                false,
            )],
            FieldMatching::Path,
        )
        .transform_record_batch(&batch)
        .unwrap();
        let array = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5
        );
    }

    #[test]
    fn map_entry_roles_are_positional_but_value_structs_keep_named_or_id_matching() {
        use arrow_buffer::OffsetBuffer;
        let values = StructArray::new(
            vec![
                field("a", DataType::Int32, 30),
                field("b", DataType::Int32, 40),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![20])),
                Arc::new(Int32Array::from(vec![30])),
            ],
            None,
        );
        let entries = StructArray::new(
            vec![
                field("file_key", DataType::Int32, 11).with_nullable(false),
                field("file_value", values.data_type().clone(), 12),
            ]
            .into(),
            vec![Arc::new(Int32Array::from(vec![10])), Arc::new(values)],
            None,
        );
        let source = MapArray::try_new(
            Arc::new(Field::new(
                "file_entries",
                entries.data_type().clone(),
                false,
            )),
            OffsetBuffer::new(vec![0_i32, 1, 1].into()),
            entries,
            Some(NullBuffer::from(vec![true, false])),
            false,
        )
        .unwrap();
        let target = DataType::Map(
            Arc::new(Field::new(
                "metastore_entries",
                DataType::Struct(
                    vec![
                        field("metastore_key", DataType::Int64, 11).with_nullable(false),
                        field(
                            "metastore_value",
                            DataType::Struct(
                                vec![
                                    field("b", DataType::Int64, 40),
                                    field("a", DataType::Int64, 30),
                                ]
                                .into(),
                            ),
                            12,
                        ),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        for matching in [FieldMatching::Path, FieldMatching::FieldId] {
            let array = align_array(Arc::new(source.clone()), &target, matching).unwrap();
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            assert_eq!(map.offsets().as_ref(), &[0, 1, 1]);
            assert!(map.is_null(1));
            assert_eq!(
                map.entries()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                10
            );
            let values = map
                .entries()
                .column(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(
                values
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                30
            );
            assert_eq!(
                values
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                20
            );
        }
    }

    #[test]
    fn map_path_matching_does_not_swap_keys_and_values_when_labels_cross() {
        use arrow_buffer::OffsetBuffer;
        let entries = StructArray::new(
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int32, true),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(Int32Array::from(vec![20])),
            ],
            None,
        );
        let source = MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0_i32, 1].into()),
            entries,
            None,
            false,
        )
        .unwrap();
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("value", DataType::Int64, false),
                        Field::new("key", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let array = align_array(Arc::new(source), &target, FieldMatching::Path).unwrap();
        let map = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            map.entries()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            map.entries()
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
    }

    #[test]
    fn fixed_size_lists_preserve_sliced_offsets_nulls_and_child_promotions() {
        use arrow_array::FixedSizeListArray;

        let source = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            2,
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                None,
            ])),
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .unwrap();
        let target = DataType::LargeList(Arc::new(Field::new("element", DataType::Int64, true)));
        for matching in [FieldMatching::Path, FieldMatching::FieldId] {
            let array = align_array(Arc::new(source.slice(1, 2)), &target, matching).unwrap();
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            assert_eq!(list.len(), 2);
            assert!(list.is_null(0));
            assert!(!list.is_null(1));
            let second = list.value(1);
            let values = second.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.len(), 2);
            assert_eq!(values.value(0), 5);
            assert!(values.is_null(1));
        }
    }

    #[test]
    fn cached_path_plan_does_not_require_identical_nested_id_metadata() {
        let first = StructArray::new(
            vec![field("value", DataType::Int64, 1)].into(),
            vec![Arc::new(Int64Array::from(vec![10]))],
            None,
        );
        let source = Arc::new(Schema::new(vec![Field::new(
            "item",
            first.data_type().clone(),
            false,
        )]));
        let plan = RecordBatchTransformer::try_create(source.clone(), source, FieldMatching::Path)
            .unwrap();
        let second = StructArray::new(
            vec![field("value", DataType::Int64, 2)].into(),
            vec![Arc::new(Int64Array::from(vec![20]))],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "item",
                second.data_type().clone(),
                false,
            )])),
            vec![Arc::new(second)],
        )
        .unwrap();
        let output = plan.transform_record_batch(&batch).unwrap();
        let item = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            item.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
    }

    #[test]
    fn collection_alignment_keeps_offsets_and_nulls() {
        use arrow_array::ListArray;
        use arrow_array::StringArray;
        use arrow_buffer::OffsetBuffer;
        let source = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            OffsetBuffer::new(vec![0_i32, 2, 2].into()),
            Arc::new(Int32Array::from(vec![5, 6])),
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap();
        let target = DataType::LargeList(Arc::new(Field::new("element", DataType::Int64, false)));
        let array = align_array(Arc::new(source), &target, FieldMatching::Path).unwrap();
        let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
        assert_eq!(list.offsets().as_ref(), &[0, 2, 2]);
        assert!(list.is_null(1));
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            6
        );

        let entries = StructArray::new(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int32Array::from(vec![7])),
            ],
            None,
        );
        let source = MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0_i32, 1, 1].into()),
            entries,
            Some(NullBuffer::from(vec![true, false])),
            false,
        )
        .unwrap();
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let array = align_array(Arc::new(source), &target, FieldMatching::Path).unwrap();
        let map = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.offsets().as_ref(), &[0, 1, 1]);
        assert!(map.is_null(1));
        assert_eq!(
            map.entries()
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
    }

    #[test]
    fn empty_projection_preserves_row_count() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(3)),
        )
        .unwrap();
        let output = transformer(&batch, vec![], FieldMatching::Path)
            .transform_record_batch(&batch)
            .unwrap();
        assert_eq!(output.num_columns(), 0);
        assert_eq!(output.num_rows(), 3);
    }
    #[test]
    fn flat_names_with_path_separators_and_quoted_names_are_supported() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a:b", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![10]))],
        )
        .unwrap();
        let output = transformer(
            &batch,
            vec![Field::new("\"A:B\"", DataType::Int64, false)],
            FieldMatching::Path,
        )
        .transform_record_batch(&batch)
        .unwrap();
        assert_eq!(int64(&output, 0, 0), 10);
    }
}
