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

use std::str::FromStr;

use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::Bitmap;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::parquet_to_arrow_schema_by_columns;
use parquet::schema::types::SchemaDescriptor;

use crate::iceberg_schema::ICEBERG_ARROW_SCHEMA;
use crate::iceberg_schema::align_array;
use crate::iceberg_schema::target_field;
use crate::iceberg_schema::target_field_id;
use crate::transformer::RecordBatchTransformer;

pub type DataBlockIterator = Box<dyn Iterator<Item = Result<DataBlock>> + Send>;

/// Traverse `batch` by `path_indices` to get output [`Column`].
fn traverse_column(
    field: &DataField,
    path: &[FieldIndex],
    batch: &RecordBatch,
    schema: &arrow_schema::Schema,
) -> Result<Column> {
    assert!(!path.is_empty());
    let mut columns = batch.columns();
    for idx in path.iter().take(path.len() - 1) {
        let struct_array = columns
            .get(*idx)
            .ok_or_else(|| error_cannot_traverse_path(path, schema))?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error_cannot_traverse_path(path, schema))?;
        columns = struct_array.columns();
    }
    let idx = *path.last().unwrap();
    let array = columns
        .get(idx)
        .ok_or_else(|| error_cannot_traverse_path(path, schema))?;
    Column::from_arrow_rs(array.clone(), field.data_type())
}

fn error_cannot_traverse_path(path: &[FieldIndex], schema: &arrow_schema::Schema) -> ErrorCode {
    ErrorCode::TableSchemaMismatch(format!(
        "Cannot traverse path {:?} in the arrow schema {:?}",
        path, schema
    ))
}

/// Transform a [`RecordBatch`] to [`DataBlock`].
///
/// `field_paths` is used to traverse nested columns in `batch`.
pub fn transform_record_batch(
    data_schema: &DataSchema,
    batch: &RecordBatch,
    field_paths: &Option<FieldPaths>,
) -> Result<DataBlock> {
    if let Some(json) = data_schema.meta().get(ICEBERG_ARROW_SCHEMA) {
        let target: Schema = serde_json::from_str(json).map_err(|e| {
            ErrorCode::TableSchemaMismatch(format!("Invalid Iceberg Arrow schema: {e}"))
        })?;
        let sources =
            RecordBatchTransformer::build_field_id_to_arrow_schema_map(batch.schema_ref())?;
        // Prefetch policies pass a combined schema here but only read a subset
        // of its fields. FieldPaths describes that exact subset and output order.
        let fields: Vec<_> = match field_paths {
            Some(paths) => paths.iter().map(|(field, _)| field).collect(),
            None => data_schema.fields().iter().collect(),
        };
        let mut columns = Vec::with_capacity(fields.len());
        for field in fields {
            let id = target_field_id(&target, field.name())?;
            let id = u32::try_from(id)
                .map_err(|_| ErrorCode::TableSchemaMismatch("Invalid Iceberg field ID"))?;
            let (_, index, nested) = sources.get(&id).ok_or_else(|| {
                ErrorCode::TableSchemaMismatch(format!(
                    "Iceberg field {} (ID {id}) does not exist in the batch",
                    field.name()
                ))
            })?;
            let array = RecordBatchTransformer::extract_nested_array(batch.column(*index), nested)?;
            // DataSchema's Tuple type has no child names. Recover the named
            // target from Iceberg before aligning structs (including in lists/maps).
            let iceberg_field = target_field(&target, field.name())?;
            let target_field = Field::from(&TableField::try_from(iceberg_field)?);
            let array = align_array(array, target_field.data_type())?;
            columns.push(Column::from_arrow_rs(array, field.data_type())?);
        }
        return Ok(if columns.is_empty() {
            DataBlock::new(vec![], batch.num_rows())
        } else {
            DataBlock::new_from_columns(columns)
        });
    }
    if let Some(field_paths) = field_paths {
        let schema_aligned = batch.schema().fields().len() == data_schema.fields().len()
            && batch
                .schema()
                .fields()
                .iter()
                .zip(data_schema.fields().iter())
                .all(|(arrow_field, data_field)| arrow_field.name() == data_field.name());

        if schema_aligned {
            return DataBlock::from_record_batch(data_schema, batch);
        }

        return transform_record_batch_by_field_paths(batch, field_paths);
    }

    DataBlock::from_record_batch(data_schema, batch)
}

pub fn transform_record_batch_by_field_paths(
    batch: &RecordBatch,
    field_paths: &[(DataField, Vec<FieldIndex>)],
) -> Result<DataBlock> {
    if batch.num_columns() == 0 {
        return Ok(DataBlock::new(vec![], batch.num_rows()));
    }
    let mut columns = Vec::with_capacity(field_paths.len());
    let schema = batch.schema();
    for (field, path) in field_paths.iter() {
        let col = traverse_column(field, path, batch, &schema)?;
        columns.push(col);
    }
    Ok(DataBlock::new_from_columns(columns))
}

pub fn bitmap_to_boolean_array(bitmap: Bitmap) -> BooleanArray {
    BooleanArray::from(bitmap.into_array_data())
}

/// FieldPaths is used to traverse nested columns in [`RecordBatch`].
///
/// It records the DFS order of each field.
pub type FieldPaths = Vec<(DataField, Vec<FieldIndex>)>;

/// Search `batch_schema` by column names from `output_schema` to compute path indices for getting columns from [`RecordBatch`].
pub fn compute_output_field_paths(
    schema_desc: &SchemaDescriptor,
    projection: &ProjectionMask,
    expected_schema: &TableSchema,
    inner_projection: bool,
) -> Result<Option<FieldPaths>> {
    if !inner_projection {
        return Ok(None);
    }
    let iceberg = expected_schema.metadata.contains_key(ICEBERG_ARROW_SCHEMA);
    let expected_schema = DataSchema::from(expected_schema);
    let batch_schema = parquet_to_arrow_schema_by_columns(schema_desc, projection.clone(), None)?;
    let output_fields = expected_schema.fields();
    let parquet_schema_desc = ArrowSchemaConverter::new().convert(&batch_schema)?;

    let parquet_schema = parquet_schema_desc.root_schema();

    let mut path_indices = Vec::with_capacity(output_fields.len());
    for field in output_fields {
        let name_path = field.name().split(':').collect::<Vec<_>>();
        assert!(!name_path.is_empty());
        let mut path = Vec::with_capacity(name_path.len());
        let mut ty = parquet_schema;
        for name in name_path {
            match ty {
                parquet::schema::types::Type::GroupType { fields, .. } => {
                    let idx = fields
                        .iter()
                        .position(|t| {
                            if iceberg {
                                t.name() == name
                            } else {
                                parquet_field_name_matches_path_name(t.name(), name)
                            }
                        })
                        .ok_or_else(|| error_cannot_find_field(field.name(), parquet_schema))?;
                    path.push(idx);
                    ty = &fields[idx];
                }
                _ => return Err(error_cannot_find_field(field.name(), parquet_schema)),
            }
        }

        path_indices.push((field.clone(), path));
    }

    Ok(Some(path_indices))
}

pub fn error_cannot_find_field(name: &str, schema: &parquet::schema::types::Type) -> ErrorCode {
    ErrorCode::TableSchemaMismatch(format!(
        "Cannot find field {} in the parquet schema {:?}",
        name, schema
    ))
}

fn parquet_field_name_matches_path_name(parquet_field_name: &str, path_name: &str) -> bool {
    parquet_field_name.eq_ignore_ascii_case(path_name)
        || QuotedIdent::<String>::from_str(path_name)
            .is_ok_and(|quoted| parquet_field_name.eq_ignore_ascii_case(&quoted.0))
}

#[cfg(test)]
mod tests {
    use super::parquet_field_name_matches_path_name;

    #[test]
    fn test_parquet_field_name_matches_quoted_path_name() {
        assert!(parquet_field_name_matches_path_name("A", "\"A\""));
        assert!(parquet_field_name_matches_path_name("A", "\"a\""));
        assert!(parquet_field_name_matches_path_name("simple", "simple"));
        assert!(parquet_field_name_matches_path_name("simple", "SIMPLE"));
        assert!(!parquet_field_name_matches_path_name("A", "\"B\""));
    }
}
