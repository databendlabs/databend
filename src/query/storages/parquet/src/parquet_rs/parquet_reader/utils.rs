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

use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use databend_common_arrow::arrow::array::Arrow2Arrow;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableSchema;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::parquet_to_arrow_schema_by_columns;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

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
    Ok(Column::from_arrow_rs(array.clone(), field)?)
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
    if let Some(field_paths) = field_paths {
        transform_record_batch_by_field_paths(batch, field_paths)
    } else {
        let (block, _) = DataBlock::from_record_batch(data_schema, batch)?;
        Ok(block)
    }
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
    let res = Box::new(
        databend_common_arrow::arrow::array::BooleanArray::try_new(
            databend_common_arrow::arrow::datatypes::DataType::Boolean,
            bitmap,
            None,
        )
        .unwrap(),
    );
    BooleanArray::from(res.to_data())
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
    let expected_schema = DataSchema::from(expected_schema);
    let batch_schema = parquet_to_arrow_schema_by_columns(schema_desc, projection.clone(), None)?;
    let output_fields = expected_schema.fields();
    let parquet_schema_desc = arrow_to_parquet_schema(&batch_schema)?;
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
                        .position(|t| t.name().eq_ignore_ascii_case(name))
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
