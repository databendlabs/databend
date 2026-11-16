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

use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::TopK;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataSchema;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TopKSorter;
use databend_common_expression::types::Bitmap;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::schema::types::SchemaDescriptor;

use super::utils::BatchReadPlan;
use super::utils::compute_output_read_plan;
use crate::read_schema::ParquetReadSchema;

pub struct ParquetTopK {
    projection: ProjectionMask,
    field_levels: FieldLevels,
    read_plan: Option<BatchReadPlan>,
    data_schema: DataSchema,
}

impl ParquetTopK {
    pub fn new(
        projection: ProjectionMask,
        field_levels: FieldLevels,
        read_plan: Option<BatchReadPlan>,
        data_schema: DataSchema,
    ) -> Self {
        Self {
            projection,
            field_levels,
            read_plan,
            data_schema,
        }
    }

    pub fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    pub fn field_levels(&self) -> &FieldLevels {
        &self.field_levels
    }

    pub fn read_plan(&self) -> &Option<BatchReadPlan> {
        &self.read_plan
    }

    pub fn data_schema(&self) -> &DataSchema {
        &self.data_schema
    }

    pub fn evaluate_column(&self, column: &Column, sorter: &mut TopKSorter) -> Bitmap {
        let num_rows = column.len();
        let bitmap = Bitmap::new_constant(true, num_rows);
        let mut bitmap = bitmap.make_mut();
        sorter.push_column(column, &mut bitmap);
        bitmap.into()
    }
}

/// The information used for evaluate TopK.
pub struct BuiltTopK {
    pub topk: Arc<ParquetTopK>,
    pub field: TableField,
    pub leaf_id: usize,
}

/// Build [`TopK`] into [`ParquetTopK`] and get its [`TableField`].
pub fn build_topk(
    topk: &TopK,
    schema_desc: &SchemaDescriptor,
    arrow_schema: Option<&arrow_schema::Schema>,
    read_schema: Option<&ParquetReadSchema>,
) -> Result<BuiltTopK> {
    let schema = TableSchema::new(vec![topk.field.clone()]);
    let leaf_id = if let Some(read_schema) = read_schema {
        let (_, leaves) =
            read_schema.project(&Projection::Columns(vec![0]), &schema, schema_desc)?;
        if leaves.len() != 1 {
            return Err(ErrorCode::TableSchemaMismatch(
                "TopK requires exactly one Parquet leaf",
            ));
        }
        leaves[0]
    } else {
        topk.leaf_id
    };
    let projection = ProjectionMask::leaves(schema_desc, vec![leaf_id]);
    let field_levels = parquet_to_arrow_field_levels(
        schema_desc,
        projection.clone(),
        arrow_schema.map(|s| &s.fields),
    )?;
    let read_plan = compute_output_read_plan(
        schema_desc,
        &projection,
        &schema,
        topk.field.name.contains(':'),
        read_schema,
        arrow_schema,
    )?;

    Ok(BuiltTopK {
        topk: Arc::new(ParquetTopK::new(
            projection,
            field_levels,
            read_plan,
            DataSchema::from(&schema),
        )),
        field: topk.field.clone(),
        leaf_id,
    })
}
