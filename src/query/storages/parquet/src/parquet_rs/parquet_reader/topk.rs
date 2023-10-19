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

use common_arrow::arrow::bitmap::Bitmap;
use common_catalog::plan::TopK;
use common_exception::Result;
use common_expression::Column;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TopKSorter;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use super::utils::compute_output_field_paths;
use super::utils::FieldPaths;

pub struct ParquetTopK {
    projection: ProjectionMask,
    field_levels: FieldLevels,
    field_paths: Option<FieldPaths>,
}

impl ParquetTopK {
    pub fn new(
        projection: ProjectionMask,
        field_levels: FieldLevels,
        field_paths: Option<FieldPaths>,
    ) -> Self {
        Self {
            projection,
            field_levels,
            field_paths,
        }
    }

    pub fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    pub fn field_levels(&self) -> &FieldLevels {
        &self.field_levels
    }

    pub fn field_paths(&self) -> &Option<FieldPaths> {
        &self.field_paths
    }

    pub fn evaluate_column(&self, column: &Column, sorter: &mut TopKSorter) -> Bitmap {
        let num_rows = column.len();
        let bitmap = Bitmap::new_constant(true, num_rows);
        let mut bitmap = bitmap.make_mut();
        sorter.push_column(column, &mut bitmap);
        bitmap.into()
    }
}

/// The information used for evalaute TopK.
pub struct BuiltTopK {
    pub topk: Arc<ParquetTopK>,
    pub field: TableField,
    pub leaf_id: usize,
}

/// Build [`TopK`] into [`ParquetTopK`] and get its [`TableField`].
pub fn build_topk(topk: &TopK, schema_desc: &SchemaDescriptor) -> Result<BuiltTopK> {
    let projection = ProjectionMask::leaves(schema_desc, vec![topk.leaf_id]);
    let field_levels = parquet_to_arrow_field_levels(schema_desc, projection.clone(), None)?;
    let field_paths = if topk.field.name.contains(':') {
        // It's a inner column
        compute_output_field_paths(
            schema_desc,
            &projection,
            &TableSchema::new(vec![topk.field.clone()]),
            true,
        )?
    } else {
        None
    };

    Ok(BuiltTopK {
        topk: Arc::new(ParquetTopK::new(projection, field_levels, field_paths)),
        field: topk.field.clone(),
        leaf_id: topk.leaf_id,
    })
}
