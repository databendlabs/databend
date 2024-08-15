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

use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_functions::BUILTIN_FUNCTIONS;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use super::utils::bitmap_to_boolean_array;
use super::utils::transform_record_batch;
use super::utils::FieldPaths;
use crate::parquet_rs::parquet_reader::utils::compute_output_field_paths;

pub struct ParquetPredicate {
    func_ctx: FunctionContext,

    /// Columns used for eval predicate.
    projection: ProjectionMask,
    field_levels: FieldLevels,

    /// Predicate filter expression.
    filter: Expr,
    field_paths: Option<FieldPaths>,

    schema: TableSchema,
}

impl ParquetPredicate {
    pub fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    pub fn field_levels(&self) -> &FieldLevels {
        &self.field_levels
    }

    pub fn field_paths(&self) -> &Option<FieldPaths> {
        &self.field_paths
    }

    pub fn evaluate_block(&self, block: &DataBlock) -> Result<Bitmap> {
        let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let res = evaluator
            .run(&self.filter)?
            .convert_to_full_column(&DataType::Boolean, block.num_rows())
            .as_boolean()
            .cloned()
            .unwrap();
        Ok(res)
    }

    pub fn evaluate(
        &self,
        batch: &RecordBatch,
        partition_block_entries: Option<Vec<BlockEntry>>,
    ) -> Result<BooleanArray> {
        let data_schema = DataSchema::from(&self.schema);
        let block = transform_record_batch(&data_schema, batch, &self.field_paths)?;
        let block = if let Some(partition_block_entries) = partition_block_entries {
            let mut columns = block.columns().to_vec();
            columns.extend_from_slice(&partition_block_entries);
            DataBlock::new(columns, block.num_rows())
        } else {
            block
        };
        let res = self.evaluate_block(&block)?;
        Ok(bitmap_to_boolean_array(res))
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Build [`PrewhereInfo`] into [`ParquetPredicate`] and get the leave columnd ids.
pub fn build_predicate(
    func_ctx: FunctionContext,
    prewhere: &PrewhereInfo,
    table_schema: &TableSchema,
    schema_desc: &SchemaDescriptor,
    partition_columns: &[String],
    arrow_schema: Option<&arrow_schema::Schema>,
) -> Result<(Arc<ParquetPredicate>, Vec<usize>)> {
    let inner_projection = matches!(prewhere.output_columns, Projection::InnerColumns(_));
    let schema = prewhere.prewhere_columns.project_schema(table_schema);
    let filter = prewhere
        .filter
        .as_expr(&BUILTIN_FUNCTIONS)
        .project_column_ref(
            |name| match partition_columns.iter().position(|x| x == name) {
                Some(i) => i + schema.fields.len(),
                None => schema.index_of(name).unwrap(),
            },
        );
    let (projection, leaves) = prewhere.prewhere_columns.to_arrow_projection(schema_desc);
    let field_paths =
        compute_output_field_paths(schema_desc, &projection, &schema, inner_projection)?;
    let field_levels = parquet_to_arrow_field_levels(
        schema_desc,
        projection.clone(),
        arrow_schema.map(|s| &s.fields),
    )?;

    Ok((
        Arc::new(ParquetPredicate {
            func_ctx,
            projection,
            filter,
            field_levels,
            field_paths,
            schema,
        }),
        leaves,
    ))
}
