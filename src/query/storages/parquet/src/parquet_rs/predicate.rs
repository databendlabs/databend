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
use arrow_schema::FieldRef;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;

use super::parquet_reader::transform_record_batch;

pub struct ParquetPredicate {
    func_ctx: FunctionContext,

    /// Columns used for eval predicate.
    projection: ProjectionMask,
    /// Projected field levels.
    field_levels: FieldLevels,

    /// Predicate filter expression.
    filter: Expr,
    field_paths: Vec<(FieldRef, Vec<FieldIndex>)>,

    inner_projected: bool,
}

impl ParquetPredicate {
    pub fn new(
        func_ctx: FunctionContext,
        projection: ProjectionMask,
        field_levels: FieldLevels,
        filter: Expr,
        field_paths: Vec<(FieldRef, Vec<FieldIndex>)>,
        inner_projected: bool,
    ) -> Self {
        Self {
            func_ctx,
            projection,
            field_levels,
            filter,
            field_paths,
            inner_projected,
        }
    }

    pub fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    pub fn field_levels(&self) -> &FieldLevels {
        &self.field_levels
    }

    pub fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        let block = if self.inner_projected {
            transform_record_batch(batch, &self.field_paths)?
        } else {
            let (block, _) = DataBlock::from_record_batch(batch)?;
            block
        };
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let res = evaluator
            .run(&self.filter)?
            .convert_to_full_column(&DataType::Boolean, batch.num_rows())
            .into_arrow_rs()?;
        Ok(BooleanArray::from(res.to_data()))
    }
}
