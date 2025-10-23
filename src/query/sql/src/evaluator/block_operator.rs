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

use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::ColumnSet;

/// `BlockOperator` takes a `DataBlock` as input and produces a `DataBlock` as output.
#[derive(Clone, Debug)]
pub enum BlockOperator {
    /// Batch mode of map which merges map operators into one.
    Map {
        exprs: Vec<Expr>,
        /// The index of the output columns, based on the exprs.
        projections: Option<ColumnSet>,
    },

    /// Reorganize the input [`DataBlock`] with `projection`.
    Project { projection: Vec<FieldIndex> },
}

impl BlockOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, mut input: DataBlock) -> Result<DataBlock> {
        if input.is_empty() {
            return Ok(input);
        }
        match self {
            BlockOperator::Map { exprs, projections } => {
                let num_evals = input
                    .get_meta()
                    .and_then(AggIndexMeta::downcast_ref_from)
                    .map(|a| a.num_evals);

                if let Some(num_evals) = num_evals {
                    // It's from aggregating index.
                    match projections {
                        Some(projections) => {
                            Ok(input.project_with_agg_index(projections, num_evals))
                        }
                        None => Ok(input),
                    }
                } else {
                    for expr in exprs {
                        let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                        let result = evaluator.run(expr)?;
                        input.add_value(result, expr.data_type().clone());
                    }
                    match projections {
                        Some(projections) => Ok(input.project(projections)),
                        None => Ok(input),
                    }
                }
            }

            BlockOperator::Project { projection } => {
                let entries = projection
                    .iter()
                    .map(|i| input.get_by_offset(*i).clone())
                    .collect();
                Ok(DataBlock::new_with_meta(
                    entries,
                    input.num_rows(),
                    input.take_meta(),
                ))
            }
        }
    }
}
