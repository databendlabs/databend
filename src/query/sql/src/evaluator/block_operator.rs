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

use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;

use crate::optimizer::ColumnSet;

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

/// `RemoteExpr` version of `BlockOperator`, which can be shared between nodes
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum RemoteBlockOperator {
    /// Batch mode of map which merges map operators into one.
    Map {
        exprs: Vec<RemoteExpr>,
        /// The index of the output columns, based on the exprs.
        projections: Option<ColumnSet>,
    },

    /// Reorganize the input [`DataBlock`] with `projection`.
    Project { projection: Vec<FieldIndex> },
}

impl From<BlockOperator> for RemoteBlockOperator {
    fn from(operator: BlockOperator) -> Self {
        match operator {
            BlockOperator::Map { exprs, projections } => {
                let exprs = exprs
                    .iter()
                    .map(|expr| expr.as_remote_expr())
                    .collect::<Vec<_>>();
                Self::Map { exprs, projections }
            }
            BlockOperator::Project { projection } => Self::Project { projection },
        }
    }
}

impl From<RemoteBlockOperator> for BlockOperator {
    fn from(operator: RemoteBlockOperator) -> Self {
        match operator {
            RemoteBlockOperator::Map { exprs, projections } => {
                let exprs = exprs
                    .iter()
                    .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
                    .collect::<Vec<_>>();
                Self::Map { exprs, projections }
            }
            RemoteBlockOperator::Project { projection } => Self::Project { projection },
        }
    }
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
                        let col = BlockEntry::new(expr.data_type().clone(), result);
                        input.add_column(col);
                    }
                    match projections {
                        Some(projections) => Ok(input.project(projections)),
                        None => Ok(input),
                    }
                }
            }

            BlockOperator::Project { projection } => {
                let mut result = DataBlock::new(vec![], input.num_rows());
                for index in projection {
                    result.add_column(input.get_by_offset(*index).clone());
                }
                Ok(result)
            }
        }
    }
}

/// `CompoundBlockOperator` is a pipeline of `BlockOperator`s
pub struct CompoundBlockOperator {
    pub operators: Vec<BlockOperator>,
    pub ctx: FunctionContext,
}

impl CompoundBlockOperator {
    pub fn new(
        operators: Vec<BlockOperator>,
        ctx: FunctionContext,
        input_num_columns: usize,
    ) -> Self {
        let operators = Self::compact_map(operators, input_num_columns);
        Self { operators, ctx }
    }

    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        input_num_columns: usize,
        ctx: FunctionContext,
        operators: Vec<BlockOperator>,
    ) -> Box<dyn Processor> {
        let operators = Self::compact_map(operators, input_num_columns);
        Transformer::<Self>::create(input_port, output_port, Self { operators, ctx })
    }

    pub fn compact_map(
        operators: Vec<BlockOperator>,
        input_num_columns: usize,
    ) -> Vec<BlockOperator> {
        let mut results = Vec::with_capacity(operators.len());

        for op in operators {
            match op {
                BlockOperator::Map { exprs, projections } => {
                    if let Some(BlockOperator::Map {
                        exprs: pre_exprs,
                        projections: pre_projections,
                    }) = results.last_mut()
                    {
                        if pre_projections.is_none() && projections.is_none() {
                            pre_exprs.extend(exprs);
                        } else {
                            results.push(BlockOperator::Map { exprs, projections });
                        }
                    } else {
                        results.push(BlockOperator::Map { exprs, projections });
                    }
                }
                _ => results.push(op),
            }
        }

        crate::evaluator::cse::apply_cse(results, input_num_columns)
    }
}

impl Transform for CompoundBlockOperator {
    const NAME: &'static str = "CompoundBlockOperator";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        self.operators
            .iter()
            .try_fold(data_block, |input, op| op.execute(&self.ctx, input))
    }

    fn name(&self) -> String {
        format!(
            "{}({})",
            Self::NAME,
            self.operators
                .iter()
                .map(|op| {
                    match op {
                        BlockOperator::Map { .. } => "Map",
                        BlockOperator::Project { .. } => "Project",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
