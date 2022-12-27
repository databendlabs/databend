// Copyright 2022 Datafuse Labs.
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

use std::collections::HashSet;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::ChunkEntry;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

use crate::IndexType;

/// `ChunkOperator` takes a `DataBlock` as input and produces a `DataBlock` as output.
#[derive(Clone)]
pub enum ChunkOperator {
    /// Evaluate expression and append result column to the end.
    Map {
        /// Index of the result in `Chunk`
        index: IndexType,
        expr: Expr,
    },

    /// Filter the input `DataBlock` with the predicate `eval`.
    Filter { expr: Expr },

    /// Reorganize the input `Chunk` with `indices`.
    Project { indices: HashSet<IndexType> },
    // Remap { indices: Vec<(IndexType, IndexType)> },
}

impl ChunkOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, mut input: Chunk) -> Result<Chunk> {
        match self {
            ChunkOperator::Map { index, expr } => {
                let registry = &BUILTIN_FUNCTIONS;
                let evaluator = Evaluator::new(&input, *func_ctx, registry);
                let result = evaluator
                    .run(expr)
                    .map_err(|(_, e)| ErrorCode::Internal(e))?;
                let entry = ChunkEntry {
                    id: *index,
                    data_type: expr.data_type().clone(),
                    value: result,
                };
                input.add_column(entry);
                Ok(input)
            }

            ChunkOperator::Filter { expr } => {
                let registry = &BUILTIN_FUNCTIONS;
                let evaluator = Evaluator::new(&input, *func_ctx, registry);
                let filter = evaluator
                    .run(expr)
                    .map_err(|(_, e)| ErrorCode::Internal(e))?;
                Chunk::filter(input, &filter)
            }

            ChunkOperator::Project { indices } => {
                let mut result = Chunk::new(vec![], input.num_rows());
                for id in indices {
                    result.add_column(input.get_by_offset(*id).clone());
                }
                Ok(result)
            }
        }
    }
}

/// `CompoundChunkOperator` is a pipeline of `ChunkOperator`s
pub struct CompoundChunkOperator {
    pub operators: Vec<ChunkOperator>,
    pub ctx: FunctionContext,
}

impl CompoundChunkOperator {
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        ctx: FunctionContext,
        operators: Vec<ChunkOperator>,
    ) -> ProcessorPtr {
        Transformer::<Self>::create(input_port, output_port, Self { operators, ctx })
    }

    #[allow(dead_code)]
    pub fn append(self, operator: ChunkOperator) -> Self {
        let mut result = self;
        result.operators.push(operator);
        result
    }

    #[allow(dead_code)]
    pub fn merge(self, other: Self) -> Self {
        let mut operators = self.operators;
        operators.extend(other.operators);
        Self {
            operators,
            ctx: self.ctx,
        }
    }
}

impl Transform for CompoundChunkOperator {
    const NAME: &'static str = "CompoundChunkOperator";

    const SKIP_EMPTY_CHUNK: bool = true;

    fn transform(&mut self, data: Chunk) -> Result<Chunk> {
        self.operators
            .iter()
            .try_fold(data, |input, op| op.execute(&self.ctx, input))
    }

    fn name(&self) -> String {
        format!(
            "{}({})",
            Self::NAME,
            self.operators
                .iter()
                .map(|op| {
                    match op {
                        ChunkOperator::Map { .. } => "Map",
                        ChunkOperator::Filter { .. } => "Filter",
                        ChunkOperator::Project { .. } => "Project",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
