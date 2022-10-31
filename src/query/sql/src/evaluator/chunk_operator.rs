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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

use crate::evaluator::EvalNode;

/// `ChunkOperator` takes a `DataBlock` as input and produces a `DataBlock` as output.
#[derive(Clone)]
pub enum ChunkOperator {
    /// Evaluate expression and append result column to the end.
    Map {
        /// Name of column in `DataBlock`, should be deprecated later.
        name: String,
        eval: EvalNode,
    },

    /// Filter the input `DataBlock` with the predicate `eval`.
    Filter { eval: EvalNode },

    /// Reorganize the input `DataBlock` with `offsets`.
    Project { offsets: Vec<usize> },

    /// Replace name of `DataField`s of input `DataBlock`.
    Rename { output_schema: DataSchemaRef },
}

impl ChunkOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, input: DataBlock) -> Result<DataBlock> {
        match self {
            ChunkOperator::Map { name, eval } => {
                let result = eval.eval(func_ctx, &input)?;
                input.add_column(result.vector, DataField::new(name, result.logical_type))
            }

            ChunkOperator::Filter { eval } => {
                let result = eval.eval(func_ctx, &input)?;
                let predicate = result.vector;
                DataBlock::filter_block(input, &predicate)
            }

            ChunkOperator::Project { offsets } => {
                let mut result = DataBlock::empty();
                for offset in offsets {
                    result = result.add_column(
                        input.column(*offset).clone(),
                        input.schema().field(*offset).clone(),
                    )?;
                }
                Ok(result)
            }

            ChunkOperator::Rename { output_schema } => Ok(DataBlock::create(
                output_schema.clone(),
                input.columns().to_vec(),
            )),
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

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
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
                        ChunkOperator::Rename { .. } => "Rename",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
