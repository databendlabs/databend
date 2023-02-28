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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::array::ArrayColumn;
use common_expression::types::AnyType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_expression::FunctionID;
use common_expression::Scalar;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

/// `BlockOperator` takes a `DataBlock` as input and produces a `DataBlock` as output.
#[derive(Clone)]
pub enum BlockOperator {
    /// Batch mode of map which merges map operators into one.
    Map { exprs: Vec<Expr> },

    /// Filter the input `DataBlock` with the predicate `eval`.
    Filter { expr: Expr },

    /// Reorganize the input `DataBlock` with `projection`.
    Project { projection: Vec<FieldIndex> },
    // Remap { indices: Vec<(IndexType, IndexType)> },
}

impl BlockOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, mut input: DataBlock) -> Result<DataBlock> {
        match self {
            BlockOperator::Map { exprs } => {
                let mut unnest_columns = vec![];
                let offset = input.num_columns();

                for (i, expr) in exprs.iter().enumerate() {
                    let evaluator = Evaluator::new(&input, *func_ctx, &BUILTIN_FUNCTIONS);

                    if let Expr::FunctionCall {
                        id: FunctionID::Builtin { name, .. },
                        ..
                    } = expr
                    {
                        if name == "unnest" {
                            let result = evaluator.run(expr)?;
                            let values_and_offsets =
                                result.into_column().unwrap().into_array().unwrap();
                            unnest_columns.push((i + offset, values_and_offsets));
                            // add a temporary dummy column.
                            input.add_column(BlockEntry {
                                data_type: DataType::Null,
                                value: Value::Scalar(Scalar::Null),
                            })
                        }
                    } else {
                        let result = evaluator.run(expr)?;
                        let col = BlockEntry {
                            data_type: expr.data_type().clone(),
                            value: result,
                        };
                        input.add_column(col);
                    }
                }

                Self::fit_unnest(input, &unnest_columns)
            }

            BlockOperator::Filter { expr } => {
                assert_eq!(expr.data_type(), &DataType::Boolean);

                let evaluator = Evaluator::new(&input, *func_ctx, &BUILTIN_FUNCTIONS);
                let filter = evaluator.run(expr)?.try_downcast::<BooleanType>().unwrap();
                input.filter_boolean_value(&filter)
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

    fn fit_unnest(
        input: DataBlock,
        unnest_columns: &[(usize, Box<ArrayColumn<AnyType>>)],
    ) -> Result<DataBlock> {
        if unnest_columns.is_empty() {
            return Ok(input);
        }

        // TODO: allow multiple unnest columns.
        if unnest_columns.len() > 1 {
            return Err(ErrorCode::Internal(
                "Only one unnest column is allowed now.",
            ));
        }

        let (unnest_index, unnest_col) = &unnest_columns[0];
        let (unnest_values, unnest_offsets) = (&unnest_col.values, &unnest_col.offsets);

        let num_rows = unnest_values.len() * input.num_rows();
        // Convert unnest_offsets to take indices.
        let mut take_indices = Vec::with_capacity(num_rows);
        let offset_len = unnest_offsets.len();
        for i in 1..offset_len {
            let (from, end) = unsafe {
                (
                    *unnest_offsets.get_unchecked(i - 1) as usize,
                    *unnest_offsets.get_unchecked(i) as usize,
                )
            };
            take_indices.extend(vec![i as u64; end - from]);
        }

        let mut cols = Vec::with_capacity(input.num_columns());
        let meta = input.get_meta().cloned();

        for (i, col) in input.columns().iter().enumerate() {
            if i == *unnest_index {
                let (from, end) = unsafe {
                    (
                        *unnest_offsets.get_unchecked(0) as usize,
                        *unnest_offsets.get_unchecked(offset_len - 1) as usize,
                    )
                };
                // `unnest_col` may be sliced once, so we should slice `unnest_values` to get the needed data.
                let col = unnest_values.slice(from..end);
                cols.push(BlockEntry {
                    data_type: unnest_values.data_type(),
                    value: Value::Column(col),
                })
            } else {
                match &col.value {
                    Value::Column(col) => {
                        let new_col = col.take(&take_indices);
                        cols.push(BlockEntry {
                            data_type: col.data_type(),
                            value: Value::Column(new_col),
                        })
                    }
                    Value::Scalar(_) => cols.push(col.clone()),
                }
            }
        }

        Ok(DataBlock::new_with_meta(cols, num_rows, meta))
    }
}

/// `CompoundBlockOperator` is a pipeline of `BlockOperator`s
pub struct CompoundBlockOperator {
    pub operators: Vec<BlockOperator>,
    pub ctx: FunctionContext,
}

impl CompoundBlockOperator {
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        ctx: FunctionContext,
        operators: Vec<BlockOperator>,
    ) -> Box<dyn Processor> {
        let operators = Self::compact_map(operators);
        Transformer::<Self>::create(input_port, output_port, Self { operators, ctx })
    }

    pub fn compact_map(operators: Vec<BlockOperator>) -> Vec<BlockOperator> {
        let mut results = Vec::with_capacity(operators.len());

        for op in operators {
            match op {
                BlockOperator::Map { exprs } => {
                    if let Some(BlockOperator::Map { exprs: pre_exprs }) = results.last_mut() {
                        pre_exprs.extend(exprs);
                    } else {
                        results.push(BlockOperator::Map { exprs });
                    }
                }
                _ => results.push(op),
            }
        }
        results
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
                        BlockOperator::Filter { .. } => "Filter",
                        BlockOperator::Project { .. } => "Project",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
