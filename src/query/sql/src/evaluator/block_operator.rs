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
use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::nullable::NullableColumnBuilder;
use common_expression::types::AnyType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::GenericType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
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

    /// Filter the input [`DataBlock`] with the predicate `eval`.
    Filter { expr: Expr },

    /// Reorganize the input [`DataBlock`] with `projection`.
    Project { projection: Vec<FieldIndex> },

    /// Unnest certain fields of the input [`DataBlock`].
    Unnest { num_columns: usize },
}

impl BlockOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, mut input: DataBlock) -> Result<DataBlock> {
        match self {
            BlockOperator::Map { exprs } => {
                for expr in exprs {
                    let evaluator = Evaluator::new(&input, *func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator.run(expr)?;
                    let col = BlockEntry {
                        data_type: expr.data_type().clone(),
                        value: result,
                    };
                    input.add_column(col);
                }
                Ok(input)
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

            BlockOperator::Unnest { num_columns } => {
                let num_rows = input.num_rows();
                let mut unnest_columns = Vec::with_capacity(*num_columns);
                for col in input
                    .columns()
                    .iter()
                    .skip(input.num_columns() - num_columns)
                {
                    let array_col = match &col.value {
                        Value::Scalar(Scalar::Array(col)) => {
                            ArrayColumnBuilder::<AnyType>::repeat(col, num_rows).build()
                        }
                        Value::Column(Column::Array(col)) => *col.clone(),
                        _ => {
                            return Err(ErrorCode::Internal(
                                "Unnest can only be applied to array types.",
                            ));
                        }
                    };
                    unnest_columns.push(array_col);
                }
                Self::fit_unnest(input, unnest_columns)
            }
        }
    }

    /// Apply the `unnest`ed columns to the whole [`DataBlock`].
    /// Each row in non-unnest columns will be replicated due to the related unnest column.
    ///
    /// For example:
    ///
    /// ```
    /// +---------+--------+
    /// | arr     | number |
    /// +---------+--------+
    /// | [1,2]   |      0 |
    /// | [3,4,5] |      1 |
    /// +---------+--------+
    /// ```
    ///
    /// After `unnest(arr)`, each row of `number` will be replicated by the length of `arr` at the same row.
    /// The result will be:
    ///
    /// ```
    /// +-------------+--------+
    /// | unnest(arr) | number |
    /// +-------------+--------+
    /// | 1           |      0 |
    /// | 2           |      0 |
    /// | 3           |      1 |
    /// | 4           |      1 |
    /// | 5           |      1 |
    /// +-------------+--------+
    /// ```
    ///
    /// If the argument of `unnest` is a scalar, like:
    ///
    /// ```sql
    /// select unnest([1,2,3]), number from numbers(2);
    /// ```
    ///
    /// The array scalar `[1,2,3]` will be replicated first (See the logic in `BlockOperator::execute`).
    fn fit_unnest(
        input: DataBlock,
        unnest_columns: Vec<ArrayColumn<AnyType>>,
    ) -> Result<DataBlock> {
        if unnest_columns.is_empty() {
            return Ok(input);
        }

        let (unnest_columns, unnest_offsets) = Self::unify_unnest_columns(unnest_columns);
        let num_rows = *unnest_offsets.last().unwrap();

        // Convert unnest_offsets to take indices.
        let mut take_indices = Vec::with_capacity(num_rows);
        for (i, offset) in unnest_offsets.windows(2).enumerate() {
            take_indices.extend(vec![i as u64; offset[1] - offset[0]]);
        }

        let mut cols = Vec::with_capacity(input.num_columns());
        let meta = input.get_meta().cloned();

        let num_non_unnest = input.num_columns() - unnest_columns.len();
        for col in input.columns().iter().take(num_non_unnest) {
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
        for col in unnest_columns {
            cols.push(BlockEntry {
                data_type: col.data_type(),
                value: Value::Column(col),
            })
        }

        Ok(DataBlock::new_with_meta(cols, num_rows, meta))
    }

    /// Unify all unnest columns. Make the length of each column be the same.
    ///
    /// Return the unified unnested columns and the unnest offsets for replicating non-unnest columns.
    ///
    /// If the original length of each column is different, the shorter ones will be padded with nulls.
    ///
    /// For example:
    ///
    /// ```
    /// +---------+---------+
    /// | origin1 | origin2 |
    /// +---------+---------+
    /// | [1,2]   | [1,2,3] |
    /// | [3,4,5] | [4,5]   |
    /// +---------+---------+
    /// ```
    ///
    /// will be unnested to:
    ///
    /// ```
    /// +---------+---------+
    /// | unnest1 | unnest2 |
    /// +------ --+---------+
    /// | 1       | 1       |
    /// | 2       | 2       |
    /// | NULL    | 3       |
    /// | 3       | 4       |
    /// | 4       | 5       |
    /// | 5       | NULL    |
    /// +---------+---------+
    /// ```
    fn unify_unnest_columns(
        unnest_columns: Vec<ArrayColumn<AnyType>>,
    ) -> (Vec<Column>, Vec<usize>) {
        debug_assert!(!unnest_columns.is_empty());
        let num_rows = unnest_columns[0].len(); // Rows of the original `ArrayColumn`s.
        debug_assert!(unnest_columns.iter().all(|col| col.len() == num_rows));
        // Some pre-computed variables.
        let unnest_columns = unnest_columns
            .into_iter()
            .map(|col| {
                let typ = col.values.data_type().unnest();
                let arrays = col.iter().map(|c| c.unnest()).collect::<Vec<_>>();
                (typ, arrays)
            })
            .collect::<Vec<_>>();

        // 1. Compute the offsets after unnesting.
        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.push(0);
        let mut new_num_rows = 0; // Rows of the unnested `Column`s.
        for row in 0..num_rows {
            let len = unnest_columns
                .iter()
                .map(|(_, col)| unsafe { col.get_unchecked(row).len() })
                .max()
                .unwrap();
            new_num_rows += len;
            offsets.push(new_num_rows);
        }

        // 2. Insert NULLs to shorter arrays at each row.
        let mut col_builders = unnest_columns
            .iter()
            .map(|(typ, _)| {
                NullableColumnBuilder::<GenericType<0>>::with_capacity(new_num_rows, &[typ.clone()])
            })
            .collect::<Vec<_>>();

        for (row, w) in offsets.windows(2).enumerate() {
            let len = w[1] - w[0];
            for ((typ, cols), builder) in unnest_columns.iter().zip(col_builders.iter_mut()) {
                let inner_col = unsafe { cols.get_unchecked(row) };
                let inner_len = inner_col.len();
                debug_assert!(inner_len <= len);
                builder.builder.append_column(inner_col);
                builder.validity.extend_constant(inner_len, true);
                // Avoid using `if branch`.
                let d = Scalar::default_value(typ);
                let defaults = ColumnBuilder::repeat(&d.as_ref(), len - inner_len, typ).build();
                builder.builder.append_column(&defaults);
                builder.validity.extend_constant(len - inner_len, false);
            }
        }

        let new_columns = col_builders
            .into_iter()
            .map(|builder| Column::Nullable(Box::new(builder.build().upcast())))
            .collect();
        (new_columns, offsets)
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
                        BlockOperator::Unnest { .. } => "Unnest",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
