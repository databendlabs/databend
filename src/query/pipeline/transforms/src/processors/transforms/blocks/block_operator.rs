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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::apply_cse;

use crate::Transform;
use crate::Transformer;

/// `CompoundBlockOperator` is a pipeline of `BlockOperator`s
#[derive(Clone)]
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

        apply_cse(results, input_num_columns)
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

#[cfg(test)]
mod tests {
    use databend_common_expression::Expr;
    use databend_common_expression::FromData;
    use databend_common_expression::RawExpr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    use super::*;

    fn column(id: usize) -> RawExpr {
        RawExpr::ColumnRef {
            span: None,
            id,
            data_type: DataType::Number(NumberDataType::Int64),
            display_name: format!("column_{id}"),
        }
    }

    fn plus(expr: RawExpr, value: i64) -> RawExpr {
        RawExpr::FunctionCall {
            span: None,
            name: "plus".to_string(),
            params: vec![],
            args: vec![expr, RawExpr::Constant {
                span: None,
                scalar: Scalar::Number(NumberScalar::Int64(value)),
                data_type: None,
            }],
        }
    }

    #[test]
    fn test_compact_map_preserves_dependent_offsets() {
        let t = plus(plus(column(0), 1), 2);
        let p = plus(t.clone(), 3);
        let expr = |raw: RawExpr| -> Expr { check(&raw, &BUILTIN_FUNCTIONS).unwrap() };
        let operators = vec![
            BlockOperator::Map {
                exprs: vec![expr(plus(column(0), 100)), expr(t)],
                projections: None,
            },
            BlockOperator::Map {
                exprs: vec![expr(p.clone()), expr(p), expr(column(2))],
                projections: None,
            },
        ];
        let ctx = FunctionContext::default();
        let input = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![0, 1, 2, 3])]);
        let expected = operators
            .iter()
            .fold(input.clone(), |block, op| op.execute(&ctx, block).unwrap());
        let compacted = CompoundBlockOperator::compact_map(operators, 1);
        assert_eq!(compacted.len(), 1);
        let BlockOperator::Map { exprs, .. } = &compacted[0] else {
            unreachable!()
        };
        assert_eq!(exprs.len(), 7); // Only T and P are materialized; their child is pruned.
        let actual = compacted[0].execute(&ctx, input).unwrap();
        assert_eq!(actual.num_columns(), expected.num_columns());
        for column in 0..expected.num_columns() {
            assert_eq!(
                actual.get_by_offset(column).value(),
                expected.get_by_offset(column).value()
            );
        }
    }
}
