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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::servers::flight::v1::scatter::FlightScatter;

#[derive(Clone)]
pub struct ModFlightScatter {
    scatter_size: usize,
    func_ctx: FunctionContext,
    expr: Expr,
}

impl ModFlightScatter {
    pub fn try_create(
        func_ctx: FunctionContext,
        expr: &RemoteExpr,
        scatter_size: usize,
    ) -> Result<Box<dyn FlightScatter>> {
        let expr = check_function(
            None,
            "modulo",
            &[],
            &[
                expr.as_expr(&BUILTIN_FUNCTIONS),
                Expr::constant(
                    Scalar::Number(NumberScalar::UInt64(scatter_size as u64)),
                    Some(DataType::Number(NumberDataType::UInt64)),
                ),
            ],
            &BUILTIN_FUNCTIONS,
        )?;
        let return_type = expr.data_type();
        if !matches!(return_type, DataType::Number(NumberDataType::UInt64)) {
            return Err(ErrorCode::Internal(format!(
                "ModFlightScatter expects modulo expression to return UInt64, but got {:?}",
                return_type
            )));
        }

        Ok(Box::new(ModFlightScatter {
            scatter_size,
            func_ctx,
            expr,
        }))
    }
}

impl FlightScatter for ModFlightScatter {
    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();

        let column = evaluator
            .run(&self.expr)?
            .into_full_column(&DataType::Number(NumberDataType::UInt64), num);
        let indices = column.as_number().unwrap().as_u_int64().unwrap();
        let data_blocks = DataBlock::scatter(&data_block, indices, self.scatter_size)?;

        let block_meta = data_block.get_meta();
        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.cloned())?);
        }

        Ok(res)
    }
}
