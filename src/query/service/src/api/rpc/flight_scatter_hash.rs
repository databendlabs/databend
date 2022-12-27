// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check;
use common_expression::types::number::NumberScalar;
use common_expression::types::AnyType;
use common_expression::types::NullableType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::RawExpr;
use common_expression::Value;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_sql::executor::PhysicalScalar;

// use common_sql::executor::PhysicalScalar;
use crate::api::rpc::flight_scatter::FlightScatter;

#[derive(Clone)]
pub struct HashFlightScatter {
    func_ctx: FunctionContext,
    hash_key: Expr,
    scatter_size: usize,
}

impl HashFlightScatter {
    pub fn try_create(
        func_ctx: FunctionContext,
        scalars: Vec<PhysicalScalar>,
        scatter_size: usize,
    ) -> Result<Box<dyn FlightScatter>> {
        if scalars.len() == 1 {
            return OneHashKeyFlightScatter::try_create(func_ctx, &scalars[0], scatter_size);
        }
        let hash_keys: Vec<RawExpr> = scalars.iter().map(|e| e.as_raw_expr()).collect();

        let hash_raw = RawExpr::FunctionCall {
            span: None,
            name: "siphash".to_string(),
            params: vec![],
            args: hash_keys,
        };
        let hash_key = check(&hash_raw, &BUILTIN_FUNCTIONS)
            .map_err(|(_, _e)| ErrorCode::Internal("Invalid expression"))?;

        Ok(Box::new(Self {
            func_ctx,
            hash_key,
            scatter_size,
        }))
    }
}

#[derive(Clone)]
struct OneHashKeyFlightScatter {
    scatter_size: usize,
    func_ctx: FunctionContext,
    indices_scalar: Expr,
}

impl OneHashKeyFlightScatter {
    pub fn try_create(
        func_ctx: FunctionContext,
        scalar: &PhysicalScalar,
        scatter_size: usize,
    ) -> Result<Box<dyn FlightScatter>> {
        let hash_key = scalar.as_raw_expr();
        let hash_raw = RawExpr::FunctionCall {
            span: None,
            name: "siphash".to_string(),
            params: vec![],
            args: vec![hash_key],
        };

        let indices_scalar = check(&hash_raw, &BUILTIN_FUNCTIONS)
            .map_err(|(_, _e)| ErrorCode::Internal("Invalid expression"))?;

        Ok(Box::new(OneHashKeyFlightScatter {
            scatter_size,
            func_ctx,
            indices_scalar,
        }))
    }
}

impl FlightScatter for OneHashKeyFlightScatter {
    fn execute(&self, data_block: &DataBlock, num: usize) -> Result<Vec<DataBlock>> {
        let evaluator = Evaluator::new(data_block, self.func_ctx, &BUILTIN_FUNCTIONS);

        let indices = evaluator.run(&self.indices_scalar).unwrap();
        let indices = get_hash_values(&indices, num)?;
        let data_blocks = DataBlock::scatter(data_block, &indices, self.scatter_size)?;

        let block_meta = data_block.meta()?;
        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.clone())?);
        }

        Ok(res)
    }
}

impl FlightScatter for HashFlightScatter {
    fn execute(&self, data_block: &DataBlock, num: usize) -> Result<Vec<DataBlock>> {
        let evaluator = Evaluator::new(data_block, self.func_ctx, &BUILTIN_FUNCTIONS);

        let indices = evaluator.run(&self.hash_key).unwrap();
        let indices = get_hash_values(&indices, num)?;

        let block_meta = data_block.meta()?;
        let data_blocks = DataBlock::scatter(data_block, &indices, self.scatter_size)?;

        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.clone())?);
        }

        Ok(res)
    }
}

fn get_hash_values(column: &Value<AnyType>, rows: usize) -> Result<Vec<u64>> {
    match column {
        Value::Scalar(c) => match c {
            common_expression::Scalar::Null => Ok(vec![0; rows]),
            common_expression::Scalar::Number(NumberScalar::UInt64(x)) => Ok(vec![*x; rows]),
            _ => unreachable!(),
        },
        Value::Column(c) => {
            if let Some(column) = NumberType::<u64>::try_downcast_column(c) {
                Ok(column.iter().copied().collect())
            } else if let Some(column) = NullableType::<NumberType<u64>>::try_downcast_column(c) {
                let null_map = column.validity;
                Ok(column
                    .column
                    .iter()
                    .zip(null_map.iter())
                    .map(|(x, b)| if b { *x } else { 0 })
                    .collect())
            } else {
                unreachable!()
            }
        }
    }
}
