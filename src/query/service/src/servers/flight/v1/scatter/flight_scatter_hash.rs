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

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::servers::flight::v1::scatter::flight_scatter::FlightScatter;

#[derive(Clone)]
pub struct HashFlightScatter {
    func_ctx: FunctionContext,
    hash_key: Vec<Expr>,
    scatter_size: usize,
}

impl HashFlightScatter {
    pub fn try_create(
        func_ctx: FunctionContext,
        hash_keys: Vec<RemoteExpr>,
        scatter_size: usize,
    ) -> Result<Box<dyn FlightScatter>> {
        if hash_keys.len() == 1 {
            return OneHashKeyFlightScatter::try_create(func_ctx, &hash_keys[0], scatter_size);
        }
        let hash_key = hash_keys
            .iter()
            .map(|key| {
                check_function(
                    None,
                    "siphash",
                    &[],
                    &[key.as_expr(&BUILTIN_FUNCTIONS)],
                    &BUILTIN_FUNCTIONS,
                )
            })
            .collect::<Result<_>>()?;

        Ok(Box::new(Self {
            func_ctx,
            scatter_size,
            hash_key,
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
        hash_key: &RemoteExpr,
        scatter_size: usize,
    ) -> Result<Box<dyn FlightScatter>> {
        let indices_scalar = check_function(
            None,
            "modulo",
            &[],
            &[
                check_function(
                    None,
                    "siphash",
                    &[],
                    &[hash_key.as_expr(&BUILTIN_FUNCTIONS)],
                    &BUILTIN_FUNCTIONS,
                )?,
                Expr::Constant {
                    span: None,
                    scalar: Scalar::Number(NumberScalar::UInt64(scatter_size as u64)),
                    data_type: DataType::Number(NumberDataType::UInt64),
                },
            ],
            &BUILTIN_FUNCTIONS,
        )?;

        Ok(Box::new(OneHashKeyFlightScatter {
            scatter_size,
            func_ctx,
            indices_scalar,
        }))
    }
}

impl FlightScatter for OneHashKeyFlightScatter {
    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();

        let indices = evaluator.run(&self.indices_scalar).unwrap();
        let indices = get_hash_values(indices, num)?;
        let data_blocks = DataBlock::scatter(&data_block, &indices, self.scatter_size)?;

        let block_meta = data_block.get_meta();
        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.cloned())?);
        }

        Ok(res)
    }
}

impl FlightScatter for HashFlightScatter {
    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();
        let indices = if !self.hash_key.is_empty() {
            let mut hash_keys = Vec::with_capacity(self.hash_key.len());
            for expr in &self.hash_key {
                let indices = evaluator.run(expr).unwrap();
                let indices = get_hash_values(indices, num)?;
                hash_keys.push(indices)
            }
            self.combine_hash_keys(&hash_keys, num)
        } else {
            Ok(vec![0; num])
        }?;

        let block_meta = data_block.get_meta();
        let data_blocks = DataBlock::scatter(&data_block, &indices, self.scatter_size)?;

        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.cloned())?);
        }

        Ok(res)
    }
}

impl HashFlightScatter {
    pub fn combine_hash_keys(
        &self,
        hash_keys: &[Buffer<u64>],
        num_rows: usize,
    ) -> Result<Vec<u64>> {
        if self.hash_key.len() != hash_keys.len() {
            return Err(ErrorCode::Internal(
                "Hash keys and hash functions must be the same length.",
            ));
        }
        let mut hash = vec![DefaultHasher::default(); num_rows];
        for keys in hash_keys.iter() {
            for (i, value) in keys.iter().enumerate() {
                hash[i].write_u64(*value);
            }
        }

        let m = self.scatter_size as u64;
        Ok(hash.into_iter().map(|h| h.finish() % m).collect())
    }
}

fn get_hash_values(column: Value<AnyType>, rows: usize) -> Result<Buffer<u64>> {
    match column {
        Value::Scalar(c) => match c {
            databend_common_expression::Scalar::Null => Ok(vec![0; rows].into()),
            databend_common_expression::Scalar::Number(NumberScalar::UInt64(x)) => {
                Ok(vec![x; rows].into())
            }
            _ => unreachable!(),
        },
        Value::Column(c) => {
            if let Some(column) = NumberType::<u64>::try_downcast_column(&c) {
                Ok(column)
            } else if let Some(mut column) =
                NullableType::<NumberType<u64>>::try_downcast_column(&c)
            {
                let null_map = column.validity;
                if null_map.unset_bits() == 0 {
                    Ok(column.column)
                } else if null_map.unset_bits() == null_map.len() {
                    Ok(vec![0; rows].into())
                } else {
                    let mut need_new_vec = true;
                    if let Some(column) = unsafe { column.column.get_mut() } {
                        column
                            .iter_mut()
                            .zip(null_map.iter())
                            .for_each(|(x, valid)| {
                                *x *= valid as u64;
                            });
                        need_new_vec = false;
                    }

                    if !need_new_vec {
                        Ok(column.column)
                    } else {
                        Ok(column
                            .column
                            .iter()
                            .zip(null_map.iter())
                            .map(|(x, b)| if b { *x } else { 0 })
                            .collect())
                    }
                }
            } else {
                unreachable!()
            }
        }
    }
}
