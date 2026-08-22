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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionID;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::servers::flight::v1::partition::partition_stream::PartitionStream;
use crate::servers::flight::v1::partition::partition_stream::PartitionedBlock;

#[derive(Clone)]
struct HashPartitioner {
    func_ctx: FunctionContext,
    hash_key: Vec<Expr>,
    scatter_size: usize,
}

impl HashPartitioner {
    fn try_create(
        func_ctx: FunctionContext,
        hash_keys: Vec<RemoteExpr>,
        scatter_size: usize,
    ) -> Result<Self> {
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

        Ok(Self {
            func_ctx,
            scatter_size,
            hash_key,
        })
    }
}

#[derive(Clone)]
struct OneHashKeyPartitioner {
    func_ctx: FunctionContext,
    indices_scalar: Expr,
    default_scatter_index: u64,
}

impl OneHashKeyPartitioner {
    fn try_create(
        func_ctx: FunctionContext,
        hash_key: &RemoteExpr,
        scatter_size: usize,
        local_pos: usize,
    ) -> Result<Self> {
        let default_scatter_index = if shuffle_by_block_id_in_merge_into(hash_key) {
            local_pos as u64
        } else {
            0
        };
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
                Expr::constant(
                    Scalar::Number(NumberScalar::UInt64(scatter_size as u64)),
                    Some(DataType::Number(NumberDataType::UInt64)),
                ),
            ],
            &BUILTIN_FUNCTIONS,
        )?;

        Ok(OneHashKeyPartitioner {
            func_ctx,
            indices_scalar,
            default_scatter_index,
        })
    }
}

trait RowPartitioner: Send + Sync {
    fn partition_ids(&self, data_block: &DataBlock) -> Result<Vec<u64>>;
}

impl RowPartitioner for OneHashKeyPartitioner {
    fn partition_ids(&self, data_block: &DataBlock) -> Result<Vec<u64>> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();

        let indices = evaluator.run(&self.indices_scalar).unwrap();
        let indices = get_hash_values(indices, num, self.default_scatter_index)?;
        Ok(indices.to_vec())
    }
}

impl RowPartitioner for HashPartitioner {
    fn partition_ids(&self, data_block: &DataBlock) -> Result<Vec<u64>> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();
        let indices = if !self.hash_key.is_empty() {
            let mut hash_keys = Vec::with_capacity(self.hash_key.len());
            for expr in &self.hash_key {
                let indices = evaluator.run(expr).unwrap();
                let indices = get_hash_values(indices, num, 0)?;
                hash_keys.push(indices)
            }
            self.combine_hash_keys(&hash_keys, num)
        } else {
            Ok(vec![0; num])
        }?;

        Ok(indices)
    }
}

impl HashPartitioner {
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

struct HashPartitionStream {
    partitions: usize,
    partitioner: Arc<dyn RowPartitioner>,
    buffer: BlockPartitionStream,
}

impl PartitionStream for HashPartitionStream {
    fn push(&mut self, data_block: DataBlock) -> Result<Vec<PartitionedBlock>> {
        let partition_ids = self.partitioner.partition_ids(&data_block)?;
        Ok(self
            .buffer
            .partition(partition_ids, data_block, true)
            .into_iter()
            .map(|(partition_id, block)| PartitionedBlock::create(partition_id, block))
            .collect())
    }

    fn finish(&mut self) -> Result<Vec<PartitionedBlock>> {
        Ok((0..self.partitions)
            .filter_map(|partition_id| {
                self.buffer
                    .finalize_partition(partition_id)
                    .map(|block| PartitionedBlock::create(partition_id, block))
            })
            .collect())
    }
}

pub fn create_hash_partition_streams(
    func_ctx: FunctionContext,
    hash_keys: Vec<RemoteExpr>,
    partitions: usize,
    local_pos: usize,
    streams: usize,
    rows_threshold: usize,
    bytes_threshold: usize,
) -> Result<Vec<Box<dyn PartitionStream>>> {
    let partitioner: Arc<dyn RowPartitioner> = if hash_keys.len() == 1 {
        Arc::new(OneHashKeyPartitioner::try_create(
            func_ctx,
            &hash_keys[0],
            partitions,
            local_pos,
        )?)
    } else {
        Arc::new(HashPartitioner::try_create(
            func_ctx, hash_keys, partitions,
        )?)
    };

    Ok((0..streams)
        .map(|_| {
            Box::new(HashPartitionStream {
                partitions,
                partitioner: partitioner.clone(),
                buffer: BlockPartitionStream::create(rows_threshold, bytes_threshold, partitions),
            }) as Box<dyn PartitionStream>
        })
        .collect())
}

fn shuffle_by_block_id_in_merge_into(expr: &RemoteExpr) -> bool {
    if let RemoteExpr::FunctionCall {
        id: box FunctionID::Builtin { name, .. },
        args,
        ..
    } = expr
    {
        if name == "bit_and" {
            if let RemoteExpr::FunctionCall {
                id: box FunctionID::Builtin { name, .. },
                ..
            } = &args[0]
            {
                if name == "bit_shift_right" {
                    return true;
                }
            }
        }
    }
    false
}

fn get_hash_values(
    column: Value<AnyType>,
    rows: usize,
    default_scatter_index: u64,
) -> Result<Buffer<u64>> {
    match column {
        Value::Scalar(c) => match c {
            databend_common_expression::Scalar::Null => {
                Ok(vec![default_scatter_index; rows].into())
            }
            databend_common_expression::Scalar::Number(NumberScalar::UInt64(x)) => {
                Ok(vec![x; rows].into())
            }
            _ => unreachable!(),
        },
        Value::Column(c) => {
            if let Ok(column) = NumberType::<u64>::try_downcast_column(&c) {
                return Ok(column);
            }

            let mut column = NullableType::<NumberType<u64>>::try_downcast_column(&c).unwrap();
            let null_map = column.validity;
            if null_map.null_count() == 0 {
                return Ok(column.column);
            }
            if null_map.null_count() == null_map.len() {
                return Ok(vec![default_scatter_index; rows].into());
            }

            let mut need_new_vec = true;
            if let Some(column) = unsafe { column.column.get_mut() } {
                column
                    .iter_mut()
                    .zip(null_map.iter())
                    .for_each(|(x, valid)| {
                        if valid {
                            *x *= valid as u64;
                        } else {
                            *x = default_scatter_index;
                        }
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
                    .map(|(x, b)| if b { *x } else { default_scatter_index })
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::types::UInt64Type;

    use super::create_hash_partition_streams;

    fn block(values: Vec<u64>) -> DataBlock {
        DataBlock::new_from_columns(vec![UInt64Type::from_data(values)])
    }

    #[test]
    fn test_hash_partition_stream_batches_and_flushes_per_worker() {
        let mut streams =
            create_hash_partition_streams(FunctionContext::default(), vec![], 3, 0, 2, 3, 0)
                .unwrap();

        assert!(streams[0].push(block(vec![1, 2])).unwrap().is_empty());
        let ready = streams[0].push(block(vec![3, 4])).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].partition_id, 0);
        assert_eq!(ready[0].block.num_rows(), 4);
        assert!(streams[0].finish().unwrap().is_empty());

        assert!(streams[1].push(block(vec![5, 6])).unwrap().is_empty());
        let flushed = streams[1].finish().unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].partition_id, 0);
        assert_eq!(flushed[0].block.num_rows(), 2);
        assert!(streams[1].finish().unwrap().is_empty());
    }
}
