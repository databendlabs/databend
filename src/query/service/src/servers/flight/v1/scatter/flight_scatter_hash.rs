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
use databend_common_sql::plans::SkewHashInfo;
use databend_common_sql::plans::SkewHashRole;

use crate::servers::flight::v1::scatter::flight_scatter::FlightScatter;
use crate::servers::flight::v1::scatter::flight_scatter::FlightScatterState;

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
        local_pos: usize,
    ) -> Result<Box<dyn FlightScatter>> {
        if hash_keys.len() == 1 {
            return OneHashKeyFlightScatter::try_create(
                func_ctx,
                &hash_keys[0],
                scatter_size,
                local_pos,
            );
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
    default_scatter_index: u64,
}

#[derive(Clone)]
pub struct SkewHashFlightScatter {
    func_ctx: FunctionContext,
    key_expr: Expr,
    hash_expr: Expr,
    scatter_size: usize,
    node_partitions: Vec<(usize, usize)>,
    skew_info: SkewHashInfo,
}

impl SkewHashFlightScatter {
    pub fn try_create(
        func_ctx: FunctionContext,
        hash_keys: Vec<RemoteExpr>,
        scatter_size: usize,
        destination_channels: &[(String, Vec<String>)],
        skew_info: SkewHashInfo,
    ) -> Result<Box<dyn FlightScatter>> {
        if hash_keys.len() != 1 {
            return Err(ErrorCode::Internal(
                "Skew hash shuffle only supports one hash key.",
            ));
        }
        let mut partition_start = 0;
        let mut node_partitions = Vec::with_capacity(destination_channels.len());
        for (destination, channels) in destination_channels {
            if channels.is_empty() {
                return Err(ErrorCode::Internal(format!(
                    "Skew hash shuffle destination {destination} has no channels."
                )));
            }
            node_partitions.push((partition_start, channels.len()));
            partition_start += channels.len();
        }
        if partition_start != scatter_size {
            return Err(ErrorCode::Internal(format!(
                "Skew hash shuffle channels mismatch: scatter_size={}, destination_channels={}.",
                scatter_size, partition_start
            )));
        }
        if skew_info.bucket_count > node_partitions.len() {
            return Err(ErrorCode::Internal(format!(
                "Skew hash bucket count {} exceeds destination node count {}.",
                skew_info.bucket_count,
                node_partitions.len()
            )));
        }
        debug_assert!(
            skew_info.hot_keys.windows(2).all(|pair| pair[0] <= pair[1]),
            "Skew hash hot keys must be sorted for binary search."
        );

        let key_expr = hash_keys[0].as_expr(&BUILTIN_FUNCTIONS);
        let hash_expr = check_function(
            None,
            "siphash",
            &[],
            &[key_expr.clone()],
            &BUILTIN_FUNCTIONS,
        )?;

        Ok(Box::new(Self {
            func_ctx,
            key_expr,
            hash_expr,
            scatter_size,
            node_partitions,
            skew_info,
        }))
    }

    fn normal_partition(&self, hash: u64) -> u64 {
        hash % self.scatter_size as u64
    }

    fn skew_partition(&self, hash: u64, salt: usize) -> u64 {
        // Skew buckets are node-level buckets. Hash join currently shares one
        // build table per node, so a hot build row must not be duplicated into
        // multiple local worker partitions on the same node.
        let node_count = self.node_partitions.len();
        let node_index = ((hash % node_count as u64) as usize + salt) % node_count;
        let (partition_start, partition_count) = self.node_partitions[node_index];
        let local_partition = (hash % partition_count as u64) as usize;
        (partition_start + local_partition) as u64
    }

    fn is_hot_key(&self, value: &Value<AnyType>, row: usize) -> bool {
        match value {
            Value::Scalar(scalar) => {
                !matches!(scalar, Scalar::Null)
                    && self.skew_info.hot_keys.binary_search(scalar).is_ok()
            }
            Value::Column(column) => {
                let scalar = unsafe { column.index_unchecked(row).to_owned() };
                !matches!(scalar, Scalar::Null)
                    && self.skew_info.hot_keys.binary_search(&scalar).is_ok()
            }
        }
    }

    fn probe_indices(
        &self,
        key_values: &Value<AnyType>,
        hashes: &Buffer<u64>,
        rows: usize,
        state: &mut FlightScatterState,
    ) -> Vec<u64> {
        (0..rows)
            .map(|row| {
                let hash = hashes[row];
                if self.is_hot_key(key_values, row) {
                    let salt = state.next_skew_probe_salt(self.skew_info.bucket_count);
                    self.skew_partition(hash, salt)
                } else {
                    self.normal_partition(hash)
                }
            })
            .collect()
    }

    fn build_blocks(
        &self,
        data_block: &DataBlock,
        key_values: &Value<AnyType>,
        hashes: &Buffer<u64>,
        rows: usize,
    ) -> Result<Vec<DataBlock>> {
        let mut partition_rows = vec![Vec::<u32>::new(); self.scatter_size];
        for row in 0..rows {
            let hash = hashes[row];
            if self.is_hot_key(key_values, row) {
                for salt in 0..self.skew_info.bucket_count {
                    let target = self.skew_partition(hash, salt) as usize;
                    partition_rows[target].push(row as u32);
                }
            } else {
                let target = self.normal_partition(hash) as usize;
                partition_rows[target].push(row as u32);
            }
        }

        let mut data_blocks = Vec::with_capacity(self.scatter_size);
        for rows in partition_rows {
            data_blocks.push(data_block.take_with_optimize_size(rows.as_slice())?);
        }
        Ok(data_blocks)
    }
}

impl OneHashKeyFlightScatter {
    pub fn try_create(
        func_ctx: FunctionContext,
        hash_key: &RemoteExpr,
        scatter_size: usize,
        local_pos: usize,
    ) -> Result<Box<dyn FlightScatter>> {
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

        Ok(Box::new(OneHashKeyFlightScatter {
            scatter_size,
            func_ctx,
            indices_scalar,
            default_scatter_index,
        }))
    }

    fn partition_indices(&self, data_block: &DataBlock) -> Result<Vec<u64>> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();
        let indices = evaluator.run(&self.indices_scalar).unwrap();
        let indices = get_hash_values(indices, num, self.default_scatter_index)?;
        Ok(indices.to_vec())
    }
}

impl FlightScatter for OneHashKeyFlightScatter {
    fn name(&self) -> &'static str {
        "OneHashKey"
    }

    fn execute(
        &self,
        data_block: DataBlock,
        _state: &mut FlightScatterState,
    ) -> Result<Vec<DataBlock>> {
        let indices = self.partition_indices(&data_block)?;
        let data_blocks = DataBlock::scatter(&data_block, &indices, self.scatter_size)?;

        let block_meta = data_block.get_meta();
        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.cloned())?);
        }

        Ok(res)
    }

    fn scatter_block(
        &self,
        data_block: DataBlock,
        partition_stream: &mut BlockPartitionStream,
        _state: &mut FlightScatterState,
    ) -> Result<Vec<(usize, DataBlock)>> {
        let indices = self.partition_indices(&data_block)?;
        Ok(partition_stream.partition(indices, data_block, true))
    }
}

impl FlightScatter for HashFlightScatter {
    fn name(&self) -> &'static str {
        "Hash"
    }

    fn execute(
        &self,
        data_block: DataBlock,
        _state: &mut FlightScatterState,
    ) -> Result<Vec<DataBlock>> {
        let indices = self.partition_indices(&data_block)?;
        let block_meta = data_block.get_meta();
        let data_blocks = DataBlock::scatter(&data_block, &indices, self.scatter_size)?;

        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.cloned())?);
        }

        Ok(res)
    }

    fn scatter_block(
        &self,
        data_block: DataBlock,
        partition_stream: &mut BlockPartitionStream,
        _state: &mut FlightScatterState,
    ) -> Result<Vec<(usize, DataBlock)>> {
        let indices = self.partition_indices(&data_block)?;
        Ok(partition_stream.partition(indices, data_block, true))
    }
}

impl FlightScatter for SkewHashFlightScatter {
    fn name(&self) -> &'static str {
        "SkewHash"
    }

    fn execute(
        &self,
        data_block: DataBlock,
        state: &mut FlightScatterState,
    ) -> Result<Vec<DataBlock>> {
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let rows = data_block.num_rows();
        let key_values = evaluator.run(&self.key_expr)?;
        let hashes = evaluator.run(&self.hash_expr)?;
        let hashes = get_hash_values(hashes, rows, 0)?;

        match self.skew_info.role {
            SkewHashRole::Probe => {
                let indices = self.probe_indices(&key_values, &hashes, rows, state);
                DataBlock::scatter(&data_block, &indices, self.scatter_size)
            }
            SkewHashRole::Build => self.build_blocks(&data_block, &key_values, &hashes, rows),
        }
    }

    fn scatter_block(
        &self,
        data_block: DataBlock,
        partition_stream: &mut BlockPartitionStream,
        state: &mut FlightScatterState,
    ) -> Result<Vec<(usize, DataBlock)>> {
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let rows = data_block.num_rows();
        let key_values = evaluator.run(&self.key_expr)?;
        let hashes = evaluator.run(&self.hash_expr)?;
        let hashes = get_hash_values(hashes, rows, 0)?;

        match self.skew_info.role {
            SkewHashRole::Probe => {
                let indices = self.probe_indices(&key_values, &hashes, rows, state);
                Ok(partition_stream.partition(indices, data_block, true))
            }
            SkewHashRole::Build => Ok(self
                .build_blocks(&data_block, &key_values, &hashes, rows)?
                .into_iter()
                .enumerate()
                .collect()),
        }
    }
}

impl HashFlightScatter {
    fn partition_indices(&self, data_block: &DataBlock) -> Result<Vec<u64>> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();
        if !self.hash_key.is_empty() {
            let mut hash_keys = Vec::with_capacity(self.hash_key.len());
            for expr in &self.hash_key {
                let indices = evaluator.run(expr).unwrap();
                let indices = get_hash_values(indices, num, 0)?;
                hash_keys.push(indices)
            }
            self.combine_hash_keys(&hash_keys, num)
        } else {
            Ok(vec![0; num])
        }
    }

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
    use databend_common_expression::FromData;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    fn uint64_scalar(value: u64) -> Scalar {
        Scalar::Number(NumberScalar::UInt64(value))
    }

    fn constant_uint64_expr(value: u64) -> Expr {
        Expr::constant(
            uint64_scalar(value),
            Some(DataType::Number(NumberDataType::UInt64)),
        )
    }

    fn one_row_block(value: u64) -> DataBlock {
        DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![value])])
    }

    fn skew_probe_scatter() -> SkewHashFlightScatter {
        SkewHashFlightScatter {
            func_ctx: FunctionContext::default(),
            key_expr: constant_uint64_expr(1),
            hash_expr: constant_uint64_expr(0),
            scatter_size: 4,
            node_partitions: vec![(0, 1), (1, 1), (2, 1), (3, 1)],
            skew_info: SkewHashInfo {
                role: SkewHashRole::Probe,
                hot_keys: vec![uint64_scalar(1)],
                bucket_count: 4,
                normal_skew_penalty_rows: 0,
                skew_skew_penalty_rows: 0,
                extra_build_rows: 0,
            },
        }
    }

    #[test]
    fn test_skew_hash_probe_salt_rotates_across_scatter_blocks() -> Result<()> {
        let scatter = skew_probe_scatter();
        let mut state = FlightScatterState::default();
        let mut partition_stream = BlockPartitionStream::create(1, 0, 4);
        let mut partition_ids = Vec::new();

        for value in 0..4 {
            let ready_blocks =
                scatter.scatter_block(one_row_block(value), &mut partition_stream, &mut state)?;

            assert_eq!(ready_blocks.len(), 1);
            assert_eq!(ready_blocks[0].1.num_rows(), 1);
            partition_ids.push(ready_blocks[0].0);
        }

        assert_eq!(partition_ids, vec![0, 1, 2, 3]);
        Ok(())
    }
}
