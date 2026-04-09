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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_catalog::plan::split_row_id;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline::basic::Exchange;

/// Partitions data blocks by block_id extracted from the `_row_id` column.
///
/// This ensures that rows belonging to the same physical block are routed
/// to the same downstream processor, eliminating duplicate block reads
/// in the RowFetch stage of MERGE INTO.
pub struct BlockIdPartitionExchange {
    row_id_col_offset: usize,
    /// Round-robin counter for NULL row_ids (unmatched rows in MixedMatched).
    null_counter: AtomicU64,
}

impl BlockIdPartitionExchange {
    pub fn create(row_id_col_offset: usize) -> Self {
        Self {
            row_id_col_offset,
            null_counter: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn partition_index(row_id: u64, n: usize) -> u16 {
        let (prefix, _) = split_row_id(row_id);
        (prefix % n as u64) as u16
    }
}

impl Exchange for BlockIdPartitionExchange {
    const NAME: &'static str = "BlockIdPartition";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let num_rows = data_block.num_rows();
        let entry = &data_block.columns()[self.row_id_col_offset];
        let mut indices: Vec<u16> = Vec::with_capacity(num_rows);

        match entry.data_type() {
            DataType::Number(NumberDataType::UInt64) => {
                let col = entry
                    .to_column()
                    .into_number()
                    .unwrap()
                    .into_u_int64()
                    .unwrap();
                for row_id in col.iter() {
                    indices.push(Self::partition_index(*row_id, n));
                }
            }
            DataType::Nullable(inner)
                if matches!(inner.as_ref(), DataType::Number(NumberDataType::UInt64)) =>
            {
                let col = entry.to_column();
                let nullable = col.into_nullable().unwrap();
                let row_ids = nullable
                    .column
                    .into_number()
                    .unwrap()
                    .into_u_int64()
                    .unwrap();
                for (row_id, is_valid) in row_ids.iter().zip(nullable.validity.iter()) {
                    if is_valid {
                        indices.push(Self::partition_index(*row_id, n));
                    } else {
                        let counter = self.null_counter.fetch_add(1, Ordering::Relaxed);
                        indices.push((counter % n as u64) as u16);
                    }
                }
            }
            _ => unreachable!(
                "Row id column should be UInt64 or Nullable(UInt64) for block_id partition"
            ),
        }

        DataBlock::scatter(&data_block, &indices, n)
    }
}
