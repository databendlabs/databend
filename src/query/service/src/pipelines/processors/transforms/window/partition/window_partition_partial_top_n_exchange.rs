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
use databend_common_expression::group_hash_value_spread;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_expression::DataBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortCompareEquality;
use databend_common_pipeline_core::processors::Exchange;

use super::WindowPartitionMeta;
use crate::sql::executor::physical_plans::WindowPartitionTopNFunc;

pub struct WindowPartitionTopNExchange {
    partition_indices: Box<[usize]>,
    top: usize,
    func: WindowPartitionTopNFunc,

    sort_desc: Box<[SortColumnDescription]>,
    num_partitions: u64,
}

impl WindowPartitionTopNExchange {
    pub fn create(
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        top: usize,
        func: WindowPartitionTopNFunc,
        num_partitions: u64,
    ) -> Arc<WindowPartitionTopNExchange> {
        assert!(top > 0);
        let partition_indices = partition_indices.into_boxed_slice();
        let sort_desc = partition_indices
            .iter()
            .map(|&offset| SortColumnDescription {
                offset,
                asc: true,
                nulls_first: false,
            })
            .chain(order_by)
            .collect::<Vec<_>>()
            .into();

        Arc::new(WindowPartitionTopNExchange {
            num_partitions,
            partition_indices,
            top,
            func,
            sort_desc,
        })
    }
}

impl Exchange for WindowPartitionTopNExchange {
    const NAME: &'static str = "WindowTopN";
    fn partition(&self, block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let rows = block.num_rows();

        let mut sort_compare = SortCompareEquality::new(self.sort_desc.to_vec(), rows);

        for &offset in &self.partition_indices {
            let array = block.get_by_offset(offset).value.clone();
            sort_compare.visit_value(array)?;
            sort_compare.increment_column_index();
        }

        let partition_equality = sort_compare.equality_index().to_vec();

        for desc in self.sort_desc.iter().skip(self.partition_indices.len()) {
            let array = block.get_by_offset(desc.offset).value.clone();
            sort_compare.visit_value(array)?;
            sort_compare.increment_column_index();
        }

        let full_equality = sort_compare.equality_index().to_vec();
        let permutation = sort_compare.take_permutation();

        let hash_indices = std::iter::once(permutation[0])
            .chain(
                partition_equality
                    .iter()
                    .enumerate()
                    .filter_map(|(i, &eq)| if eq == 0 { Some(permutation[i]) } else { None }),
            )
            .collect::<Vec<_>>();

        let mut hashes = vec![0u64; rows];
        for (i, &offset) in self.partition_indices.iter().enumerate() {
            let entry = block.get_by_offset(offset);
            group_hash_value_spread(&hash_indices, entry.value.to_owned(), i == 0, &mut hashes)?;
        }

        let partition_permutation = self.partition_permutation(
            rows,
            &permutation,
            &hashes,
            &partition_equality,
            &full_equality,
        );

        // Partition the data blocks to different processors.
        let mut output_data_blocks = vec![vec![]; n];
        let mut buf = None;
        for (partition_id, indices) in partition_permutation.into_iter().enumerate() {
            output_data_blocks[partition_id % n]
                .push((partition_id, block.take(&indices, &mut buf)?));
        }

        // Union data blocks for each processor.
        Ok(output_data_blocks
            .into_iter()
            .map(WindowPartitionMeta::create)
            .map(DataBlock::empty_with_meta)
            .collect())
    }
}

impl WindowPartitionTopNExchange {
    fn partition_permutation(
        &self,
        rows: usize,
        permutation: &[u32],
        hashes: &[u64],
        partition_equality: &[u8],
        full_equality: &[u8],
    ) -> Vec<Vec<u32>> {
        let mut partition_permutation = vec![Vec::new(); self.num_partitions as usize];

        let mut start = 0;
        let mut cur = 0;
        while cur < rows {
            let partition =
                &mut partition_permutation[(hashes[start] % self.num_partitions) as usize];
            partition.push(permutation[start]);

            let mut rank = 0; // this first value is rank 0
            cur = start + 1;
            while cur < rows {
                if partition_equality[cur] == 0 {
                    start = cur;
                    break;
                }

                match self.func {
                    WindowPartitionTopNFunc::RowNumber => {
                        if cur - start < self.top {
                            partition.push(permutation[cur]);
                        }
                    }
                    WindowPartitionTopNFunc::Rank | WindowPartitionTopNFunc::DenseRank => {
                        if full_equality[cur] == 0 {
                            if matches!(self.func, WindowPartitionTopNFunc::Rank) {
                                rank = cur - start
                            } else {
                                rank += 1
                            }
                        }

                        if rank < self.top {
                            partition.push(permutation[cur]);
                        }
                    }
                }
                cur += 1;
            }
        }
        partition_permutation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_number() {
        let p = WindowPartitionTopNExchange::create(
            vec![0, 1],
            vec![SortColumnDescription {
                offset: 2,
                asc: true,
                nulls_first: false,
            }],
            3,
            WindowPartitionTopNFunc::RowNumber,
            8,
        );

        let permutation: Vec<u32> = vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let hashes: Vec<u64> = vec![5, 5, 5, 5, 5, 6, 7, 7, 7, 9, 10];
        let partition_equality = vec![1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0];
        let full_equality = vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let got = p.partition_permutation(
            permutation.len(),
            &permutation,
            &hashes,
            &partition_equality,
            &full_equality,
        );

        let want = vec![
            vec![],
            vec![1],
            vec![0],
            vec![],
            vec![],
            vec![10, 9, 8],
            vec![5],
            vec![4, 3, 2],
        ];
        assert_eq!(&want, &got)
    }

    #[test]
    fn test_rank() {
        let p = WindowPartitionTopNExchange::create(
            vec![0, 1],
            vec![SortColumnDescription {
                offset: 2,
                asc: true,
                nulls_first: false,
            }],
            3,
            WindowPartitionTopNFunc::Rank,
            8,
        );

        let permutation: Vec<u32> = vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let hashes: Vec<u64> = vec![5, 5, 5, 5, 5, 6, 7, 7, 7, 9, 10];
        let partition_equality = vec![1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0];
        let full_equality = vec![1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0];
        let got = p.partition_permutation(
            permutation.len(),
            &permutation,
            &hashes,
            &partition_equality,
            &full_equality,
        );

        let want = vec![
            vec![],
            vec![1],
            vec![0],
            vec![],
            vec![],
            vec![10, 9, 8, 7],
            vec![5],
            vec![4, 3, 2],
        ];
        assert_eq!(&want, &got)
    }

    #[test]
    fn test_dense_rank() {
        let p = WindowPartitionTopNExchange::create(
            vec![0, 1],
            vec![SortColumnDescription {
                offset: 2,
                asc: true,
                nulls_first: false,
            }],
            3,
            WindowPartitionTopNFunc::DenseRank,
            8,
        );

        let permutation: Vec<u32> = vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let hashes: Vec<u64> = vec![5, 5, 5, 5, 5, 6, 7, 7, 7, 9, 10];
        let partition_equality = vec![1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0];
        let full_equality = vec![1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0];
        let got = p.partition_permutation(
            permutation.len(),
            &permutation,
            &hashes,
            &partition_equality,
            &full_equality,
        );

        let want = vec![
            vec![],
            vec![1],
            vec![0],
            vec![],
            vec![],
            vec![10, 9, 8, 7, 6],
            vec![5],
            vec![4, 3, 2],
        ];
        assert_eq!(&want, &got)
    }
}
