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
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortCompare;
use databend_common_expression::group_hash_value_spread;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_pipeline::basic::Exchange;

use super::WindowPartitionMeta;
use crate::physical_plans::WindowPartitionTopNFunc;

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
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn partition(&self, block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let partition_permutation = self.partition_permutation(&block);

        // Partition the data blocks to different processors.
        let mut output_data_blocks = vec![vec![]; n];
        for (partition_id, indices) in partition_permutation.into_iter().enumerate() {
            output_data_blocks[partition_id % n]
                .push((partition_id, block.take(indices.as_slice())?));
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
    fn partition_permutation(&self, block: &DataBlock) -> Vec<Vec<u32>> {
        let rows = block.num_rows();
        let mut sort_compare = SortCompare::with_force_equality(self.sort_desc.to_vec(), rows);

        for &offset in &self.partition_indices {
            let array = block.get_by_offset(offset).value();
            sort_compare.visit_value(array).unwrap();
            sort_compare.increment_column_index();
        }

        let partition_equality = sort_compare.equality_index().to_vec();

        for desc in self.sort_desc.iter().skip(self.partition_indices.len()) {
            let array = block.get_by_offset(desc.offset).value();
            sort_compare.visit_value(array).unwrap();
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
            group_hash_value_spread(&hash_indices, entry.value(), i == 0, &mut hashes).unwrap();
        }

        let mut partition_permutation = vec![Vec::new(); self.num_partitions as usize];

        let mut start = 0;
        let mut cur = 0;
        while cur < rows {
            let partition = &mut partition_permutation
                [(hashes[permutation[start] as usize] % self.num_partitions) as usize];
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
    use databend_common_expression::BlockEntry;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;

    use super::*;

    #[test]
    fn test_row_number() -> Result<()> {
        let p = WindowPartitionTopNExchange::create(
            vec![1, 2],
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }],
            3,
            WindowPartitionTopNFunc::RowNumber,
            8,
        );

        let data = DataBlock::new(
            vec![
                Int32Type::from_data(vec![3, 1, 2, 2, 4, 3, 7, 0, 3]).into(),
                BlockEntry::new_const_column_arg::<StringType>("a".to_string(), 9),
                Int32Type::from_data(vec![3, 1, 3, 2, 2, 3, 4, 3, 3]).into(),
                StringType::from_data(vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"]).into(),
            ],
            9,
        );
        data.check_valid()?;

        let got = p.partition_permutation(&data);

        let want = vec![
            vec![],
            vec![1],
            vec![],
            vec![3, 4],
            vec![],
            vec![6],
            vec![],
            vec![7, 2, 0],
        ];
        // if got != want {
        //     let got = got
        //         .iter()
        //         .map(|indices| data.take(indices, &mut None).unwrap())
        //         .collect::<Vec<_>>();
        //     for x in got {
        //         println!("{}", x)
        //     }
        // }
        assert_eq!(&want, &got);

        Ok(())
    }

    #[test]
    fn test_rank() -> Result<()> {
        let p = WindowPartitionTopNExchange::create(
            vec![1],
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }],
            3,
            WindowPartitionTopNFunc::Rank,
            8,
        );

        let data = DataBlock::new(
            vec![
                Int32Type::from_data(vec![7, 7, 7, 6, 5, 5, 4, 1, 3, 1, 1]).into(),
                Int32Type::from_data(vec![7, 6, 5, 5, 5, 4, 3, 3, 2, 3, 3]).into(),
            ],
            11,
        );
        data.check_valid()?;

        let got = p.partition_permutation(&data);

        let want = vec![
            vec![],
            vec![1],
            vec![8, 0],
            vec![],
            vec![5, 4, 3, 2],
            vec![],
            vec![7, 9, 10],
            vec![],
        ];
        assert_eq!(&want, &got);
        Ok(())
    }

    #[test]
    fn test_dense_rank() -> Result<()> {
        let p = WindowPartitionTopNExchange::create(
            vec![1],
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }],
            3,
            WindowPartitionTopNFunc::DenseRank,
            8,
        );

        let data = DataBlock::new(
            vec![
                Int32Type::from_data(vec![5, 2, 3, 3, 2, 2, 1, 1, 1, 1, 1]).into(),
                Int32Type::from_data(vec![2, 2, 4, 3, 2, 2, 5, 4, 3, 3, 3]).into(),
            ],
            11,
        );
        data.check_valid()?;

        let got = p.partition_permutation(&data);

        let want = vec![
            vec![],
            vec![],
            vec![1, 4, 5, 0],
            vec![],
            vec![7, 2, 6],
            vec![],
            vec![8, 9, 10, 3],
            vec![],
        ];
        assert_eq!(&want, &got);
        Ok(())
    }
}
