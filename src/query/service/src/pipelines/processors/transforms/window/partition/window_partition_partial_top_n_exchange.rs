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
        let selector = WindowTopNSelector {
            partition_indices: &self.partition_indices,
            sort_desc: &self.sort_desc,
            top: self.top,
            func: self.func,
        };
        let selection = selector.select(block);

        // Hash the first row of every surviving partition; all rows of that
        // partition share the key, so they share the bucket.
        let hash_indices = selection
            .partition_starts
            .iter()
            .map(|&start| selection.rows[start as usize])
            .collect::<Vec<_>>();

        let mut hashes = vec![0u64; block.num_rows()];
        for (i, &offset) in self.partition_indices.iter().enumerate() {
            let entry = block.get_by_offset(offset);
            group_hash_value_spread(&hash_indices, entry.value(), i == 0, &mut hashes).unwrap();
        }

        let mut partition_permutation = vec![Vec::new(); self.num_partitions as usize];
        for (segment, &start) in selection.partition_starts.iter().enumerate() {
            let end = selection
                .partition_starts
                .get(segment + 1)
                .map_or(selection.rows.len(), |&end| end as usize);
            let rows = &selection.rows[start as usize..end];
            let bucket = (hashes[rows[0] as usize] % self.num_partitions) as usize;
            partition_permutation[bucket].extend_from_slice(rows);
        }
        partition_permutation
    }
}

/// Rows of a block that can still rank within the top `top` of their
/// partition, in (partition keys, order keys) order.
pub struct WindowTopNSelection {
    /// Kept row indices, sorted by partition keys then order keys.
    pub rows: Vec<u32>,
    /// Offsets into `rows` where each partition starts.
    pub partition_starts: Vec<u32>,
}

/// Selects, per partition, the rows a ranking function could still assign a
/// rank `<= top`. For `row_number` that is the first `top` rows; for `rank`
/// and `dense_rank` it includes every row tied at the boundary.
pub struct WindowTopNSelector<'a> {
    pub partition_indices: &'a [usize],
    /// Partition columns first (ascending), then the window ORDER BY.
    pub sort_desc: &'a [SortColumnDescription],
    pub top: usize,
    pub func: WindowPartitionTopNFunc,
}

impl WindowTopNSelector<'_> {
    pub fn select(&self, block: &DataBlock) -> WindowTopNSelection {
        let rows = block.num_rows();
        let mut selection = WindowTopNSelection {
            rows: Vec::new(),
            partition_starts: Vec::new(),
        };
        if rows == 0 {
            return selection;
        }

        let mut sort_compare = SortCompare::with_force_equality(self.sort_desc.to_vec(), rows);

        for &offset in self.partition_indices {
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

        let mut start = 0;
        let mut cur = 0;
        while cur < rows {
            selection.partition_starts.push(selection.rows.len() as u32);
            selection.rows.push(permutation[start]);

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
                            selection.rows.push(permutation[cur]);
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
                            selection.rows.push(permutation[cur]);
                        }
                    }
                }
                cur += 1;
            }
        }
        selection
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
