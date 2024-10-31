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

use databend_common_exception::Result;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_expression::DataBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortCompareEquality;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_sql::executor::physical_plans::WindowPartitionTopNFunc;

pub struct TransformWindowPartialTopN {
    partition_indices: Box<[usize]>,
    top: usize,
    func: WindowPartitionTopNFunc,

    sort_desc: Box<[SortColumnDescription]>,
    indices: Vec<u32>,
}

impl TransformWindowPartialTopN {
    pub fn new(
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        top: usize,
        func: WindowPartitionTopNFunc,
    ) -> Self {
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

        Self {
            partition_indices,
            top,
            sort_desc,
            indices: Vec::new(),
            func,
        }
    }
}

impl Transform for TransformWindowPartialTopN {
    const NAME: &'static str = "Window Partial Top N";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        self.indices.clear();
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

        let mut start = 0;
        let mut cur = 0;

        while cur < rows {
            self.indices.push(permutation[start]);

            let mut rank = 1; // 0 start
            cur = start + 1;
            while cur < rows {
                if partition_equality[cur] == 0 {
                    start = cur;
                    break;
                }

                match self.func {
                    WindowPartitionTopNFunc::RowNumber => {
                        if cur - start < self.top {
                            self.indices.push(permutation[cur])
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
                            self.indices.push(permutation[cur]);
                        }
                    }
                }

                cur += 1;
            }
        }

        block.take(&self.indices, &mut None)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::Value;

    use super::*;

    #[test]
    fn test_row_number() -> Result<()> {
        let partition_indices = vec![1, 2];
        let order_by = vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: false,
        }];

        let data = DataBlock::new(
            vec![
                BlockEntry::new(
                    Int32Type::data_type(),
                    Value::Column(Int32Type::from_data(vec![3, 1, 2, 2, 4, 3, 7, 0, 3])),
                ),
                BlockEntry::new(
                    StringType::data_type(),
                    Value::Scalar(Scalar::String("a".to_string())),
                ),
                BlockEntry::new(
                    Int32Type::data_type(),
                    Value::Column(Int32Type::from_data(vec![3, 1, 3, 2, 2, 3, 4, 3, 3])),
                ),
                BlockEntry::new(
                    StringType::data_type(),
                    Value::Column(StringType::from_data(vec![
                        "a", "b", "c", "d", "e", "f", "g", "h", "i",
                    ])),
                ),
            ],
            9,
        );
        data.check_valid()?;

        {
            let mut transform = TransformWindowPartialTopN::new(
                partition_indices.clone(),
                order_by.clone(),
                3,
                WindowPartitionTopNFunc::RowNumber,
            );

            let got = transform.transform(data.clone())?;
            let want = DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 2, 4, 0, 2, 3, 7]),
                StringType::from_data(vec!["a", "a", "a", "a", "a", "a", "a"]),
                Int32Type::from_data(vec![1, 2, 2, 3, 3, 3, 4]),
                StringType::from_data(vec!["b", "d", "e", "h", "c", "a", "g"]),
            ]);
            assert_eq!(want.to_string(), got.to_string());
        }

        {
            let mut transform = TransformWindowPartialTopN::new(
                partition_indices,
                order_by,
                1,
                WindowPartitionTopNFunc::RowNumber,
            );

            let got = transform.transform(data.clone())?;
            let want = DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 2, 0, 7]),
                StringType::from_data(vec!["a", "a", "a", "a"]),
                Int32Type::from_data(vec![1, 2, 3, 4]),
                StringType::from_data(vec!["b", "d", "h", "g"]),
            ]);
            assert_eq!(want.to_string(), got.to_string());

            let want = got;
            let got = transform.transform(want.clone())?;
            assert_eq!(want.to_string(), got.to_string());
        }

        Ok(())
    }

    #[test]
    fn test_rank() -> Result<()> {
        let partition_indices = vec![1];
        let order_by = vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: false,
        }];

        let data = DataBlock::new(
            vec![
                BlockEntry::new(
                    Int32Type::data_type(),
                    Value::Column(Int32Type::from_data(vec![1, 1, 1, 2, 2, 4, 0, 3, 3])),
                ),
                BlockEntry::new(
                    Int32Type::data_type(),
                    Value::Column(Int32Type::from_data(vec![1, 1, 1, 2, 2, 2, 3, 3, 3])),
                ),
            ],
            9,
        );
        data.check_valid()?;

        {
            let mut transform = TransformWindowPartialTopN::new(
                partition_indices.clone(),
                order_by.clone(),
                2,
                WindowPartitionTopNFunc::Rank,
            );

            let got = transform.transform(data.clone())?;
            let want = DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 0, 3, 3]),
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ]);
            assert_eq!(want.to_string(), got.to_string());
        }

        Ok(())
    }
}
