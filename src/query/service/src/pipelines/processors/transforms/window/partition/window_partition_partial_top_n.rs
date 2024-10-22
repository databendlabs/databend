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
use databend_common_expression::DataBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::processors::Transform;

pub struct TransformWindowPartialTopN {
    partition_indices: Box<[usize]>,
    limit: usize,

    sort_desc: Box<[SortColumnDescription]>,
    indices: Vec<u32>,
}

impl TransformWindowPartialTopN {
    pub fn try_new(
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        limit: usize,
    ) -> Result<Self> {
        assert!(limit > 0);
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

        Ok(Self {
            partition_indices,
            limit,
            sort_desc,
            indices: Vec::new(),
        })
    }
}

impl Transform for TransformWindowPartialTopN {
    const NAME: &'static str = "Window Partial Top N";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let block = DataBlock::sort(&block, &self.sort_desc, None)?;

        let columns = self
            .partition_indices
            .iter()
            .filter_map(|i| block.get_by_offset(*i).value.as_column())
            .collect::<Vec<_>>();

        if columns.is_empty() {
            return Ok(block.slice(0..self.limit.min(block.num_rows())));
        }

        let mut start = 0;
        let mut cur = 0;
        self.indices.clear();

        while cur < block.num_rows() {
            self.indices.push(start as u32);
            let start_values = columns
                .iter()
                .map(|col| col.index(start).unwrap())
                .collect::<Vec<_>>();

            cur = start + 1;
            while cur < block.num_rows() {
                if !columns
                    .iter()
                    .zip(start_values.iter())
                    .all(|(col, value)| col.index(cur).unwrap() == *value)
                {
                    start = cur;
                    break;
                }

                if cur - start < self.limit {
                    self.indices.push(cur as u32)
                }
                cur += 1;
            }
        }

        let mut buf = None;
        block.take(&self.indices, &mut buf)
    }
}
