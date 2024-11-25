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

use std::cmp::Ordering;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Exchange;

use super::sort_simple::SortSimpleState;

pub struct SortRangeExchange {
    sort_desc: Arc<Vec<SortColumnDescription>>,
    state: Arc<SortSimpleState>,
}

impl Exchange for SortRangeExchange {
    const NAME: &'static str = "SortRange";
    fn partition(&self, data: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let bounds = self.state.bounds().unwrap();
        debug_assert_eq!(n, self.state.partitions());
        debug_assert!(bounds.num_rows() < n);

        let max_bound = bounds.num_rows();
        let mut indices = vec![max_bound as u32; data.num_rows()];
        for bound_idx in 0..bounds.num_rows() {
            let mut finish = true;
            for (row, partition) in indices.iter_mut().enumerate() {
                if *partition < max_bound as u32 {
                    continue;
                }
                if self.cmp(&data, row, &bounds, bound_idx) == Ordering::Less {
                    *partition = bound_idx as u32
                }
                finish = false
            }
            if finish {
                break;
            }
        }

        DataBlock::scatter(&data, &indices, n)
    }
}

impl SortRangeExchange {
    pub fn new(
        sort_desc: Arc<Vec<SortColumnDescription>>,
        state: Arc<SortSimpleState>,
    ) -> Arc<Self> {
        Arc::new(Self { sort_desc, state })
    }

    fn cmp(&self, block: &DataBlock, row: usize, bounds: &DataBlock, bound_idx: usize) -> Ordering {
        use databend_common_expression::Value;
        for (i, desc) in self.sort_desc.iter().enumerate() {
            let data = match &block.get_by_offset(desc.offset).value {
                Value::Scalar(scalar) => scalar.as_ref(),
                Value::Column(column) => column.index(row).unwrap(),
            };
            let bound = bounds
                .get_by_offset(i)
                .value
                .as_column()
                .unwrap()
                .index(bound_idx)
                .unwrap();

            let o = data.cmp(&bound);
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    }
}
