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

use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Exchange;
use databend_common_pipeline_transforms::processors::sort::Rows;

use super::wait::SortSampleState;

pub struct SortRangeExchange<R: Rows> {
    state: Arc<SortSampleState>,
    _r: PhantomData<R>,
}

unsafe impl<R: Rows> Send for SortRangeExchange<R> {}

unsafe impl<R: Rows> Sync for SortRangeExchange<R> {}

impl<R: Rows + 'static> Exchange for SortRangeExchange<R> {
    const NAME: &'static str = "SortRange";
    fn partition(&self, data: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let bounds = self.state.bounds();
        // debug_assert_eq!(n, self.state.partitions());
        debug_assert!(bounds.len() < n);

        if bounds.is_empty() {
            return Ok(vec![data]);
        }

        todo!()

        // let bounds = R::from_column(&bounds.0)?;
        // let rows = R::from_column(data.get_last_column())?;

        // let mut i = 0;
        // let mut j = 0;
        // let mut bound = bounds.row(j);
        // let mut indices = Vec::new();
        // while i < rows.len() {
        //     match rows.row(i).cmp(&bound) {
        //         Ordering::Less => indices.push(j as u32),
        //         Ordering::Greater if j + 1 < bounds.len() => {
        //             j += 1;
        //             bound = bounds.row(j);
        //             continue;
        //         }
        //         _ => indices.push(j as u32 + 1),
        //     }
        //     i += 1;
        // }

        // DataBlock::scatter(&data, &indices, n)
    }
}
