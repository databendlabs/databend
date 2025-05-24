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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Exchange;

use super::SortScatteredMeta;

pub struct SortRangeExchange;

impl Exchange for SortRangeExchange {
    const NAME: &'static str = "SortRange";
    fn partition(&self, mut data: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let scattered = data
            .take_meta()
            .and_then(SortScatteredMeta::downcast_from)
            .expect("require a SortScatteredMeta")
            .0;
        assert!(scattered.len() <= n);

        let blocks = scattered
            .into_iter()
            .map(|meta| {
                meta.map(|meta| DataBlock::empty_with_meta(Box::new(meta)))
                    .unwrap_or_else(DataBlock::empty)
            })
            .collect();

        Ok(blocks)
    }
}
