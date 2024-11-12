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
use databend_common_expression::group_hash_columns_slice;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Exchange;

pub struct SortRangeExchange {
    num_partitions: usize,
    init: bool,
}

impl Exchange for SortRangeExchange {
    fn partition(&self, mut block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        match block.take_meta() {
            Some(_) => {}
            None => {}
        };
        todo!()
    }
}
