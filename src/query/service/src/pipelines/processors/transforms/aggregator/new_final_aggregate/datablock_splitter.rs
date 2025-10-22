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

use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

/// Split partitioned metadata evenly into DataBlock chunks.
pub fn split_partitioned_meta_into_datablocks(
    bucket: isize,
    data: Vec<AggregateMeta>,
    outputs_len: usize,
) -> Vec<DataBlock> {
    if outputs_len == 0 {
        return vec![];
    }

    let total_len = data.len();
    let base_chunk_size = total_len / outputs_len;
    let remainder = total_len % outputs_len;

    let mut result = Vec::with_capacity(outputs_len);
    let mut data_iter = data.into_iter();

    for index in 0..outputs_len {
        let chunk_size = if index < remainder {
            base_chunk_size + 1
        } else {
            base_chunk_size
        };

        let chunk: Vec<AggregateMeta> = data_iter.by_ref().take(chunk_size).collect();
        result.push(DataBlock::empty_with_meta(
            AggregateMeta::create_partitioned(bucket, chunk),
        ));
    }

    result
}
