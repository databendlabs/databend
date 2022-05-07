// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;
use std::convert::TryInto;
use std::task::Context;
use std::task::Poll;

use common_arrow::arrow;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodSerializer;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct LimitByStream {
    input: SendableDataBlockStream,
    limit: usize,
    limit_by_columns_name: Vec<String>,
    keys_count: HashMap<Vec<u8>, usize>,
}

impl LimitByStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        limit: usize,
        limit_by_columns_name: Vec<String>,
    ) -> Result<Self> {
        Ok(LimitByStream {
            input,
            limit,
            limit_by_columns_name,
            keys_count: HashMap::new(),
        })
    }

    pub fn limit_by(&mut self, block: &DataBlock) -> Result<Option<DataBlock>> {
        let mut filter = MutableBitmap::from_len_zeroed(block.num_rows());
        let method = HashMethodSerializer::default();
        let group_indices = method.group_by_get_indices(block, &self.limit_by_columns_name)?;

        for (limit_by_key, (rows, _)) in group_indices {
            let count = self.keys_count.entry(limit_by_key.to_vec()).or_default();
            // limit reached, no need to check rows
            if *count >= self.limit {
                continue;
            }

            // limit not reached yet, still have room for rows, keep filling with row index
            let remaining = self.limit - *count;
            for row in rows.iter().take(std::cmp::min(remaining, rows.len())) {
                filter.set(*row as usize, *count <= self.limit);
                *count += 1;
            }
        }

        let array = BooleanArray::from_data(ArrowType::Boolean, filter.into(), None);
        let chunk = block.clone().try_into()?;
        let chunk = arrow::compute::filter::filter_chunk(&chunk, &array)?;
        Some(DataBlock::from_chunk(block.schema(), &chunk)).transpose()
    }
}

impl Stream for LimitByStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(ref v)) => self.limit_by(v).transpose(),
            other => other,
        })
    }
}
