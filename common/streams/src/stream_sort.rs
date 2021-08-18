// Copyright 2020 Datafuse Labs.
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

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct SortStream {
    input: SendableDataBlockStream,
    sort_columns_descriptions: Vec<SortColumnDescription>,
    limit: Option<usize>,
}

impl SortStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        sort_columns_descriptions: Vec<SortColumnDescription>,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(SortStream {
            input,
            sort_columns_descriptions,
            limit,
        })
    }
}

impl Stream for SortStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some(DataBlock::sort_block(
                &v,
                &self.sort_columns_descriptions,
                self.limit,
            )),
            other => other,
        })
    }
}
