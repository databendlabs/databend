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

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct TakeStream {
    input: SendableDataBlockStream,
    remaining: usize,
}

impl TakeStream {
    pub fn new(input: SendableDataBlockStream, n: usize) -> Self {
        TakeStream {
            input,
            remaining: n,
        }
    }
}

impl Stream for TakeStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(ref block)) => {
                let rows = block.num_rows();
                if self.remaining == 0 {
                    None
                } else if self.remaining >= rows {
                    self.remaining -= rows;
                    Some(block.clone())
                } else {
                    let remaining = self.remaining;
                    self.remaining = 0;
                    Some(block.slice(0, remaining))
                }
            }
            .map(Ok),
            other => other,
        })
    }
}
