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

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::usize;

use common_datablocks::DataBlock;
use common_exception::Result;
use futures::stream::Stream;

use crate::sessions::QueryContext;

#[derive(Debug, Clone)]
struct BlockRange {
    _begin: u64,
    _end: u64,
}

pub struct MemoryTableStream {
    ctx: Arc<QueryContext>,
    block_index: usize,
    block_ranges: Vec<usize>,
    blocks: Vec<DataBlock>,
}

impl MemoryTableStream {
    pub fn try_create(ctx: Arc<QueryContext>, blocks: Vec<DataBlock>) -> Result<Self> {
        Ok(Self {
            ctx,
            block_index: 0,
            block_ranges: vec![],
            blocks,
        })
    }

    fn try_get_one_block(&mut self) -> Result<Option<DataBlock>> {
        if (self.block_index as usize) == self.block_ranges.len() {
            let partitions = self.ctx.try_get_partitions(1)?;
            if partitions.is_empty() {
                return Ok(None);
            }
            if partitions.len() == 1 && partitions[0].name.is_empty() {
                return Ok(None);
            }

            let mut block_ranges = Vec::with_capacity(partitions.len());

            for part in partitions {
                let names: Vec<_> = part.name.split('-').collect();
                let begin: usize = names[1].parse()?;
                let end: usize = names[2].parse()?;

                let s: Vec<usize> = (begin..end).collect();
                block_ranges.extend_from_slice(&s);
            }
            self.block_ranges = block_ranges;
            self.block_index = 0;
        }

        if self.block_index == self.block_ranges.len() {
            return Ok(None);
        }
        let current = self.block_ranges[self.block_index];
        self.block_index += 1;
        Ok(Some(self.blocks[current].clone()))
    }
}

impl Stream for MemoryTableStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let block = self.try_get_one_block()?;

        Poll::Ready(block.map(Ok))
    }
}
