// Copyright 2022 Datafuse Labs.
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

use std::collections::VecDeque;
use std::sync::Arc;

use common_base::base::tokio::sync::Mutex;
use common_base::base::tokio::sync::Notify;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfoPtr;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockReader;

struct BlockInfo {
    pub num_row: usize,
    pub part_info: PartInfoPtr,
}

enum BlockDataOrInfo {
    Data(DataBlock),
    Info(BlockInfo),
}

impl BlockDataOrInfo {
    fn num_rows(&self) -> usize {
        match self {
            BlockDataOrInfo::Data(d) => d.num_rows(),
            BlockDataOrInfo::Info(i) => i.num_row,
        }
    }
}

pub struct BlockBufferInner {
    pop_stopped: bool,
    push_stopped: bool,
    curr_rows: usize,
    max_rows: usize,
    blocks: VecDeque<BlockDataOrInfo>,
    block_reader: Option<Arc<BlockReader>>,
    block_notify: Arc<Notify>,
}

impl BlockBufferInner {
    pub fn new(max_rows: usize, block_notify: Arc<Notify>) -> Self {
        BlockBufferInner {
            pop_stopped: false,
            push_stopped: false,
            curr_rows: 0,
            blocks: Default::default(),
            block_reader: None,
            max_rows,
            block_notify,
        }
    }
}

impl BlockBufferInner {
    fn push_block(&mut self, block: DataBlock, part_info: PartInfoPtr) {
        if self.pop_stopped {
            return;
        }
        let b = if self.curr_rows + block.num_rows() <= self.max_rows {
            BlockDataOrInfo::Data(block)
        } else {
            BlockDataOrInfo::Info(BlockInfo {
                num_row: block.num_rows(),
                part_info,
            })
        };
        self.curr_rows += b.num_rows();
        self.blocks.push_back(b);
        self.block_notify.notify_one();
    }

    pub fn init_reader(&mut self, ctx: Arc<QueryContext>, schema: DataSchemaRef) -> Result<()> {
        let projection = (0..schema.fields().len())
            .into_iter()
            .collect::<Vec<usize>>();

        self.block_reader = Some(BlockReader::create(
            ctx.get_storage_operator()?,
            schema,
            projection,
        )?);
        Ok(())
    }

    async fn pop(&mut self) -> (Option<BlockDataOrInfo>, bool) {
        let block = self.blocks.pop_front();
        if let Some(ref b) = block {
            self.curr_rows -= b.num_rows();
        }
        let done = self.is_pop_done();
        (block, done)
    }

    fn is_pop_done(&self) -> bool {
        self.push_stopped && self.blocks.is_empty()
    }

    fn stop_push(&mut self) {
        self.push_stopped = true;
        self.block_notify.notify_one();
    }

    pub fn stop_pop(&mut self) {
        self.pop_stopped = true;
        self.blocks.truncate(0)
    }
}

pub struct BlockBuffer {
    buffer: Arc<Mutex<BlockBufferInner>>,
    pub block_notify: Arc<Notify>,
}

impl BlockBuffer {
    pub fn new(max_rows: usize) -> Arc<Self> {
        let block_notify = Arc::new(Notify::new());
        let buffer = Arc::new(Mutex::new(BlockBufferInner::new(
            max_rows,
            block_notify.clone(),
        )));
        Arc::new(BlockBuffer {
            block_notify,
            buffer,
        })
    }

    pub async fn init_reader(
        self: &Arc<Self>,
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
    ) -> Result<()> {
        let mut guard = self.buffer.lock().await;
        guard.init_reader(ctx, schema)
    }

    pub async fn pop(self: &Arc<Self>) -> Result<(Option<DataBlock>, bool)> {
        let (block, done) = {
            let mut guard = self.buffer.lock().await;
            guard.pop().await
        };

        let r = match block {
            None => None,
            Some(o) => match o {
                BlockDataOrInfo::Data(b) => Some(b),
                BlockDataOrInfo::Info(p) => {
                    let reader = {
                        let guard = self.buffer.lock().await;
                        guard
                            .block_reader
                            .as_ref()
                            .ok_or_else(|| ErrorCode::UnknownException("block buffer pop fail"))?
                            .clone()
                    };
                    Some(reader.read(p.part_info).await?)
                }
            },
        };
        Ok((r, done))
    }

    pub async fn push(self: &Arc<Self>, block: DataBlock, part_info: PartInfoPtr) {
        let mut guard = self.buffer.lock().await;
        guard.push_block(block, part_info)
    }

    pub async fn stop_push(self: &Arc<Self>) {
        let mut guard = self.buffer.lock().await;
        guard.stop_push()
    }

    pub async fn stop_pop(self: &Arc<Self>) {
        let mut guard = self.buffer.lock().await;
        guard.stop_pop()
    }

    pub async fn is_pop_done(self: &Arc<Self>) -> bool {
        let guard = self.buffer.lock().await;
        guard.is_pop_done()
    }
}
