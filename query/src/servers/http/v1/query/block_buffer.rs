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
use common_exception::Result;
use common_planners::PartInfoPtr;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockReader;
use crate::storages::result::ResultQueryInfo;
use crate::storages::result::ResultTableWriter;

trait BlockGetter {}

pub struct BlockInfo {
    pub part_info: PartInfoPtr,
    pub reader: Arc<BlockReader>,
}

impl BlockInfo {
    async fn get_block(&self) -> Result<DataBlock> {
        self.reader.read(self.part_info.clone()).await
    }
}

enum BlockDataOrInfo {
    Data(DataBlock),
    Info(BlockInfo),
}

pub struct BlockBufferInner {
    pop_stopped: bool,
    push_stopped: bool,
    curr_rows: usize,
    max_rows: usize,
    blocks: VecDeque<BlockDataOrInfo>,
    block_notify: Arc<Notify>,
}

impl BlockBufferInner {
    pub fn new(max_rows: usize, block_notify: Arc<Notify>) -> Self {
        BlockBufferInner {
            pop_stopped: false,
            push_stopped: false,
            curr_rows: 0,
            blocks: Default::default(),
            max_rows,
            block_notify,
        }
    }
}

impl BlockBufferInner {
    fn try_push_block(&mut self, block: DataBlock) -> bool {
        if self.pop_stopped {
            return false;
        }
        if self.curr_rows + block.num_rows() >= self.max_rows {
            false
        } else {
            self.push_block(block);
            true
        }
    }

    fn push_block(&mut self, block: DataBlock) {
        self.curr_rows += block.num_rows();
        self.blocks.push_back(BlockDataOrInfo::Data(block));
        self.block_notify.notify_one();
    }

    fn push_info(&mut self, block_info: BlockInfo) {
        self.blocks.push_back(BlockDataOrInfo::Info(block_info));
        self.block_notify.notify_one();
    }

    fn stop_push(&mut self) {
        self.push_stopped = true;
        self.block_notify.notify_one();
    }

    async fn pop(&mut self) -> (Option<BlockDataOrInfo>, bool) {
        let block = self.blocks.pop_front();
        if let Some(BlockDataOrInfo::Data(b)) = &block {
            self.curr_rows -= b.num_rows();
        }
        let done = self.is_pop_done();
        (block, done)
    }

    fn is_pop_done(&self) -> bool {
        self.push_stopped && self.blocks.is_empty()
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

    pub async fn pop(self: &Arc<Self>) -> Result<(Option<DataBlock>, bool)> {
        let (block, done) = {
            let mut guard = self.buffer.lock().await;
            guard.pop().await
        };

        let r = match block {
            None => None,
            Some(o) => match o {
                BlockDataOrInfo::Data(b) => Some(b),
                BlockDataOrInfo::Info(i) => Some(i.get_block().await?),
            },
        };
        Ok((r, done))
    }

    pub async fn stop_pop(self: &Arc<Self>) {
        let mut guard = self.buffer.lock().await;
        guard.stop_pop()
    }

    pub async fn is_pop_done(self: &Arc<Self>) -> bool {
        let guard = self.buffer.lock().await;
        guard.is_pop_done()
    }

    pub async fn try_push_block(&self, block: DataBlock) -> bool {
        let mut guard = self.buffer.lock().await;
        guard.try_push_block(block)
    }

    pub async fn push_info(&self, block_info: BlockInfo) {
        let mut guard = self.buffer.lock().await;
        guard.push_info(block_info)
    }

    async fn push(&self, block: DataBlock) -> Result<()> {
        let mut guard = self.buffer.lock().await;
        guard.push_block(block);
        Ok(())
    }

    pub(crate) async fn stop_push(&self) -> Result<()> {
        let mut guard = self.buffer.lock().await;
        guard.stop_push();
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait BlockBufferWriter: Send {
    async fn push(&mut self, block: DataBlock) -> Result<()>;
    async fn stop_push(&mut self, abort: bool) -> Result<()>;
}

pub struct BlockBufferWriterMemOnly(pub Arc<BlockBuffer>);

#[async_trait::async_trait]
impl BlockBufferWriter for BlockBufferWriterMemOnly {
    async fn push(&mut self, block: DataBlock) -> Result<()> {
        self.0.push(block).await
    }

    async fn stop_push(&mut self, _abort: bool) -> Result<()> {
        self.0.stop_push().await
    }
}

pub struct BlockBufferWriterWithResultTable {
    buffer: Arc<BlockBuffer>,
    reader: Arc<BlockReader>,
    writer: ResultTableWriter,
}

impl BlockBufferWriterWithResultTable {
    pub async fn create(
        buffer: Arc<BlockBuffer>,
        ctx: Arc<QueryContext>,
        query_info: ResultQueryInfo,
    ) -> Result<Box<dyn BlockBufferWriter>> {
        let schema = query_info.schema.clone();
        let writer = ResultTableWriter::new(ctx.clone(), query_info).await?;
        let projection = (0..schema.fields().len())
            .into_iter()
            .collect::<Vec<usize>>();

        let reader = BlockReader::create(ctx.get_storage_operator()?, schema, projection)?;
        Ok(Box::new(Self {
            buffer,
            reader,
            writer,
        }))
    }
}

#[async_trait::async_trait]
impl BlockBufferWriter for BlockBufferWriterWithResultTable {
    async fn push(&mut self, block: DataBlock) -> Result<()> {
        let block_clone = block.clone();
        if self.buffer.try_push_block(block).await {
            let _ = self.writer.append_block(block_clone).await?;
        } else {
            let part_info = self.writer.append_block(block_clone).await?;
            let block_info = BlockInfo {
                part_info,
                reader: self.reader.clone(),
            };
            self.buffer.push_info(block_info).await;
        }
        Ok(())
    }

    async fn stop_push(&mut self, abort: bool) -> Result<()> {
        if abort {
            self.writer.abort().await?;
        } else {
            self.writer.commit().await?;
        }
        self.buffer.stop_push().await?;
        Ok(())
    }
}
