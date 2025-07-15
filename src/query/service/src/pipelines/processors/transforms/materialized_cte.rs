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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::Sink;
use databend_common_pipeline_sinks::Sinker;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_storages_fuse::TableContext;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::sync::watch::Sender;

pub struct MaterializedCteSink {
    sender: Sender<Arc<MaterializedCteData>>,
    blocks: Vec<DataBlock>,
}

pub struct MaterializedCteData {
    blocks: Vec<DataBlock>,
}

impl MaterializedCteData {
    pub fn new(blocks: Vec<DataBlock>) -> Self {
        Self { blocks }
    }

    pub fn get_data_block_at(&self, index: usize) -> Option<DataBlock> {
        self.blocks.get(index).cloned()
    }
}

impl MaterializedCteSink {
    pub fn create(
        input: Arc<InputPort>,
        sender: Sender<Arc<MaterializedCteData>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Sinker::create(input, Self {
            blocks: vec![],
            sender,
        })))
    }
}

impl Sink for MaterializedCteSink {
    const NAME: &'static str = "MaterializedCteSink";

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.blocks.push(data_block);
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        self.sender
            .send(Arc::new(MaterializedCteData::new(std::mem::take(
                &mut self.blocks,
            ))))
            .map_err(|_| {
                ErrorCode::Internal("Failed to send blocks to materialized cte consumer")
            })?;
        Ok(())
    }
}

pub struct CTESource {
    receiver: Receiver<Arc<MaterializedCteData>>,
    data: Option<Arc<MaterializedCteData>>,
    next_block_id: Arc<AtomicUsize>,
}

impl CTESource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output_port: Arc<OutputPort>,
        receiver: Receiver<Arc<MaterializedCteData>>,
        next_block_id: Arc<AtomicUsize>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output_port, Self {
            receiver,
            data: None,
            next_block_id,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for CTESource {
    const NAME: &'static str = "CTEConsumerSource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.data.is_none() {
            self.receiver.changed().await.map_err(|_| {
                ErrorCode::Internal("Failed to get data from receiver in CTEConsumerSource")
            })?;
            self.data = Some(self.receiver.borrow().clone());
        }

        if let Some(data) = &self.data {
            let id = self.next_block_id.fetch_add(1, Ordering::Relaxed);
            if let Some(block) = data.get_data_block_at(id) {
                return Ok(Some(block));
            }
        }
        Ok(None)
    }
}

pub struct MaterializedCteChannel {
    pub sender: Sender<Arc<MaterializedCteData>>,
    pub receiver: Receiver<Arc<MaterializedCteData>>,
}

impl MaterializedCteChannel {
    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(Arc::new(MaterializedCteData::new(vec![])));
        Self { sender, receiver }
    }
}

impl Default for MaterializedCteChannel {
    fn default() -> Self {
        Self::new()
    }
}
