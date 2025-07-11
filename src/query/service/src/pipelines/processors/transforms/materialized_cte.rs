use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
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
use databend_common_pipeline_sinks::Sink;
use databend_common_pipeline_sinks::Sinker;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use tokio::sync::watch::Receiver;
use tokio::sync::watch::Sender;

pub struct MaterializedCteSink {
    sender: Sender<Arc<MaterializedCteData>>,
    blocks: Vec<DataBlock>,
}

#[derive(Default)]
pub struct MaterializedCteData {
    blocks: Vec<DataBlock>,
    // consumer_id -> current_index
    consumer_states: Arc<Mutex<HashMap<usize, usize>>>,
}

impl MaterializedCteData {
    pub fn new(blocks: Vec<DataBlock>) -> Self {
        Self {
            blocks,
            consumer_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_next_block(&self, consumer_id: usize) -> Option<DataBlock> {
        let mut states = self.consumer_states.lock().unwrap();
        let current_index = states.get(&consumer_id).copied().unwrap_or(0);

        if current_index < self.blocks.len() {
            let block = self.blocks[current_index].clone();
            states.insert(consumer_id, current_index + 1);
            Some(block)
        } else {
            None
        }
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
            .send(Arc::new(MaterializedCteData::new(self.blocks.clone())))
            .map_err(|_| {
                ErrorCode::Internal("Failed to send blocks to materialized cte consumer")
            })?;
        Ok(())
    }
}

pub struct CTESource {
    receiver: Receiver<Arc<MaterializedCteData>>,
    data: Option<Arc<MaterializedCteData>>,
    consumer_id: usize,
}

impl CTESource {
    pub fn create(
        ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        output_port: Arc<OutputPort>,
        receiver: Receiver<Arc<MaterializedCteData>>,
        consumer_id: usize,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output_port, Self {
            receiver,
            data: None,
            consumer_id,
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
            if let Some(block) = data.get_next_block(self.consumer_id) {
                return Ok(Some(block));
            }
        }
        Ok(None)
    }
}
