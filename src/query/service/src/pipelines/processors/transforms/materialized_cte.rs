use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
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
use tokio::sync::watch::Sender;

pub struct MaterializedCteSink {
    sender: Sender<Arc<MaterializedCteData>>,
    blocks: Vec<DataBlock>,
}

#[derive(Default)]
pub struct MaterializedCteData {
    blocks: Vec<DataBlock>,
}

impl MaterializedCteData {
    pub fn new(blocks: Vec<DataBlock>) -> Self {
        Self { blocks }
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
