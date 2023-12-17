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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use parking_lot::Mutex;

use crate::SyncSource;
use crate::SyncSourcer;

pub struct BlocksSource {
    data_blocks: Arc<Mutex<VecDeque<DataBlock>>>,
}

impl BlocksSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        data_blocks: Arc<Mutex<VecDeque<DataBlock>>>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, BlocksSource { data_blocks })
    }
}

impl SyncSource for BlocksSource {
    const NAME: &'static str = "BlocksSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        let mut blocks_guard = self.data_blocks.lock();
        match blocks_guard.pop_front() {
            None => Ok(None),
            Some(data_block) => Ok(Some(data_block)),
        }
    }
}
