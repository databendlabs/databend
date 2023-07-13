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

use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_formats::output_format::OutputFormat;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

use crate::row_based_file::buffers::FileOutputBuffers;

pub(super) struct SerializeProcessor {
    output_format: Box<dyn OutputFormat>,
}

impl SerializeProcessor {
    pub(super) fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        output_format: Box<dyn OutputFormat>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            SerializeProcessor { output_format },
        )))
    }
}

impl Transform for SerializeProcessor {
    const NAME: &'static str = "SerializeProcessor";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let mut buffers = vec![];
        let step = 1024;
        let num_rows = block.num_rows();
        for i in (0..num_rows).step_by(step) {
            let end = (i + step).min(num_rows);
            let small_block = block.slice(i..end);
            let bs = self.output_format.serialize_block(&small_block)?;
            buffers.push(bs);
        }
        Ok(FileOutputBuffers::create_block(buffers))
    }
}
