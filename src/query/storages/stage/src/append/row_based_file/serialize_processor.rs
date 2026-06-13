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

use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_formats::output_format::OutputFormat;
use databend_common_pipeline_transforms::processors::Transform;

use super::buffers::FileOutputBuffer;
use super::buffers::FileOutputBuffers;
use crate::append::partition::partition_from_block;

pub(super) struct SerializeProcessor {
    ctx: Arc<dyn TableContext>,
    output_format: Box<dyn OutputFormat>,
}

impl SerializeProcessor {
    pub(super) fn new(ctx: Arc<dyn TableContext>, output_format: Box<dyn OutputFormat>) -> Self {
        SerializeProcessor { output_format, ctx }
    }
}

impl Transform for SerializeProcessor {
    const NAME: &'static str = "SerializeProcessor";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let partition = partition_from_block(&block);
        let mut buffers = vec![];
        let step = 1024;
        let num_rows = block.num_rows();
        let mut bytes = 0;
        for i in (0..num_rows).step_by(step) {
            let end = (i + step).min(num_rows);
            let small_block = block.slice(i..end);
            let bs = self.output_format.serialize_block(&small_block)?;
            bytes += bs.len();
            buffers.push(FileOutputBuffer::create(bs, small_block.num_rows()));
        }
        let progress_values = ProgressValues {
            rows: num_rows,
            bytes,
        };
        self.ctx.get_write_progress().incr(&progress_values);
        Ok(FileOutputBuffers::create_block(buffers, partition))
    }
}
