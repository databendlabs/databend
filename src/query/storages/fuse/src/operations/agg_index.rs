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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::TransformDummy;

use crate::operations::index::RefreshAggIndexTransform;
use crate::FuseTable;

impl FuseTable {
    pub(crate) fn do_refresh_aggregating_indexes(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        // 1. Duplicate the pipes.
        pipeline.duplicate(false)?;
        // 2. Reorder the pipes.
        let output_len = pipeline.output_len();
        debug_assert!(output_len % 2 == 0);
        let mut rule = vec![0; output_len];
        for (i, r) in rule.iter_mut().enumerate().take(output_len) {
            *r = if i % 2 == 0 {
                i / 2
            } else {
                output_len / 2 + i / 2
            };
        }
        pipeline.reorder_inputs(rule);

        // `output_len` / 2 for `TransformDummy`; 1 for `WriteResultCacheSink`.
        let mut items = Vec::with_capacity(output_len / 2 + 1);
        // 3. Add `TransformDummy` to the front half pipes.
        for _ in 0..output_len / 2 {
            let input = InputPort::create();
            let output = OutputPort::create();
            items.push(PipeItem::create(
                TransformDummy::create(input.clone(), output.clone()),
                vec![input],
                vec![output],
            ));
        }

        // 4. Add `WriteResultCacheSink` (`AsyncMpscSinker`) to the back half pipes.
        for _ in 0..output_len / 2 {
            let input = InputPort::create();
            let output = OutputPort::create();
            items.push(PipeItem::create(
                RefreshAggIndexTransform::try_create(ctx.clone(), input.clone(), output.clone())?,
                vec![input],
                vec![output],
            ));
        }

        pipeline.add_pipe(Pipe::create(output_len, output_len / 2 + 1, items));

        Ok(())
    }
}
