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

use databend_common_exception::Result;

use crate::physical_plans::BroadcastSink;
use crate::physical_plans::BroadcastSource;
use crate::pipelines::processors::transforms::BroadcastSinkProcessor;
use crate::pipelines::processors::transforms::BroadcastSourceProcessor;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_broadcast_source(&mut self, source: &BroadcastSource) -> Result<()> {
        let receiver = self.ctx.broadcast_source_receiver(source.broadcast_id);
        self.main_pipeline.add_source(
            |output| BroadcastSourceProcessor::create(self.ctx.clone(), receiver.clone(), output),
            1,
        )
    }

    pub(crate) fn build_broadcast_sink(&mut self, sink: &BroadcastSink) -> Result<()> {
        self.build_pipeline(&sink.input)?;
        self.main_pipeline.resize(1, true)?;
        self.main_pipeline.add_sink(|input| {
            BroadcastSinkProcessor::create(input, self.ctx.broadcast_sink_sender(sink.broadcast_id))
        })
    }
}
