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
use databend_common_sql::executor::physical_plans::RuntimeFilterSink;
use databend_common_sql::executor::physical_plans::RuntimeFilterSource;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::processors::transforms::RuntimeFilterSinkProcessor;
use crate::pipelines::processors::transforms::RuntimeFilterSourceProcessor;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_runtime_filter_source(
        &mut self,
        _source: &RuntimeFilterSource,
    ) -> Result<()> {
        let receiver = self.ctx.rf_src_recv(_source.join_id);
        self.main_pipeline.add_source(
            |output| {
                RuntimeFilterSourceProcessor::create(self.ctx.clone(), receiver.clone(), output)
            },
            1,
        )
    }

    pub(crate) fn build_runtime_filter_sink(&mut self, sink: &RuntimeFilterSink) -> Result<()> {
        self.build_pipeline(&sink.input)?;
        self.main_pipeline.resize(1, true)?;
        let node_num = self.ctx.get_cluster().nodes.len();
        self.main_pipeline
            .add_sink(|input| RuntimeFilterSinkProcessor::create(input, node_num))
    }
}
