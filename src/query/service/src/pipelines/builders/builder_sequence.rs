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
use databend_common_sql::executor::physical_plans::Sequence;

use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
impl PipelineBuilder {
    pub(crate) fn build_sequence(&mut self, sequence: &Sequence) -> Result<()> {
        // init builder for cte pipeline
        let sub_context = QueryContext::create_from(self.ctx.as_ref());
        let sub_builder =
            PipelineBuilder::create(self.func_ctx.clone(), self.settings.clone(), sub_context);

        // build cte pipeline
        let build_res = sub_builder.finalize(&sequence.left)?;

        // add cte pipeline to pipelines
        self.pipelines.push(build_res.main_pipeline);
        self.pipelines.extend(build_res.sources_pipelines);

        // build main pipeline
        self.build_pipeline(&sequence.right)
    }
}
