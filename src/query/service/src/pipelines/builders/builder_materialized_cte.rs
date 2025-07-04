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

use databend_common_exception::Result;
use databend_common_sql::executor::physical_plans::MaterializedCTE;

use crate::pipelines::processors::transforms::MaterializedCteData;
use crate::pipelines::processors::transforms::MaterializedCteSink;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
impl PipelineBuilder {
    pub(crate) fn build_materialized_cte(&mut self, cte: &MaterializedCTE) -> Result<()> {
        // init builder for cte pipeline
        let sub_context = QueryContext::create_from(self.ctx.as_ref());
        let mut sub_builder =
            PipelineBuilder::create(self.func_ctx.clone(), self.settings.clone(), sub_context);
        sub_builder.cte_receivers = self.cte_receivers.clone();
        sub_builder.next_cte_consumer_id = self.next_cte_consumer_id.clone();

        // build cte pipeline
        let mut build_res = sub_builder.finalize(&cte.left)?;
        build_res.main_pipeline.try_resize(1)?;
        let (tx, rx) = tokio::sync::watch::channel(Arc::new(MaterializedCteData::default()));
        self.cte_receivers.insert(cte.cte_name.clone(), rx);
        self.next_cte_consumer_id
            .lock()
            .insert(cte.cte_name.clone(), 0);
        build_res
            .main_pipeline
            .add_sink(|input| MaterializedCteSink::create(input, tx.clone()))?;

        // add cte pipeline to pipelines
        self.pipelines.push(build_res.main_pipeline);
        self.pipelines.extend(build_res.sources_pipelines);

        // build main pipeline
        self.build_pipeline(&cte.right)?;
        Ok(())
    }
}
