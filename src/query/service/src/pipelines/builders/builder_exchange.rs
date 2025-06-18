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
use databend_common_pipeline_core::PlanScope;
use databend_common_sql::executor::physical_plans::ExchangeSink;
use databend_common_sql::executor::physical_plans::ExchangeSource;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub fn build_exchange_source(&mut self, exchange_source: &ExchangeSource) -> Result<()> {
        let exchange_manager = self.ctx.get_exchange_manager();
        let mut build_res = exchange_manager.get_fragment_source(
            &exchange_source.query_id,
            exchange_source.source_fragment_id,
            self.exchange_injector.clone(),
        )?;

        let plan_scope = PlanScope::get_plan_scope();
        for node in build_res.main_pipeline.graph.node_weights_mut() {
            let Some(scope) = node.scope.as_mut() else {
                node.scope = plan_scope.clone();
                continue;
            };

            if scope.parent_id.is_none() {
                unsafe {
                    let scope = Arc::get_mut_unchecked(scope);
                    scope.parent_id = plan_scope.as_ref().map(|x| x.id);
                }
            }
        }

        // add sharing data
        self.join_state = build_res.builder_data.input_join_state;
        self.merge_into_probe_data_fields = build_res.builder_data.input_probe_schema;

        self.main_pipeline = build_res.main_pipeline;
        self.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }

    pub fn build_exchange_sink(&mut self, exchange_sink: &ExchangeSink) -> Result<()> {
        // ExchangeSink will be appended by `ExchangeManager::execute_pipeline`
        self.build_pipeline(&exchange_sink.input)?;
        Ok(())
    }
}
