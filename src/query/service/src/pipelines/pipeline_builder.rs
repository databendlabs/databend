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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::FunctionContext;
use databend_common_pipeline_core::always_callback;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_settings::Settings;

use super::PipelineBuilderData;
use crate::interpreters::CreateTableInterpreter;
use crate::physical_plans::PhysicalPlan;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::HashJoinBuildState;
use crate::pipelines::processors::HashJoinState;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::exchange::DefaultExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::sessions::QueryContext;

#[derive(Clone)]
pub enum HashJoinStateRef {
    OldHashJoinState(Arc<HashJoinState>),
    NewHashJoinState(Arc<BasicHashJoinState>),
}

pub struct PipelineBuilder {
    pub(crate) ctx: Arc<QueryContext>,
    pub(crate) func_ctx: FunctionContext,
    pub(crate) main_pipeline: Pipeline,
    pub(crate) settings: Arc<Settings>,

    pub pipelines: Vec<Pipeline>,

    // probe data_fields for distributed merge into when source build
    pub merge_into_probe_data_fields: Option<Vec<DataField>>,
    pub join_state: Option<Arc<HashJoinBuildState>>,

    pub(crate) exchange_injector: Arc<dyn ExchangeInjector>,

    pub hash_join_states: HashMap<usize, HashJoinStateRef>,

    pub r_cte_scan_interpreters: Vec<CreateTableInterpreter>,
    pub(crate) is_exchange_stack: Vec<bool>,
}

impl PipelineBuilder {
    pub fn create(
        func_ctx: FunctionContext,
        settings: Arc<Settings>,
        ctx: Arc<QueryContext>,
    ) -> PipelineBuilder {
        PipelineBuilder {
            ctx,
            func_ctx,
            settings,
            pipelines: vec![],
            main_pipeline: Pipeline::create(),
            exchange_injector: DefaultExchangeInjector::create(),
            merge_into_probe_data_fields: None,
            join_state: None,
            hash_join_states: HashMap::new(),
            r_cte_scan_interpreters: vec![],
            is_exchange_stack: vec![],
        }
    }

    pub fn finalize(mut self, plan: &PhysicalPlan) -> Result<PipelineBuildResult> {
        self.build_pipeline(plan)?;

        for source_pipeline in &self.pipelines {
            if !source_pipeline.is_complete_pipeline()? {
                return Err(ErrorCode::Internal(
                    "Source pipeline must be complete pipeline.",
                ));
            }
        }

        // unload spill metas
        if !self.ctx.mark_unload_callbacked() {
            self.main_pipeline
                .set_on_finished(always_callback(move |_info: &ExecutionInfo| {
                    self.ctx.unload_spill_meta();
                    Ok(())
                }));
        }

        Ok(PipelineBuildResult {
            main_pipeline: self.main_pipeline,
            sources_pipelines: self.pipelines,
            exchange_injector: self.exchange_injector,
            builder_data: PipelineBuilderData {
                input_join_state: self.join_state,
                input_probe_schema: self.merge_into_probe_data_fields,
            },
            r_cte_scan_interpreters: self.r_cte_scan_interpreters,
        })
    }

    pub(crate) fn is_exchange_parent(&self) -> bool {
        if self.is_exchange_stack.len() >= 2 {
            return self.is_exchange_stack[self.is_exchange_stack.len() - 2];
        }

        false
    }

    #[recursive::recursive]
    pub(crate) fn build_pipeline(&mut self, plan: &PhysicalPlan) -> Result<()> {
        plan.build_pipeline(self)
    }
}

pub fn attach_runtime_filter_logger(ctx: Arc<QueryContext>, pipeline: &mut Pipeline) {
    pipeline.set_on_finished(always_callback(move |_info: &ExecutionInfo| {
        ctx.log_runtime_filter_stats();
        Ok(())
    }));
}
