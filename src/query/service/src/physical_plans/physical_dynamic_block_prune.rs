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

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::runtime_filter_info::DynamicBlockPruneFilter;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::ScalarRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::pipelines::PipelineBuilder;
use crate::sessions::TableContextRuntimeFilter;

/// Executes the build side first, collects its string values into an exact set,
/// then returns the probe side after Fuse has pruned non-matching blocks.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DynamicBlockPrune {
    pub meta: PhysicalPlanMeta,
    pub build: PhysicalPlan,
    pub probe: PhysicalPlan,
    pub build_key: RemoteExpr,
    pub scan_id: usize,
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for DynamicBlockPrune {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        self.probe.output_schema()
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.probe).chain(std::iter::once(&self.build)))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.probe).chain(std::iter::once(&mut self.build)))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 2);
        let build = children.pop().unwrap();
        let probe = children.pop().unwrap();
        PhysicalPlan::new(Self {
            meta: self.meta.clone(),
            build,
            probe,
            build_key: self.build_key.clone(),
            scan_id: self.scan_id,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let filter = DynamicBlockPruneFilter::create();
        builder
            .ctx
            .set_dynamic_block_prune_filter(self.scan_id, filter.clone());

        let build_builder = builder.create_sub_pipeline_builder();
        let mut build_res = build_builder.finalize(&self.build)?;
        build_res.main_pipeline.resize(1, true)?;

        let build_key = self.build_key.as_expr(&BUILTIN_FUNCTIONS);
        let func_ctx = builder.func_ctx.clone();
        build_res.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(AsyncSinker::create(
                input,
                DynamicBlockPruneSink {
                    filter: filter.clone(),
                    build_key: build_key.clone(),
                    func_ctx: func_ctx.clone(),
                    locations: HashSet::new(),
                },
            )))
        })?;

        builder
            .pipelines
            .push(build_res.main_pipeline.finalize(None));
        builder.pipelines.extend(build_res.sources_pipelines);
        self.probe.build_pipeline(builder)
    }
}

struct DynamicBlockPruneSink {
    filter: Arc<DynamicBlockPruneFilter>,
    build_key: databend_common_expression::Expr,
    func_ctx: FunctionContext,
    locations: HashSet<String>,
}

#[async_trait::async_trait]
impl AsyncSink for DynamicBlockPruneSink {
    const NAME: &'static str = "DynamicBlockPruneSink";
    const CALL_ON_FINISH_ON_ERROR: bool = false;

    async fn consume(&mut self, block: DataBlock) -> Result<bool> {
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let values = evaluator
            .run(&self.build_key)?
            .convert_to_full_column(self.build_key.data_type(), block.num_rows());

        for value in values.iter() {
            match value {
                ScalarRef::String(location) => {
                    self.locations.insert(location.to_string());
                }
                ScalarRef::Null => {}
                _ => {
                    return Err(ErrorCode::Internal(
                        "dynamic block prune build key must evaluate to String",
                    ));
                }
            }
        }
        Ok(false)
    }

    async fn on_finish(&mut self) -> Result<()> {
        self.filter.publish(std::mem::take(&mut self.locations));
        Ok(())
    }
}
