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
use std::sync::Arc;

use databend_common_base::base::Progress;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;
use databend_common_functions::topk_count::exact::ExactTopKCountState;
use databend_common_functions::topk_count::exact::create_exact_topk_count_state;
use databend_common_functions::topk_count::topk_count_capacity;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use futures::TryStreamExt;

use super::AggregateFinal;
use super::AggregatePartial;
use super::physical_plan::IPhysicalPlan;
use super::physical_plan::PhysicalPlan;
use super::physical_plan::PhysicalPlanCast;
use super::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::sessions::QueryContext;
use crate::stream::PullingExecutorStream;

/// Local, adaptive COUNT aggregation. Its child is an unchanged exact
/// aggregation; that pipeline is not built unless the bounded proof fails.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AdaptiveTopKCount {
    pub meta: PhysicalPlanMeta,
    pub fallback: PhysicalPlan,
    pub limit: usize,
}

#[typetag::serde]
impl IPhysicalPlan for AdaptiveTopKCount {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }
    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }
    fn children(&self) -> Box<dyn Iterator<Item = &PhysicalPlan> + '_> {
        Box::new(std::iter::once(&self.fallback))
    }
    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.fallback))
    }
    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(Self {
            meta: self.meta.clone(),
            fallback: children.remove(0),
            limit: self.limit,
        })
    }
    fn get_desc(&self) -> Result<String> {
        Ok(format!(
            "limit {}, bounded exact counts or full aggregation",
            self.limit
        ))
    }
    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let aggregate = AggregateFinal::from_physical_plan(&self.fallback)
            .ok_or_else(|| ErrorCode::Internal("Adaptive Top-K requires a final aggregation"))?;
        let partial = AggregatePartial::from_physical_plan(&aggregate.input).ok_or_else(|| {
            ErrorCode::Internal("Adaptive Top-K requires a local partial aggregation")
        })?;
        let key = partial
            .input
            .output_schema()?
            .index_of(&aggregate.group_by[0].to_string())?;
        let key_type = partial
            .input
            .output_schema()?
            .field(key)
            .data_type()
            .clone();
        builder.main_pipeline.add_source(
            |output| {
                AsyncSourcer::create(Arc::new(Progress::create()), output, AdaptiveCountSource {
                    ctx: QueryContext::create_from(&builder.ctx),
                    input: Some(partial.input.clone()),
                    fallback: self.fallback.clone(),
                    key,
                    key_type: key_type.clone(),
                    limit: self.limit,
                    stream: None,
                    finished: false,
                })
            },
            1,
        )
    }
}

struct PartialCount {
    key: usize,
    limit: usize,
    nullable: bool,
    failed: bool,
    state: Box<dyn ExactTopKCountState>,
}

impl AccumulatingTransform for PartialCount {
    const NAME: &'static str = "PartialExactTopKCount";
    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if !self.failed {
            self.state.add_block(block.get_by_offset(self.key))?;
            if !self.state.can_still_prove(self.limit, self.nullable) {
                self.failed = true;
                // An empty payload asks the consumer to cancel this pass.
                // It cannot become provable even if more input is read.
                let mut builder = ColumnBuilder::with_capacity(&DataType::Binary, 1);
                builder.push(ScalarRef::Binary(&[]));
                return Ok(vec![DataBlock::new_from_columns(vec![builder.build()])]);
            }
        }
        Ok(vec![])
    }
    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        if !generate_data || self.failed {
            return Ok(vec![]);
        }
        let data = self.state.serialize()?;
        let mut builder = ColumnBuilder::with_capacity(&DataType::Binary, 1);
        builder.push(ScalarRef::Binary(&data));
        Ok(vec![DataBlock::new_from_columns(vec![builder.build()])])
    }
}

struct AdaptiveCountSource {
    ctx: Arc<QueryContext>,
    input: Option<PhysicalPlan>,
    fallback: PhysicalPlan,
    key: usize,
    key_type: DataType,
    limit: usize,
    stream: Option<PullingExecutorStream>,
    finished: bool,
}

impl AdaptiveCountSource {
    fn executor(&self, plan: &PhysicalPlan, bounded: bool) -> Result<PullingExecutorStream> {
        // Cancellation of an unprovable first pass may leave scan tasks
        // winding down. Each attempt needs its own partition queue.
        let ctx = QueryContext::create_from(&self.ctx);
        let builder =
            PipelineBuilder::create(ctx.get_function_context()?, ctx.get_settings(), ctx.clone());
        let mut pipelines = builder.finalize(plan)?;
        if bounded {
            let capacity = topk_count_capacity(self.limit);
            pipelines.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                    input,
                    output,
                    PartialCount {
                        key: self.key,
                        limit: self.limit,
                        nullable: self.key_type.is_nullable(),
                        failed: false,
                        state: create_exact_topk_count_state(&self.key_type, capacity)?,
                    },
                )))
            })?;
        }
        pipelines.set_max_threads(self.ctx.get_settings().get_max_threads()? as usize);
        let executor =
            PipelinePullingExecutor::from_pipelines(pipelines, ExecutorSettings::try_create(ctx)?)?;
        // Do not replace QueryContext's executor: it belongs to the parent
        // pipeline. Dropping this stream (including cancellation of generate)
        // finishes the nested executor through PipelinePullingExecutor::drop.
        PullingExecutorStream::create(executor)
    }
}

#[async_trait::async_trait]
impl AsyncSource for AdaptiveCountSource {
    const NAME: &'static str = "AdaptiveTopKCount";

    async fn on_finish(&mut self) -> Result<()> {
        // Also stop a fallback when the consumer finishes early.
        self.stream = None;
        Ok(())
    }
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        if let Some(input) = self.input.take() {
            let mut stream = self.executor(&input, true)?;
            let mut merged =
                create_exact_topk_count_state(&self.key_type, topk_count_capacity(self.limit))?;
            let mut failed = false;
            while let Some(block) = stream.try_next().await? {
                let column = block.get_by_offset(0).to_column();
                let ScalarRef::Binary(data) = column.index(0).unwrap() else {
                    return Err(ErrorCode::Internal("Invalid adaptive Top-K partial state"));
                };
                if data.is_empty() {
                    failed = true;
                    break;
                }
                merged.merge_serialized(data)?;
                if !merged.can_still_prove(self.limit, self.key_type.is_nullable()) {
                    failed = true;
                    break;
                }
            }
            drop(stream);
            if !failed && let Some(result) = merged.result(self.limit, &self.key_type) {
                self.finished = true;
                return Ok(Some(result));
            }
            // Only now build the full aggregation and start its fresh scan.
            // Both plans use the same pinned FUSE snapshot.
            self.stream = Some(self.executor(&self.fallback, false)?);
        }
        let next = self.stream.as_mut().unwrap().try_next().await?;
        if next.is_none() {
            self.stream = None;
            self.finished = true;
        }
        Ok(next)
    }
}
