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
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::topk_count::IS_TOPK_COUNT_CANDIDATE;
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
/// aggregation; that pipeline is not built unless the bounded exact proof
/// fails. A complete candidate proof may filter its input before validation.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AdaptiveTopKCount {
    pub meta: PhysicalPlanMeta,
    pub fallback: PhysicalPlan,
    pub limit: usize,
    /// Cost hint only; runtime proofs are still required for any output/filter.
    pub likely_candidates: bool,
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
            likely_candidates: self.likely_candidates,
        })
    }
    fn get_desc(&self) -> Result<String> {
        Ok(format!(
            "limit {}, exact counts, candidate validation or full aggregation",
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
                    likely_candidates: self.likely_candidates,
                    stream: None,
                    finished: false,
                })
            },
            1,
        )
    }
}

const CHECKPOINT_STATE: u8 = 0;
const FINAL_STATE: u8 = 1;

struct PartialCount {
    key: usize,
    limit: usize,
    nullable: bool,
    sample_on_failure: bool,
    state: Box<dyn ExactTopKCountState>,
}

impl PartialCount {
    fn emit(&self, kind: u8) -> Result<DataBlock> {
        let mut data = vec![kind];
        data.extend(self.state.serialize()?);
        let mut builder = ColumnBuilder::with_capacity(&DataType::Binary, 1);
        builder.push(ScalarRef::Binary(&data));
        Ok(DataBlock::new_from_columns(vec![builder.build()]))
    }
}

impl AccumulatingTransform for PartialCount {
    const NAME: &'static str = "PartialExactTopKCount";
    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.state.add_block(block.get_by_offset(self.key))?;
        if self.sample_on_failure && !self.state.can_still_prove(self.limit, self.nullable) {
            // Notify only when exact counts become irrecoverable. Successful
            // workers need neither checkpoint serialization nor backpressure.
            self.sample_on_failure = false;
            return Ok(vec![self.emit(CHECKPOINT_STATE)?]);
        }
        Ok(vec![])
    }
    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        if !generate_data {
            return Ok(vec![]);
        }
        Ok(vec![self.emit(FINAL_STATE)?])
    }
}

struct AdaptiveCountSource {
    ctx: Arc<QueryContext>,
    input: Option<PhysicalPlan>,
    fallback: PhysicalPlan,
    key: usize,
    key_type: DataType,
    limit: usize,
    likely_candidates: bool,
    stream: Option<PullingExecutorStream>,
    finished: bool,
}

impl AdaptiveCountSource {
    fn executor(
        &self,
        plan: &PhysicalPlan,
        bounded: bool,
    ) -> Result<(PullingExecutorStream, usize)> {
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
                        // No sampling or checkpoint traffic is needed when
                        // a cost hint already commits us to the complete pass.
                        sample_on_failure: !self.likely_candidates,
                        state: create_exact_topk_count_state(&self.key_type, capacity)?,
                    },
                )))
            })?;
        }
        let workers = pipelines.main_pipeline.output_len();
        pipelines.set_max_threads(self.ctx.get_settings().get_max_threads()? as usize);
        let executor =
            PipelinePullingExecutor::from_pipelines(pipelines, ExecutorSettings::try_create(ctx)?)?;
        // Do not replace QueryContext's executor: it belongs to the parent
        // pipeline. Dropping this stream (including cancellation of generate)
        // finishes the nested executor through PipelinePullingExecutor::drop.
        Ok((PullingExecutorStream::create(executor)?, workers))
    }

    fn filtered_fallback(&self, candidates: Vec<u8>) -> Result<PhysicalPlan> {
        let final_agg = AggregateFinal::from_physical_plan(&self.fallback)
            .ok_or_else(|| ErrorCode::Internal("Missing adaptive final aggregation"))?;
        let partial = AggregatePartial::from_physical_plan(&final_agg.input)
            .ok_or_else(|| ErrorCode::Internal("Missing adaptive partial aggregation"))?;
        let filtered = PhysicalPlan::new(CandidateInput {
            meta: partial.input.get_meta().clone(),
            input: partial.input.clone(),
            key: self.key,
            candidates,
        });
        let partial = final_agg.input.derive(vec![filtered]);
        Ok(self.fallback.derive(vec![partial]))
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
            let (mut stream, workers) = self.executor(&input, true)?;
            let capacity = topk_count_capacity(self.limit);
            let mut merged = create_exact_topk_count_state(&self.key_type, capacity)?;
            let mut prefix = create_exact_topk_count_state(&self.key_type, capacity)?;
            let mut prefixes = 0;
            let mut finals = 0;
            let mut abandoned = false;
            while let Some(block) = stream.try_next().await? {
                let column = block.get_by_offset(0).to_column();
                let ScalarRef::Binary(data) = column.index(0).unwrap() else {
                    return Err(ErrorCode::Internal("Invalid adaptive Top-K partial state"));
                };
                let Some((&kind, data)) = data.split_first() else {
                    return Err(ErrorCode::Internal("Empty adaptive Top-K partial state"));
                };
                match kind {
                    CHECKPOINT_STATE => {
                        prefix.merge_serialized(data)?;
                        prefixes += 1;
                    }
                    FINAL_STATE => {
                        merged.merge_serialized(data)?;
                        finals += 1;
                    }
                    _ => return Err(ErrorCode::Internal("Unknown adaptive Top-K state kind")),
                }
                // This is only a cost heuristic: a prefix can never authorize
                // candidate pruning or exact output. A positive ANALYZE cost
                // hint keeps the pass running to completion. Otherwise sample
                // up to four failed-worker checkpoints to retain early fallback
                // on unskewed inputs. A missed late hot key only costs speed.
                if !self.likely_candidates
                    && prefixes >= workers.min(4)
                    && finals < workers
                    && !prefix.can_still_prove(self.limit, self.key_type.is_nullable())
                    && prefix.candidates(self.limit)?.is_none()
                {
                    abandoned = true;
                    break;
                }
            }
            drop(stream);
            if !abandoned {
                if finals != workers {
                    return Err(ErrorCode::Internal("Incomplete adaptive Top-K summary"));
                }
                if let Some(result) = merged.result(self.limit, &self.key_type) {
                    self.finished = true;
                    return Ok(Some(result));
                }
            }
            // Only a completed summary may prove a candidate filter. Prefix
            // sampling is never used for filtering; abandoned passes aggregate
            // every row. All attempts use the same pinned FUSE snapshot.
            let candidates = if abandoned {
                None
            } else {
                merged.candidates(self.limit)?
            };
            let fallback = match candidates {
                Some(candidates) => self.filtered_fallback(candidates)?,
                None => self.fallback.clone(),
            };
            self.stream = Some(self.executor(&fallback, false)?.0);
        }
        let next = self.stream.as_mut().unwrap().try_next().await?;
        if next.is_none() {
            self.stream = None;
            self.finished = true;
        }
        Ok(next)
    }
}

/// An execution-only filter injected below the fallback partial aggregation.
/// It adds no profile plan ID: the original scan/aggregation IDs remain stable.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct CandidateInput {
    meta: PhysicalPlanMeta,
    input: PhysicalPlan,
    key: usize,
    candidates: Vec<u8>,
}

#[typetag::serde]
impl IPhysicalPlan for CandidateInput {
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
        Box::new(std::iter::once(&self.input))
    }
    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.input))
    }
    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(Self {
            meta: self.meta.clone(),
            input: children.remove(0),
            key: self.key,
            candidates: self.candidates.clone(),
        })
    }
    fn display_in_profile(&self) -> bool {
        false
    }
    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let schema = self.input.output_schema()?;
        let field = schema.field(self.key);
        let predicate = check_function(
            None,
            IS_TOPK_COUNT_CANDIDATE,
            &[],
            &[
                Expr::constant(Scalar::Binary(self.candidates.clone()), None),
                Expr::ColumnRef(ColumnRef {
                    span: None,
                    id: self.key,
                    data_type: field.data_type().clone(),
                    display_name: field.name().clone(),
                }),
            ],
            &BUILTIN_FUNCTIONS,
        )?
        .as_remote_expr();
        self.input.build_pipeline(builder)?;
        builder.main_pipeline.add_transform(
            builder.filter_transform_builder::<false>(
                &[predicate],
                (0..schema.num_fields()).collect(),
            )?,
        )
    }
}
