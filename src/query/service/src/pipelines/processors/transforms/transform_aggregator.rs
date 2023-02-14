// Copyright 2022 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::*;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::transforms::aggregator::*;
use crate::pipelines::processors::AggregatorTransformParams;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

pub struct TransformAggregator;

impl TransformAggregator {
    pub fn try_create_final(
        ctx: Arc<QueryContext>,
        transform_params: AggregatorTransformParams,
    ) -> Result<Box<dyn Processor>> {
        let aggregator_params = transform_params.aggregator_params.clone();

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        if aggregator_params.group_columns.is_empty() {
            return AggregatorTransform::create(
                ctx,
                transform_params,
                FinalSingleStateAggregator::try_create(&aggregator_params, max_threads)?,
            );
        }

        match aggregator_params.aggregate_functions.is_empty() {
            true => with_mappedhash_method!(|T| match transform_params.method.clone() {
                HashMethodKind::T(method) => AggregatorTransform::create(
                    ctx.clone(),
                    transform_params,
                    ParallelFinalAggregator::<false, T>::create(ctx, method, aggregator_params)?,
                ),
            }),

            false => with_mappedhash_method!(|T| match transform_params.method.clone() {
                HashMethodKind::T(method) => AggregatorTransform::create(
                    ctx.clone(),
                    transform_params,
                    ParallelFinalAggregator::<true, T>::create(ctx, method, aggregator_params)?,
                ),
            }),
        }
    }

    pub fn try_create_partial(
        transform_params: AggregatorTransformParams,
        ctx: Arc<QueryContext>,
        pass_state_to_final: bool,
    ) -> Result<Box<dyn Processor>> {
        let aggregator_params = transform_params.aggregator_params.clone();

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        if aggregator_params.group_columns.is_empty() {
            return AggregatorTransform::create(
                ctx,
                transform_params,
                PartialSingleStateAggregator::try_create(&aggregator_params, max_threads)?,
            );
        }

        match aggregator_params.aggregate_functions.is_empty() {
            true => with_mappedhash_method!(|T| match transform_params.method.clone() {
                HashMethodKind::T(method) => AggregatorTransform::create(
                    ctx,
                    transform_params,
                    PartialAggregator::<false, T>::create(
                        method,
                        aggregator_params,
                        pass_state_to_final,
                    )?,
                ),
            }),
            false => with_mappedhash_method!(|T| match transform_params.method.clone() {
                HashMethodKind::T(method) => AggregatorTransform::create(
                    ctx,
                    transform_params,
                    PartialAggregator::<true, T>::create(
                        method,
                        aggregator_params,
                        pass_state_to_final,
                    )?,
                ),
            }),
        }
    }
}

pub trait Aggregator: Sized + Send {
    const NAME: &'static str;

    fn consume(&mut self, data: DataBlock) -> Result<()>;
    // Generate could be called multiple times util it returns empty.
    fn generate(&mut self) -> Result<Vec<DataBlock>>;
}

enum AggregatorTransform<TAggregator: Aggregator + PartitionedAggregatorLike> {
    ConsumeData(ConsumeState<TAggregator>),
    PartitionedConsumeData(PartitionedConsumeState<TAggregator>),
    Generate(GenerateState<TAggregator>),
    PartitionedGenerate(GenerateState<PartitionedAggregator<TAggregator>>),
    Finished,
}

impl<TAggregator: Aggregator + PartitionedAggregatorLike + 'static>
    AggregatorTransform<TAggregator>
{
    pub fn create(
        ctx: Arc<QueryContext>,
        transform_params: AggregatorTransformParams,
        inner: TAggregator,
    ) -> Result<Box<dyn Processor>> {
        let settings = ctx.get_settings();
        let two_level_threshold = settings.get_group_by_two_level_threshold()? as usize;

        let transformer = AggregatorTransform::<TAggregator>::ConsumeData(ConsumeState {
            inner,
            input_port: transform_params.transform_input_port,
            output_port: transform_params.transform_output_port,
            two_level_threshold,
            input_data_block: None,
        });

        if TAggregator::SUPPORT_TWO_LEVEL
            && transform_params.aggregator_params.has_distinct_combinator()
        {
            Ok(Box::new(transformer.convert_to_two_level_consume()?))
        } else {
            Ok(Box::new(transformer))
        }
    }

    pub fn convert_to_generate(self) -> Result<Self> {
        match self {
            AggregatorTransform::ConsumeData(s) => {
                Ok(AggregatorTransform::Generate(GenerateState {
                    inner: s.inner,
                    is_generated: false,
                    output_port: s.output_port,
                    output_data_block: vec![],
                }))
            }
            AggregatorTransform::PartitionedConsumeData(s) => {
                Ok(AggregatorTransform::PartitionedGenerate(GenerateState {
                    inner: s.inner,
                    is_generated: false,
                    output_port: s.output_port,
                    output_data_block: vec![],
                }))
            }
            _ => Err(ErrorCode::Internal("")),
        }
    }

    pub fn convert_to_two_level_consume(self) -> Result<Self> {
        match self {
            AggregatorTransform::ConsumeData(s) => Ok(AggregatorTransform::PartitionedConsumeData(
                PartitionedConsumeState {
                    inner: s.inner.convert_partitioned()?,
                    input_port: s.input_port,
                    output_port: s.output_port,
                    input_data_block: None,
                },
            )),
            _ => Err(ErrorCode::Internal("")),
        }
    }
}

impl<TAggregator: Aggregator + PartitionedAggregatorLike + 'static> Processor
    for AggregatorTransform<TAggregator>
{
    fn name(&self) -> String {
        TAggregator::NAME.to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self {
            AggregatorTransform::Finished => Ok(Event::Finished),
            AggregatorTransform::Generate(_) => self.generate_event(),
            AggregatorTransform::ConsumeData(_) => self.consume_event(),
            AggregatorTransform::PartitionedConsumeData(_) => self.consume_event(),
            AggregatorTransform::PartitionedGenerate(_) => self.generate_event(),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self {
            AggregatorTransform::Finished => Ok(()),
            AggregatorTransform::ConsumeData(state) => state.consume(),
            AggregatorTransform::Generate(state) => state.generate(),
            AggregatorTransform::PartitionedConsumeData(state) => state.consume(),
            AggregatorTransform::PartitionedGenerate(state) => state.generate(),
        }
    }
}

impl<TAggregator: Aggregator + PartitionedAggregatorLike + 'static>
    AggregatorTransform<TAggregator>
{
    #[inline(always)]
    fn consume_event(&mut self) -> Result<Event> {
        if let AggregatorTransform::ConsumeData(state) = self {
            if TAggregator::SUPPORT_TWO_LEVEL {
                let cardinality = state.inner.get_state_cardinality();

                if cardinality >= state.two_level_threshold {
                    let mut temp_state = AggregatorTransform::Finished;
                    std::mem::swap(self, &mut temp_state);
                    temp_state = temp_state.convert_to_two_level_consume()?;
                    std::mem::swap(self, &mut temp_state);
                    debug_assert!(matches!(temp_state, AggregatorTransform::Finished));
                    return Ok(Event::Sync);
                }
            }

            if state.input_data_block.is_some() {
                return Ok(Event::Sync);
            }

            if state.input_port.is_finished() {
                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                temp_state = temp_state.convert_to_generate()?;
                std::mem::swap(self, &mut temp_state);
                debug_assert!(matches!(temp_state, AggregatorTransform::Finished));
                return Ok(Event::Sync);
            }

            return match state.input_port.has_data() {
                true => {
                    state.input_data_block = Some(state.input_port.pull_data().unwrap()?);
                    Ok(Event::Sync)
                }
                false => {
                    state.input_port.set_need_data();
                    Ok(Event::NeedData)
                }
            };
        }

        if let AggregatorTransform::PartitionedConsumeData(state) = self {
            if state.input_data_block.is_some() {
                return Ok(Event::Sync);
            }

            if state.input_port.is_finished() {
                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                temp_state = temp_state.convert_to_generate()?;
                std::mem::swap(self, &mut temp_state);
                debug_assert!(matches!(temp_state, AggregatorTransform::Finished));
                return Ok(Event::Sync);
            }

            return match state.input_port.has_data() {
                true => {
                    state.input_data_block = Some(state.input_port.pull_data().unwrap()?);
                    Ok(Event::Sync)
                }
                false => {
                    state.input_port.set_need_data();
                    Ok(Event::NeedData)
                }
            };
        }

        Err(ErrorCode::Internal("It's a bug"))
    }

    #[inline(always)]
    fn generate_event(&mut self) -> Result<Event> {
        if let AggregatorTransform::Generate(state) = self {
            if state.output_port.is_finished() {
                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            if !state.output_port.can_push() {
                return Ok(Event::NeedConsume);
            }

            if let Some(block) = state.output_data_block.pop() {
                state.output_port.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }

            if state.is_generated {
                if !state.output_port.is_finished() {
                    state.output_port.finish();
                }

                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            return Ok(Event::Sync);
        }

        if let AggregatorTransform::PartitionedGenerate(state) = self {
            if state.output_port.is_finished() {
                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            if !state.output_port.can_push() {
                return Ok(Event::NeedConsume);
            }

            if !state.output_data_block.is_empty() {
                let block = state.output_data_block.remove(0);
                state.output_port.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }

            if state.is_generated {
                if !state.output_port.is_finished() {
                    state.output_port.finish();
                }

                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            return Ok(Event::Sync);
        }

        Err(ErrorCode::Internal("It's a bug"))
    }
}

struct ConsumeState<TAggregator: Aggregator> {
    inner: TAggregator,
    two_level_threshold: usize,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data_block: Option<DataBlock>,
}

impl<TAggregator: Aggregator> ConsumeState<TAggregator> {
    pub fn consume(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_data_block.take() {
            self.inner.consume(input_data)?;
        }

        Ok(())
    }
}

struct PartitionedConsumeState<TAggregator: Aggregator + PartitionedAggregatorLike> {
    inner: PartitionedAggregator<TAggregator>,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data_block: Option<DataBlock>,
}

impl<TAggregator: Aggregator + PartitionedAggregatorLike> PartitionedConsumeState<TAggregator> {
    pub fn consume(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_data_block.take() {
            self.inner.consume(input_data)?;
        }

        Ok(())
    }
}

struct GenerateState<TAggregator: Aggregator> {
    inner: TAggregator,
    is_generated: bool,
    output_port: Arc<OutputPort>,
    output_data_block: Vec<DataBlock>,
}

impl<TAggregator: Aggregator> GenerateState<TAggregator> {
    pub fn generate(&mut self) -> Result<()> {
        if !self.is_generated {
            self.output_data_block = self.inner.generate()?;

            // if it's empty, it means the aggregator is finished.
            if self.output_data_block.is_empty() {
                self.is_generated = true;
            }
        }
        Ok(())
    }
}
