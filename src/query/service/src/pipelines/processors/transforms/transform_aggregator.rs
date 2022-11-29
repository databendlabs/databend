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
use common_expression::Chunk;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::aggregator::*;
use crate::pipelines::processors::AggregatorTransformParams;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

pub struct TransformAggregator;

impl TransformAggregator {
    pub fn try_create_final(
        transform_params: AggregatorTransformParams,
        ctx: Arc<QueryContext>,
    ) -> Result<ProcessorPtr> {
        let aggregator_params = transform_params.aggregator_params.clone();

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        if aggregator_params.group_columns.is_empty() {
            return AggregatorTransform::create(
                ctx,
                transform_params,
                FinalSingleStateAggregator::try_create(&aggregator_params, max_threads)?,
            );
        }

        todo!("expression");
    }

    pub fn try_create_partial(
        transform_params: AggregatorTransformParams,
        ctx: Arc<QueryContext>,
    ) -> Result<ProcessorPtr> {
        let aggregator_params = transform_params.aggregator_params.clone();

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        if aggregator_params.group_columns.is_empty() {
            return AggregatorTransform::create(
                ctx,
                transform_params,
                PartialSingleStateAggregator::try_create(&aggregator_params, max_threads)?,
            );
        }

        todo!("expression");
    }
}

pub trait Aggregator: Sized + Send {
    const NAME: &'static str;

    fn consume(&mut self, data: Chunk) -> Result<()>;
    fn generate(&mut self) -> Result<Vec<Chunk>>;
}

enum AggregatorTransform<TAggregator: Aggregator + TwoLevelAggregatorLike> {
    ConsumeData(ConsumeState<TAggregator>),
    TwoLevelConsumeData(TwoLevelConsumeState<TAggregator>),
    Generate(GenerateState<TAggregator>),
    TwoLevelGenerate(GenerateState<TwoLevelAggregator<TAggregator>>),
    Finished,
}

impl<TAggregator: Aggregator + TwoLevelAggregatorLike + 'static> AggregatorTransform<TAggregator> {
    pub fn create(
        ctx: Arc<QueryContext>,
        transform_params: AggregatorTransformParams,
        inner: TAggregator,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let two_level_threshold = settings.get_group_by_two_level_threshold()? as usize;

        let transformer = AggregatorTransform::<TAggregator>::ConsumeData(ConsumeState {
            inner,
            input_port: transform_params.transform_input_port,
            output_port: transform_params.transform_output_port,
            two_level_threshold,
            input_chunk: None,
        });

        if TAggregator::SUPPORT_TWO_LEVEL
            && transform_params.aggregator_params.has_distinct_combinator()
        {
            Ok(ProcessorPtr::create(Box::new(
                transformer.convert_to_two_level_consume()?,
            )))
        } else {
            Ok(ProcessorPtr::create(Box::new(transformer)))
        }
    }

    pub fn convert_to_generate(self) -> Result<Self> {
        match self {
            AggregatorTransform::ConsumeData(s) => {
                Ok(AggregatorTransform::Generate(GenerateState {
                    inner: s.inner,
                    is_generated: false,
                    output_port: s.output_port,
                    output_chunk: vec![],
                }))
            }
            AggregatorTransform::TwoLevelConsumeData(s) => {
                Ok(AggregatorTransform::TwoLevelGenerate(GenerateState {
                    inner: s.inner,
                    is_generated: false,
                    output_port: s.output_port,
                    output_chunk: vec![],
                }))
            }
            _ => Err(ErrorCode::Internal("")),
        }
    }

    pub fn convert_to_two_level_consume(self) -> Result<Self> {
        match self {
            AggregatorTransform::ConsumeData(s) => Ok(AggregatorTransform::TwoLevelConsumeData(
                TwoLevelConsumeState {
                    inner: s.inner.convert_two_level()?,
                    input_port: s.input_port,
                    output_port: s.output_port,
                    input_chunk: None,
                },
            )),
            _ => Err(ErrorCode::Internal("")),
        }
    }
}

impl<TAggregator: Aggregator + TwoLevelAggregatorLike + 'static> Processor
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
            AggregatorTransform::TwoLevelConsumeData(_) => self.consume_event(),
            AggregatorTransform::TwoLevelGenerate(_) => self.generate_event(),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self {
            AggregatorTransform::Finished => Ok(()),
            AggregatorTransform::ConsumeData(state) => state.consume(),
            AggregatorTransform::Generate(state) => state.generate(),
            AggregatorTransform::TwoLevelConsumeData(state) => state.consume(),
            AggregatorTransform::TwoLevelGenerate(state) => state.generate(),
        }
    }
}

impl<TAggregator: Aggregator + TwoLevelAggregatorLike + 'static> AggregatorTransform<TAggregator> {
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

            if state.input_chunk.is_some() {
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
                    state.input_chunk = Some(state.input_port.pull_data().unwrap()?);
                    Ok(Event::Sync)
                }
                false => {
                    state.input_port.set_need_data();
                    Ok(Event::NeedData)
                }
            };
        }

        if let AggregatorTransform::TwoLevelConsumeData(state) = self {
            if state.input_chunk.is_some() {
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
                    state.input_chunk = Some(state.input_port.pull_data().unwrap()?);
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

            if let Some(chunk) = state.output_chunk.pop() {
                state.output_port.push_data(Ok(chunk));
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

        if let AggregatorTransform::TwoLevelGenerate(state) = self {
            if state.output_port.is_finished() {
                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            if !state.output_port.can_push() {
                return Ok(Event::NeedConsume);
            }

            if let Some(chunk) = state.output_chunk.pop() {
                state.output_port.push_data(Ok(chunk));
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
    input_chunk: Option<Chunk>,
}

impl<TAggregator: Aggregator> ConsumeState<TAggregator> {
    pub fn consume(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_chunk.take() {
            self.inner.consume(input_data)?;
        }

        Ok(())
    }
}

struct TwoLevelConsumeState<TAggregator: Aggregator + TwoLevelAggregatorLike> {
    inner: TwoLevelAggregator<TAggregator>,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_chunk: Option<Chunk>,
}

impl<TAggregator: Aggregator + TwoLevelAggregatorLike> TwoLevelConsumeState<TAggregator> {
    pub fn consume(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_chunk.take() {
            self.inner.consume(input_data)?;
        }

        Ok(())
    }
}

struct GenerateState<TAggregator: Aggregator> {
    inner: TAggregator,
    is_generated: bool,
    output_port: Arc<OutputPort>,
    output_chunk: Vec<Chunk>,
}

impl<TAggregator: Aggregator> GenerateState<TAggregator> {
    pub fn generate(&mut self) -> Result<()> {
        if !self.is_generated {
            self.is_generated = true;
            self.output_chunk = self.inner.generate()?;
        }

        Ok(())
    }
}
