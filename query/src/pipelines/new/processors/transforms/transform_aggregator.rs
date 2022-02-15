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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datablocks::HashMethodKind;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::aggregator::FinalSingleKeyAggregator;
use crate::pipelines::new::processors::transforms::aggregator::KeysU16FinalAggregator;
use crate::pipelines::new::processors::transforms::aggregator::KeysU16PartialAggregator;
use crate::pipelines::new::processors::transforms::aggregator::KeysU32FinalAggregator;
use crate::pipelines::new::processors::transforms::aggregator::KeysU32PartialAggregator;
use crate::pipelines::new::processors::transforms::aggregator::KeysU64FinalAggregator;
use crate::pipelines::new::processors::transforms::aggregator::KeysU64PartialAggregator;
use crate::pipelines::new::processors::transforms::aggregator::KeysU8FinalAggregator;
use crate::pipelines::new::processors::transforms::aggregator::KeysU8PartialAggregator;
use crate::pipelines::new::processors::transforms::aggregator::PartialSingleKeyAggregator;
use crate::pipelines::new::processors::transforms::aggregator::SerializerFinalAggregator;
use crate::pipelines::new::processors::transforms::aggregator::SerializerPartialAggregator;
use crate::pipelines::new::processors::AggregatorTransformParams;
use crate::pipelines::new::processors::Processor;

pub struct TransformAggregator;

impl TransformAggregator {
    pub fn try_create_final(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        transform_params: AggregatorTransformParams,
    ) -> Result<ProcessorPtr> {
        let aggregator_params = transform_params.aggregator_params;

        if aggregator_params.group_columns_name.is_empty() {
            return AggregatorTransform::create(
                input_port,
                output_port,
                FinalSingleKeyAggregator::try_create(&aggregator_params)?,
            );
        }

        match aggregator_params.aggregate_functions.is_empty() {
            true => match transform_params.method {
                HashMethodKind::KeysU8(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU8FinalAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU16(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU16FinalAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU32(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU32FinalAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU64(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU64FinalAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::Serializer(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    SerializerFinalAggregator::<false>::create(method, aggregator_params),
                ),
            },
            false => match transform_params.method {
                HashMethodKind::KeysU8(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU8FinalAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU16(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU16FinalAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU32(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU32FinalAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU64(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU64FinalAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::Serializer(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    SerializerFinalAggregator::<true>::create(method, aggregator_params),
                ),
            },
        }
    }

    pub fn try_create_partial(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        transform_params: AggregatorTransformParams,
    ) -> Result<ProcessorPtr> {
        let aggregator_params = transform_params.aggregator_params;

        if aggregator_params.group_columns_name.is_empty() {
            return AggregatorTransform::create(
                input_port,
                output_port,
                PartialSingleKeyAggregator::try_create(&aggregator_params)?,
            );
        }

        match aggregator_params.aggregate_functions.is_empty() {
            true => match transform_params.method {
                HashMethodKind::KeysU8(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU8PartialAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU16(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU16PartialAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU32(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU32PartialAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU64(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU64PartialAggregator::<false>::create(method, aggregator_params),
                ),
                HashMethodKind::Serializer(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    SerializerPartialAggregator::<false>::create(method, aggregator_params),
                ),
            },
            false => match transform_params.method {
                HashMethodKind::KeysU8(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU8PartialAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU16(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU16PartialAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU32(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU32PartialAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::KeysU64(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    KeysU64PartialAggregator::<true>::create(method, aggregator_params),
                ),
                HashMethodKind::Serializer(method) => AggregatorTransform::create(
                    transform_params.transform_input_port,
                    transform_params.transform_output_port,
                    SerializerPartialAggregator::<true>::create(method, aggregator_params),
                ),
            },
        }
    }
}

pub trait Aggregator: Sized + Send {
    const NAME: &'static str;

    fn consume(&mut self, data: DataBlock) -> Result<()>;
    fn generate(&mut self) -> Result<Option<DataBlock>>;
}

enum AggregatorTransform<TAggregator: Aggregator> {
    ConsumeData(ConsumeState<TAggregator>),
    Generate(GenerateState<TAggregator>),
    Finished,
}

impl<TAggregator: Aggregator + 'static> AggregatorTransform<TAggregator> {
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        inner: TAggregator,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(AggregatorTransform::<
            TAggregator,
        >::ConsumeData(
            ConsumeState {
                inner,
                input_port,
                output_port,
                input_data_block: None,
            },
        ))))
    }

    pub fn convert_to_generate(self) -> Result<Self> {
        match self {
            AggregatorTransform::ConsumeData(s) => {
                Ok(AggregatorTransform::Generate(GenerateState {
                    inner: s.inner,
                    is_finished: false,
                    output_port: s.output_port,
                    output_data_block: None,
                }))
            }
            _ => Err(ErrorCode::LogicalError("")),
        }
    }
}

impl<TAggregator: Aggregator + 'static> Processor for AggregatorTransform<TAggregator> {
    fn name(&self) -> &'static str {
        TAggregator::NAME
    }

    fn event(&mut self) -> Result<Event> {
        match self {
            AggregatorTransform::Finished => Ok(Event::Finished),
            AggregatorTransform::Generate(_) => self.generate_event(),
            AggregatorTransform::ConsumeData(_) => self.consume_event(),
        }
    }

    fn process(&mut self) -> Result<()> {
        match self {
            AggregatorTransform::Finished => Ok(()),
            AggregatorTransform::ConsumeData(state) => state.consume(),
            AggregatorTransform::Generate(state) => state.generate(),
        }
    }
}

impl<TAggregator: Aggregator + 'static> AggregatorTransform<TAggregator> {
    #[inline(always)]
    fn consume_event(&mut self) -> Result<Event> {
        if let AggregatorTransform::ConsumeData(state) = self {
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

        Err(ErrorCode::LogicalError("It's a bug"))
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

            if let Some(block) = state.output_data_block.take() {
                state.output_port.push_data(Ok(block));
                return Ok(Event::NeedConsume);
            }

            if state.is_finished {
                if !state.output_port.is_finished() {
                    state.output_port.finish();
                }

                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            return Ok(Event::Sync);
        }

        Err(ErrorCode::LogicalError("It's a bug"))
    }
}

struct ConsumeState<TAggregator: Aggregator> {
    inner: TAggregator,
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

struct GenerateState<TAggregator: Aggregator> {
    inner: TAggregator,
    is_finished: bool,
    output_port: Arc<OutputPort>,
    output_data_block: Option<DataBlock>,
}

impl<TAggregator: Aggregator> GenerateState<TAggregator> {
    pub fn generate(&mut self) -> Result<()> {
        let generate_data = self.inner.generate()?;

        if generate_data.is_none() {
            self.is_finished = true;
        }

        self.output_data_block = generate_data;
        Ok(())
    }
}
