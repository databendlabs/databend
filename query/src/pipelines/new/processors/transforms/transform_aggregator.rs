use std::alloc::Layout;
use std::sync::Arc;
use tonic::Code::Unimplemented;
use common_datablocks::{DataBlock, HashMethodKeysU16, HashMethodKeysU32, HashMethodKeysU64, HashMethodKeysU8, HashMethodKind, HashMethodSerializer};
use common_datavalues::DataSchemaRef;
use common_exception::{ErrorCode, Result};
use common_functions::aggregates::{AggregateFunctionRef, get_layout_offsets};
use common_planners::Expression;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::pipelines::new::processors::transforms::group_by::{KeysU16PartialAggregator, KeysU32PartialAggregator, KeysU64PartialAggregator, KeysU8PartialAggregator, PartialAggregator, SerializerPartialAggregator, WithoutGroupBy};
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::transforms::group_by::{AggregatorParams, AggregatorParamsRef};

pub struct TransformAggregatorPartial;

impl TransformAggregatorPartial {
    fn extract_group_columns(group_exprs: &[Expression]) -> Vec<String> {
        group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>()
    }

    pub fn try_create(
        schema: DataSchemaRef,
        schema_before_group_by: DataSchemaRef,
        aggr_exprs: &[Expression],
        group_exprs: &[Expression],
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        if group_exprs.is_empty() {
            return AggregatorTransform::create(
                input_port,
                output_port,
                WithoutGroupBy::create(schema, schema_before_group_by, aggr_exprs)?,
            );
        }

        let group_cols = Self::extract_group_columns(group_exprs);
        let sample_block = DataBlock::empty_with_schema(schema_before_group_by.clone());
        let hash_method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;
        let params = AggregatorParams::try_create(&schema, &schema_before_group_by, aggr_exprs, &group_cols)?;

        match !aggr_exprs.is_empty() {
            true => Self::create::<true>(input_port, output_port, hash_method, params),
            false => Self::create::<false>(input_port, output_port, hash_method, params)
        }
    }

    fn create<const HAS_AGG: bool>(input: Arc<InputPort>, output: Arc<OutputPort>, method: HashMethodKind, params: AggregatorParamsRef) -> Result<ProcessorPtr> {
        if HAS_AGG {
            return match method {
                HashMethodKind::KeysU8(m) => AggregatorTransform::create(
                    input, output, KeysU8PartialAggregator::<true>::create(m, params)),
                HashMethodKind::KeysU16(m) => AggregatorTransform::create(
                    input, output, KeysU16PartialAggregator::<true>::create(m, params)),
                HashMethodKind::KeysU32(m) => AggregatorTransform::create(
                    input, output, KeysU32PartialAggregator::<true>::create(m, params)),
                HashMethodKind::KeysU64(m) => AggregatorTransform::create(
                    input, output, KeysU64PartialAggregator::<true>::create(m, params)),
                HashMethodKind::Serializer(m) => AggregatorTransform::create(
                    input, output, SerializerPartialAggregator::<true>::create(m, params)),
            };
        } else {
            return match method {
                HashMethodKind::KeysU8(m) => AggregatorTransform::create(
                    input, output, KeysU8PartialAggregator::<false>::create(m, params)),
                HashMethodKind::KeysU16(m) => AggregatorTransform::create(
                    input, output, KeysU16PartialAggregator::<false>::create(m, params)),
                HashMethodKind::KeysU32(m) => AggregatorTransform::create(
                    input, output, KeysU32PartialAggregator::<false>::create(m, params)),
                HashMethodKind::KeysU64(m) => AggregatorTransform::create(
                    input, output, KeysU64PartialAggregator::<false>::create(m, params)),
                HashMethodKind::Serializer(m) => AggregatorTransform::create(
                    input, output, SerializerPartialAggregator::<false>::create(m, params)),
            };
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
    pub fn create(input_port: Arc<InputPort>, output_port: Arc<OutputPort>, inner: TAggregator) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(AggregatorTransform::<TAggregator>::ConsumeData(
            ConsumeState {
                inner,
                input_port,
                output_port,
                input_data_block: None,
            }
        ))))
    }

    pub fn to_generate(self) -> Result<Self> {
        match self {
            AggregatorTransform::ConsumeData(s) => Ok(AggregatorTransform::Generate(
                GenerateState {
                    inner: s.inner,
                    is_finished: false,
                    input_port: s.input_port,
                    output_port: s.output_port,
                    output_data_block: None,
                }
            )),
            _ => Err(ErrorCode::LogicalError(""))
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
                println!("ConsumeState finished");
                let mut temp_state = AggregatorTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                temp_state = temp_state.to_generate()?;
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
            println!("ConsumeState consume");
            self.inner.consume(input_data)?;
        }

        Ok(())
    }
}

struct GenerateState<TAggregator: Aggregator> {
    inner: TAggregator,
    is_finished: bool,
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    output_data_block: Option<DataBlock>,
}

impl<TAggregator: Aggregator> GenerateState<TAggregator> {
    pub fn generate(&mut self) -> Result<()> {
        let generate_data = self.inner.generate()?;
        println!("GenerateState generate");
        if generate_data.is_none() {
            println!("GenerateState finished");
            self.is_finished = true;
        }

        self.output_data_block = generate_data;
        Ok(())
    }
}
