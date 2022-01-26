use std::sync::Arc;
use tonic::Code::Unimplemented;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::{ErrorCode, Result};
use common_planners::Expression;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::pipelines::new::processors::transforms::transform::Transform;

pub struct TransformAggregatorPartial;

impl TransformAggregatorPartial {
    pub fn try_create(
        schema: DataSchemaRef,
        schema_before_group_by: DataSchemaRef,
        aggr_exprs: &[Expression],
        group_exprs: &[Expression],
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        unimplemented!()
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

impl<TAggregator: Aggregator> AggregatorTransform<TAggregator> {
    pub fn create(inner: TAggregator) -> Result<ProcessorPtr> {
        unimplemented!()
        // Ok(ProcessorPtr::create(Box::new(AggregatorTransform::<TAggregator>::ConsumeData(inner))))
    }

    pub fn to_generate(mut self) -> Result<Self> {
        match self {
            AggregatorTransform::ConsumeData(s) => Ok(AggregatorTransform::Generate(
                GenerateState {
                    inner: s.inner,
                    input_port: s.input_port,
                    output_port: s.output_port,
                    input_data_block: None,
                }
            )),
            _ => Err(ErrorCode::LogicalError(""))
        }
    }
}

impl<TAggregator: Aggregator> Processor for AggregatorTransform<TAggregator> {
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
}

impl<TAggregator: Aggregator> AggregatorTransform<TAggregator> {
    #[inline(always)]
    fn consume_event(&mut self) -> Result<Event> {
        if let AggregatorTransform::ConsumeData(state) = self {
            if state.input_data_block.is_some() {
                return Ok(Event::Sync);
            }

            if state.input_port.is_finished() {
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
            if state.output_port.is_finished() {}
        }

        unimplemented!()
    }
}

struct ConsumeState<TAggregator: Aggregator> {
    inner: TAggregator,
    input_port: Arc<InputPort>,
    output_port: Arc<InputPort>,
    input_data_block: Option<DataBlock>,
}

impl<TAggregator: Aggregator> ConsumeState<TAggregator> {
    pub fn consume(&mut self) -> Result<()> {
        unimplemented!()
    }
}

struct GenerateState<TAggregator: Aggregator> {
    inner: TAggregator,
    is_finished: bool,
    input_port: Arc<InputPort>,
    output_port: Arc<InputPort>,
    input_data_block: Option<DataBlock>,
}

impl<TAggregator: Aggregator> GenerateState<TAggregator> {
    pub fn generate(&mut self) -> Result<()> {
        unimplemented!()
    }
}
