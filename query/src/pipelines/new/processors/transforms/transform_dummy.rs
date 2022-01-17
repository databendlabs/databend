use std::sync::Arc;
use common_datablocks::DataBlock;
use common_exception::Result;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::{Transform, Transformer};

pub struct TransformDummy;

impl TransformDummy {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>) -> ProcessorPtr {
        Transformer::create(input, output, TransformDummy {})
    }
}

#[async_trait::async_trait]
impl Transform for TransformDummy {
    const NAME: &'static str = "DummyTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        Ok(data)
    }
}

