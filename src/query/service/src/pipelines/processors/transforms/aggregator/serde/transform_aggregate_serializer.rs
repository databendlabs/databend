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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::pipelines::processors::transforms::aggregator::AggregateExchangeDataCodec;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::SerializeAggregateStream;
pub struct TransformAggregateSerializer {
    codec: Arc<AggregateExchangeDataCodec>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    input_data: Option<SerializeAggregateStream>,
}

impl TransformAggregateSerializer {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(
            TransformAggregateSerializer {
                input,
                output,
                codec: AggregateExchangeDataCodec::create(params),
                input_data: None,
                output_data: None,
            },
        )))
    }
}

impl Processor for TransformAggregateSerializer {
    fn name(&self) -> String {
        String::from("TransformAggregateSerializer")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;
            return self.transform_input_data(data_block);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(stream) = &mut self.input_data {
            self.output_data = Option::transpose(stream.next())?;

            if self.output_data.is_none() {
                self.input_data = None;
            }
        }

        Ok(())
    }
}

impl TransformAggregateSerializer {
    fn transform_input_data(&mut self, mut data_block: DataBlock) -> Result<Event> {
        debug_assert!(data_block.is_empty());

        let Some(AggregateMeta::AggregatePayload(p)) = data_block
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            unreachable!()
        };

        self.input_data = Some(self.codec.encode_stream(p));
        Ok(Event::Sync)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::Int64Type;

    use super::*;
    use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
    use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_codec::tests::params;
    use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_codec::tests::payload_block;

    #[test]
    fn test_merge_serializer_keeps_payload_flush_batches() -> Result<()> {
        let mut serializer = TransformAggregateSerializer {
            codec: AggregateExchangeDataCodec::create(params()),
            input: InputPort::create(),
            output: OutputPort::create(),
            input_data: None,
            output_data: None,
        };
        serializer.transform_input_data(payload_block((0..5000).collect()))?;
        let mut batches = 0;
        let mut values = Vec::new();
        while serializer.input_data.is_some() {
            serializer.process()?;
            if let Some(block) = serializer.output_data.take() {
                batches += 1;
                let meta = block
                    .get_meta()
                    .and_then(AggregateSerdeMeta::downcast_ref_from)
                    .unwrap();
                assert_eq!((meta.bucket, meta.max_partition_count), (7, 8));
                let column = block.get_by_offset(0).to_column();
                values
                    .extend_from_slice(Int64Type::try_downcast_column(&column).unwrap().as_slice());
            }
        }
        assert!(
            batches > 1,
            "merge exchange must not concatenate the whole payload"
        );
        values.sort_unstable();
        assert_eq!(values, (0..5000).collect::<Vec<_>>());
        Ok(())
    }
}
