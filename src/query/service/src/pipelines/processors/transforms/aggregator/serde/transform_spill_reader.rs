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
use std::collections::VecDeque;
use std::sync::Arc;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::SpilledPayload;

type DeserializingMeta = (AggregateMeta, VecDeque<Vec<u8>>);

pub struct TransformSpillReader {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    semaphore: Arc<Semaphore>,
    params: Arc<AggregatorParams>,
    output_data: Option<DataBlock>,
    reading_meta: Option<AggregateMeta>,
    deserializing_meta: Option<DeserializingMeta>,
}

#[async_trait::async_trait]
impl Processor for TransformSpillReader {
    fn name(&self) -> String {
        String::from("TransformSpillReader")
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

        if self.deserializing_meta.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Sync);
        }

        if self.reading_meta.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block
                .get_meta()
                .and_then(AggregateMeta::downcast_ref_from)
            {
                if matches!(block_meta, AggregateMeta::SpilledPayload(_)) {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.reading_meta = AggregateMeta::downcast_from(block_meta);
                    return Ok(Event::Async);
                }
            }

            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some((meta, mut read_data)) = self.deserializing_meta.take() {
            match meta {
                AggregateMeta::SpilledPayload(payload) => {
                    debug_assert!(read_data.len() == 1);
                    let data = read_data.pop_front().unwrap();
                    self.output_data = Some(DataBlock::empty_with_meta(Box::new(
                        self.deserialize(payload, data),
                    )));
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(block_meta) = self.reading_meta.take() {
            match &block_meta {
                AggregateMeta::SpilledPayload(payload) => {
                    let _guard = self.semaphore.acquire().await;
                    let data = self
                        .operator
                        .read_with(&payload.location)
                        .range(payload.data_range.clone())
                        .await?
                        .to_vec();

                    // info!(
                    //     "Read aggregate spill {} successfully, elapsed: {:?}",
                    //     &payload.location,
                    //     instant.elapsed()
                    // );

                    self.deserializing_meta = Some((block_meta, VecDeque::from(vec![data])));
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }
}

impl TransformSpillReader {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
        semaphore: Arc<Semaphore>,
        params: Arc<AggregatorParams>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformSpillReader {
            input,
            output,
            operator,
            semaphore,
            params,
            output_data: None,
            reading_meta: None,
            deserializing_meta: None,
        })))
    }

    fn deserialize(&self, payload: SpilledPayload, data: Vec<u8>) -> AggregateMeta {
        let columns = self.params.group_data_types.len() + self.params.aggregate_functions.len();

        let mut blocks = vec![];
        let mut cursor = data.as_slice();

        while !cursor.is_empty() {
            let mut block_columns = Vec::with_capacity(columns);

            for _idx in 0..columns {
                let column_size = cursor.read_u64::<BigEndian>().unwrap();
                let (left, right) = cursor.split_at(column_size as usize);
                block_columns.push(deserialize_column(left).unwrap());
                cursor = right;
            }

            let block1 = DataBlock::new_from_columns(block_columns);
            blocks.push(block1);
        }

        let block = DataBlock::concat(&blocks).unwrap();

        AggregateMeta::Serialized(SerializedPayload {
            bucket: payload.partition,
            data_block: block,
            max_partition: payload.max_partition,
            global_max_partition: payload.global_max_partition,
        })
    }
}

pub type TransformAggregateSpillReader = TransformSpillReader;
