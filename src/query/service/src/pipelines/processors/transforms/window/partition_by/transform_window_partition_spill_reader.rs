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
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use itertools::Itertools;
use log::info;
use opendal::Operator;

use super::WindowPartitionMeta;
use super::WindowPayload;
use crate::pipelines::processors::transforms::window::partition_by::BucketSpilledWindowPayload;

type DeserializingMeta = (WindowPartitionMeta, VecDeque<Vec<u8>>);

pub struct TransformWindowPartitionSpillReader {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    deserialized_meta: Option<BlockMetaInfoPtr>,
    reading_meta: Option<WindowPartitionMeta>,
    deserializing_meta: Option<DeserializingMeta>,
}

#[async_trait::async_trait]
impl Processor for TransformWindowPartitionSpillReader {
    fn name(&self) -> String {
        String::from("TransformWindowPartitionSpillReader")
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

        if let Some(deserialized_meta) = self.deserialized_meta.take() {
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(deserialized_meta)));
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

            if let Some(WindowPartitionMeta::Partitioned { data, .. }) = data_block
                .get_meta()
                .and_then(WindowPartitionMeta::downcast_ref_from)
            {
                if data
                    .iter()
                    .any(|meta| matches!(meta, WindowPartitionMeta::BucketSpilled(_)))
                {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.reading_meta = WindowPartitionMeta::downcast_from(block_meta);
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
                WindowPartitionMeta::Spilled(_) => unreachable!(),
                WindowPartitionMeta::Spilling(_) => unreachable!(),
                WindowPartitionMeta::BucketSpilled(_) => unreachable!(),
                WindowPartitionMeta::Payload(_) => unreachable!(),
                WindowPartitionMeta::Partitioned { bucket, data } => {
                    let mut new_data = Vec::with_capacity(data.len());

                    for meta in data {
                        if matches!(&meta, WindowPartitionMeta::BucketSpilled(_)) {
                            if let WindowPartitionMeta::BucketSpilled(p) = meta {
                                let data = read_data.pop_front().unwrap();
                                new_data.push(Self::deserialize(p, data));
                            }

                            continue;
                        }

                        new_data.push(meta);
                    }

                    self.deserialized_meta =
                        Some(WindowPartitionMeta::create_partitioned(bucket, new_data));
                }
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(block_meta) = self.reading_meta.take() {
            match &block_meta {
                WindowPartitionMeta::Spilled(_) => unreachable!(),
                WindowPartitionMeta::Spilling(_) => unreachable!(),
                WindowPartitionMeta::BucketSpilled(_) => unreachable!(),
                WindowPartitionMeta::Payload(_) => unreachable!(),
                WindowPartitionMeta::Partitioned { data, .. } => {
                    let mut total_elapsed = Duration::default();
                    let log_interval = 100;
                    let mut processed_count = 0;

                    let mut read_data = Vec::with_capacity(data.len());
                    for meta in data {
                        if let WindowPartitionMeta::BucketSpilled(p) = meta {
                            let location = p.location.clone();
                            let operator = self.operator.clone();
                            let data_range = p.data_range.clone();
                            read_data.push(databend_common_base::runtime::spawn(async move {
                                let instant = Instant::now();
                                let data = operator
                                    .read_with(&location)
                                    .range(data_range)
                                    .await?
                                    .to_vec();

                                // perf
                                {
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::SpillReadCount,
                                        1,
                                    );
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::SpillReadBytes,
                                        data.len(),
                                    );
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::SpillReadTime,
                                        instant.elapsed().as_millis() as usize,
                                    );
                                }

                                total_elapsed += instant.elapsed();
                                processed_count += 1;

                                // log the progress
                                if processed_count % log_interval == 0 {
                                    info!(
                                        "Read window partition {}/{} spilled buckets, elapsed: {:?}",
                                        processed_count,
                                        data.len(),
                                        total_elapsed
                                    );
                                }

                                Ok(data)
                            }));
                        }
                    }

                    match futures::future::try_join_all(read_data).await {
                        Err(_) => {
                            return Err(ErrorCode::TokioError("Cannot join tokio job"));
                        }
                        Ok(read_data) => {
                            let read_data: Result<VecDeque<Vec<u8>>, opendal::Error> =
                                read_data.into_iter().try_collect();

                            self.deserializing_meta = Some((block_meta, read_data?));
                        }
                    };

                    info!(
                        "Read {} window partition spills successfully, total elapsed: {:?}",
                        processed_count, total_elapsed
                    );
                }
            }
        }

        Ok(())
    }
}

impl TransformWindowPartitionSpillReader {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(
            TransformWindowPartitionSpillReader {
                input,
                output,
                operator,
                deserialized_meta: None,
                reading_meta: None,
                deserializing_meta: None,
            },
        )))
    }

    fn deserialize(payload: BucketSpilledWindowPayload, data: Vec<u8>) -> WindowPartitionMeta {
        let mut begin = 0;
        let mut columns = Vec::with_capacity(payload.columns_layout.len());

        for column_layout in payload.columns_layout {
            columns.push(deserialize_column(&data[begin..begin + column_layout as usize]).unwrap());
            begin += column_layout as usize;
        }

        WindowPartitionMeta::Payload(WindowPayload {
            bucket: payload.bucket,
            data: DataBlock::new_from_columns(columns),
        })
    }
}
