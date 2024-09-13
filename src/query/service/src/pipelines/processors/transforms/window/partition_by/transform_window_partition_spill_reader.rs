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

use databend_common_base::base::dma_read_file_range;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use log::info;
use opendal::Operator;
use tokio::sync::Semaphore;

use super::BucketSpilledWindowPayload;
use super::Location;
use super::Partitioned;
use super::WindowPartitionMeta;
use super::WindowPayload;
use crate::spillers::deserialize_block;

pub struct TransformWindowPartitionSpillReader {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    semaphore: Arc<Semaphore>,
    deserialized_meta: Option<BlockMetaInfoPtr>,
    reading_meta: Option<Partitioned>,
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

        if self.reading_meta.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(WindowPartitionMeta::Partitioned(Partitioned { data, .. })) = data_block
                .get_meta()
                .and_then(WindowPartitionMeta::downcast_ref_from)
            {
                if data
                    .iter()
                    .any(|meta| matches!(meta, WindowPartitionMeta::BucketSpilled(_)))
                {
                    self.input.set_not_need_data();
                    let meta = WindowPartitionMeta::downcast_from(data_block.take_meta().unwrap());
                    if let Some(WindowPartitionMeta::Partitioned(partitioned)) = meta {
                        self.reading_meta = Some(partitioned)
                    } else {
                        unreachable!()
                    }
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

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let Partitioned { bucket, data } = self.reading_meta.take().unwrap();
        let mut blocks = self.load_blocks(&data).await?;

        let new_data = data
            .into_iter()
            .map(|meta| {
                if let WindowPartitionMeta::BucketSpilled(_) = meta {
                    let data = blocks.pop_front().unwrap();
                    WindowPartitionMeta::Payload(WindowPayload { bucket, data })
                } else {
                    meta
                }
            })
            .collect::<_>();
        self.deserialized_meta = Some(WindowPartitionMeta::create_partitioned(bucket, new_data));

        Ok(())
    }
}

impl TransformWindowPartitionSpillReader {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
        semaphore: Arc<Semaphore>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(
            TransformWindowPartitionSpillReader {
                input,
                output,
                operator,
                semaphore,
                deserialized_meta: None,
                reading_meta: None,
            },
        )))
    }

    async fn load_blocks(&mut self, data: &[WindowPartitionMeta]) -> Result<VecDeque<DataBlock>> {
        let mut total_elapsed = Duration::default();
        let log_interval = 100;
        let mut processed_count = 0;

        let jobs = data
            .iter()
            .filter_map(|meta| {
                if let WindowPartitionMeta::BucketSpilled(payload) = meta {
                    let operator = self.operator.clone();
                    let semaphore = self.semaphore.clone();
                    let BucketSpilledWindowPayload {
                        location,
                        data_range,
                        columns_layout,
                        ..
                    } = payload.clone();

                    Some(databend_common_base::runtime::spawn(async move {
                        let instant = Instant::now();

                        let (block, data_size) = match location {
                            Location::Storage(path) => {
                                let _guard = semaphore.acquire().await;
                                let data = operator
                                    .read_with(&path)
                                    .range(data_range)
                                    .await?
                                    .to_bytes();
                                (deserialize_block(&columns_layout, &data), data.len())
                            }
                            Location::Disk(path) => {
                                let (buf, range) = dma_read_file_range(path, data_range).await?;
                                let data = &buf[range];
                                (deserialize_block(&columns_layout, data), data.len())
                            }
                        };

                        // perf
                        {
                            Profile::record_usize_profile(ProfileStatisticsName::SpillReadCount, 1);
                            Profile::record_usize_profile(
                                ProfileStatisticsName::SpillReadBytes,
                                data_size,
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
                                processed_count, data_size, total_elapsed,
                            );
                        }
                        Ok::<_, ErrorCode>(block)
                    }))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let blocks = match futures::future::try_join_all(jobs).await {
            Err(_) => {
                return Err(ErrorCode::TokioError("Cannot join tokio job"));
            }
            Ok(data) => data.into_iter().collect::<std::result::Result<_, _>>()?,
        };

        if processed_count > 0 {
            info!(
                "Read {processed_count} window partition spills successfully, total elapsed: {total_elapsed:?}",
            );
        }
        Ok(blocks)
    }
}
