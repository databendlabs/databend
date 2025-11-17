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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use itertools::Itertools;
use log::info;
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;

type DeserializingMeta = (AggregateMeta, VecDeque<Vec<u8>>);

pub struct TransformSpillReader {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    semaphore: Arc<Semaphore>,
    deserialized_meta: Option<BlockMetaInfoPtr>,
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

            if let Some(block_meta) = data_block
                .get_meta()
                .and_then(AggregateMeta::downcast_ref_from)
            {
                if matches!(block_meta, AggregateMeta::BucketSpilled(_)) {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.reading_meta = AggregateMeta::downcast_from(block_meta);
                    return Ok(Event::Async);
                }

                if let AggregateMeta::Partitioned { data, .. } = block_meta {
                    if data
                        .iter()
                        .any(|meta| matches!(meta, AggregateMeta::BucketSpilled(_)))
                    {
                        self.input.set_not_need_data();
                        let block_meta = data_block.take_meta().unwrap();
                        self.reading_meta = AggregateMeta::downcast_from(block_meta);
                        return Ok(Event::Async);
                    }
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
                AggregateMeta::Spilled(_) => unreachable!(),
                AggregateMeta::AggregatePayload(_) => unreachable!(),
                AggregateMeta::AggregateSpilling(_) => unreachable!(),
                AggregateMeta::Serialized(_) => unreachable!(),
                AggregateMeta::BucketSpilled(payload) => {
                    debug_assert!(read_data.len() == 1);
                    let data = read_data.pop_front().unwrap();

                    self.deserialized_meta = Some(Box::new(Self::deserialize(payload, data)));
                }
                AggregateMeta::Partitioned { bucket, data } => {
                    let mut new_data = Vec::with_capacity(data.len());

                    for meta in data {
                        if matches!(&meta, AggregateMeta::BucketSpilled(_)) {
                            if let AggregateMeta::BucketSpilled(payload) = meta {
                                let data = read_data.pop_front().unwrap();
                                new_data.push(Self::deserialize(payload, data));
                            }

                            continue;
                        }

                        new_data.push(meta);
                    }

                    self.deserialized_meta =
                        Some(AggregateMeta::create_partitioned(bucket, new_data));
                }
                AggregateMeta::NewBucketSpilled(_) => unreachable!(),
                AggregateMeta::NewSpilled(_) => unreachable!(),
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(block_meta) = self.reading_meta.take() {
            match &block_meta {
                AggregateMeta::Spilled(_) => unreachable!(),
                AggregateMeta::AggregatePayload(_) => unreachable!(),
                AggregateMeta::AggregateSpilling(_) => unreachable!(),
                AggregateMeta::Serialized(_) => unreachable!(),
                AggregateMeta::NewBucketSpilled(_) => unreachable!(),
                AggregateMeta::NewSpilled(_) => unreachable!(),
                AggregateMeta::BucketSpilled(payload) => {
                    let _guard = self.semaphore.acquire().await;
                    let instant = Instant::now();
                    let data = self
                        .operator
                        .read_with(&payload.location)
                        .range(payload.data_range.clone())
                        .await?
                        .to_vec();

                    let elapsed = instant.elapsed();

                    info!(
                        "Read aggregate finished: (location: {}, bytes: {}, elapsed: {:?})",
                        &payload.location,
                        data.len(),
                        elapsed
                    );

                    // perf
                    {
                        Profile::record_usize_profile(
                            ProfileStatisticsName::RemoteSpillReadCount,
                            1,
                        );
                        Profile::record_usize_profile(
                            ProfileStatisticsName::RemoteSpillReadBytes,
                            data.len(),
                        );
                        Profile::record_usize_profile(
                            ProfileStatisticsName::RemoteSpillReadTime,
                            elapsed.as_millis() as usize,
                        );
                    }

                    self.deserializing_meta = Some((block_meta, VecDeque::from(vec![data])));
                }
                AggregateMeta::Partitioned { data, .. } => {
                    // For log progress.
                    let log_interval = 100;
                    let total_buckets = data.len();
                    let total_elapsed = Arc::new(AtomicU64::new(0));
                    let processed_count = Arc::new(AtomicUsize::new(0));
                    let processed_bytes = Arc::new(AtomicUsize::new(0));

                    let mut read_data = Vec::with_capacity(data.len());
                    for meta in data {
                        if let AggregateMeta::BucketSpilled(payload) = meta {
                            let location = payload.location.clone();
                            let operator = self.operator.clone();
                            let data_range = payload.data_range.clone();
                            let semaphore = self.semaphore.clone();
                            let total_elapsed = total_elapsed.clone();
                            let processed_count = processed_count.clone();
                            let processed_bytes = processed_bytes.clone();
                            read_data.push(databend_common_base::runtime::spawn(async move {
                                let _guard = semaphore.acquire().await;
                                let instant = Instant::now();
                                let data = operator
                                    .read_with(&location)
                                    .range(data_range)
                                    .await?
                                    .to_vec();

                                // perf
                                {
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::RemoteSpillReadCount,
                                        1,
                                    );
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::RemoteSpillReadBytes,
                                        data.len(),
                                    );
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::RemoteSpillReadTime,
                                        instant.elapsed().as_millis() as usize,
                                    );
                                }

                                let elapsed = instant.elapsed();
                                total_elapsed
                                    .fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);
                                let finished = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                                processed_bytes.fetch_add(data.len(), Ordering::Relaxed);

                                // log the progress
                                if finished % log_interval == 0 {
                                    info!(
                                        "Read aggregate {}/{} spilled buckets, elapsed: {:?}",
                                        finished,
                                        total_buckets,
                                        Duration::from_millis(
                                            total_elapsed.load(Ordering::Relaxed)
                                        )
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
                            let read_data: std::result::Result<VecDeque<Vec<u8>>, opendal::Error> =
                                read_data.into_iter().try_collect();

                            self.deserializing_meta = Some((block_meta, read_data?));
                        }
                    };

                    let processed_count = processed_count.load(Ordering::Relaxed);
                    if processed_count != 0 {
                        info!(
                            "Read aggregate finished: (total count: {}, total bytes: {}, total elapsed: {:?})",
                            processed_count,
                            processed_bytes.load(Ordering::Relaxed),
                            Duration::from_millis(total_elapsed.load(Ordering::Relaxed))
                        );
                    }
                }
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
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformSpillReader {
            input,
            output,
            operator,
            semaphore,
            deserialized_meta: None,
            reading_meta: None,
            deserializing_meta: None,
        })))
    }

    fn deserialize(payload: BucketSpilledPayload, data: Vec<u8>) -> AggregateMeta {
        let mut begin = 0;
        let mut columns = Vec::with_capacity(payload.columns_layout.len());

        for column_layout in payload.columns_layout {
            columns.push(deserialize_column(&data[begin..begin + column_layout as usize]).unwrap());
            begin += column_layout as usize;
        }

        AggregateMeta::Serialized(SerializedPayload {
            bucket: payload.bucket,
            data_block: DataBlock::new_from_columns(columns),
            max_partition_count: payload.max_partition_count,
        })
    }
}

pub type TransformAggregateSpillReader = TransformSpillReader;
