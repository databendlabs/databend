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
use std::task::Poll;

use async_channel::Receiver;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::StealablePartitions;
use databend_common_catalog::runtime_filter_info::PartitionRuntimeFilters;
use databend_common_catalog::runtime_filter_info::RuntimeFilterSource;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::NodeIndex;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::core::SyncTaskHandle;
use databend_common_pipeline::core::SyncTaskSet;
use databend_common_sql::IndexType;

use crate::operations::read::block_partition_meta::BlockPartitionMeta;

#[async_trait::async_trait]
pub trait PartitionStream: Send + Sync {
    async fn fetch(&self, id: usize) -> Result<Option<Vec<PartInfoPtr>>>;
}

pub struct StealPartitionStream {
    partitions: StealablePartitions,
    max_batch_size: usize,
}

impl StealPartitionStream {
    pub fn new(partitions: StealablePartitions, max_batch_size: usize) -> Self {
        Self {
            partitions,
            max_batch_size,
        }
    }
}

#[async_trait::async_trait]
impl PartitionStream for StealPartitionStream {
    async fn fetch(&self, id: usize) -> Result<Option<Vec<PartInfoPtr>>> {
        Ok(self.partitions.steal(id, self.max_batch_size))
    }
}

pub struct ReceiverPartitionStream {
    receiver: Receiver<Result<PartInfoPtr>>,
}

impl ReceiverPartitionStream {
    pub fn new(receiver: Receiver<Result<PartInfoPtr>>) -> Self {
        Self { receiver }
    }
}

#[async_trait::async_trait]
impl PartitionStream for ReceiverPartitionStream {
    async fn fetch(&self, _id: usize) -> Result<Option<Vec<PartInfoPtr>>> {
        match self.receiver.recv().await {
            Ok(Ok(part)) => Ok(Some(vec![part])),
            Ok(Err(e)) => Err(e),
            Err(_) => Ok(None),
        }
    }
}

struct DummyPartitionStream;

#[async_trait::async_trait]
impl PartitionStream for DummyPartitionStream {
    async fn fetch(&self, _id: usize) -> Result<Option<Vec<PartInfoPtr>>> {
        Ok(None)
    }
}

pub struct PartitionStreamSource {
    id: NodeIndex,
    worker_id: usize,
    tasks: SyncTaskSet,
    output: Arc<OutputPort>,
    stream: Arc<dyn PartitionStream>,
    handle: Option<SyncTaskHandle<'static, Result<Option<Vec<PartInfoPtr>>>>>,
    /// Runtime filter source for waiting and reading partition filters.
    rf_source: Option<Arc<RuntimeFilterSource>>,
    rf_waited: bool,
    ctx: Arc<dyn TableContext>,
    partition_filters: PartitionRuntimeFilters,
}

impl PartitionStreamSource {
    pub fn create(
        worker_id: usize,
        waker: Arc<ExecutorWaker>,
        output: Arc<OutputPort>,
        stream: Arc<dyn PartitionStream>,
        ctx: Arc<dyn TableContext>,
        _scan_id: IndexType,
        _table_schema: Arc<TableSchema>,
        rf_source: Option<Arc<RuntimeFilterSource>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            output,
            stream,
            worker_id,
            id: Default::default(),
            tasks: SyncTaskSet::new(worker_id, waker),
            handle: None,
            rf_source,
            rf_waited: false,
            ctx,
            partition_filters: vec![],
        })))
    }

    fn close(&mut self) {
        self.stream = Arc::new(DummyPartitionStream);
        self.handle = None;
        self.rf_source = None;
    }

    fn filter_parts(&self, parts: Vec<PartInfoPtr>) -> Vec<PartInfoPtr> {
        if self.partition_filters.is_empty() {
            return parts;
        }
        parts
            .into_iter()
            .filter(|part| !self.partition_filters.iter().any(|f| f.prune(part)))
            .collect()
    }
}

#[async_trait::async_trait]
impl Processor for PartitionStreamSource {
    fn name(&self) -> String {
        String::from("PartitionStreamSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if self.output.is_finished() {
            self.close();
            return Ok(Event::Finished);
        }

        // First time: if we have a runtime filter source, enter async to wait for it.
        if let Some(ref rf_source) = self.rf_source {
            if !self.rf_waited && !rf_source.is_ready() {
                return Ok(Event::Async);
            }
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if self.handle.is_none() {
            let stream = self.stream.clone();
            let worker_id = self.worker_id;
            let fut = Box::pin(async move { stream.fetch(worker_id).await });
            self.handle = Some(self.tasks.spawn(self.id, fut));
        }

        while let Some(mut handle) = self.handle.take() {
            return match handle.poll(matches!(cause, EventCause::Other)) {
                Poll::Ready(Ok(Some(parts))) => {
                    let parts = self.filter_parts(parts);

                    if parts.is_empty() {
                        let worker_id = self.worker_id;
                        let stream = self.stream.clone();
                        let fut = Box::pin(async move { stream.fetch(worker_id).await });
                        self.handle = Some(self.tasks.spawn(self.id, fut));
                        continue;
                    }

                    let block = DataBlock::empty_with_meta(BlockPartitionMeta::create(parts));
                    self.output.push_data(Ok(block));
                    Ok(Event::NeedConsume)
                }
                Poll::Ready(Ok(None)) => {
                    self.close();
                    self.output.finish();
                    Ok(Event::Finished)
                }
                Poll::Ready(Err(e)) => {
                    self.close();
                    Err(e)
                }
                Poll::Pending => {
                    self.handle = Some(handle);
                    Ok(Event::NeedConsume)
                }
            };
        }

        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(ref rf_source) = self.rf_source {
            if !self.rf_waited {
                self.rf_waited = true;

                log::info!("RUNTIME-FILTER: waiting for runtime filters");

                let abort_notify = self.ctx.get_abort_notify();
                rf_source.wait_ready(abort_notify).await;

                // Read partition filters from the source
                self.partition_filters = rf_source.get_partition_filters();
            }
        }
        Ok(())
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}
