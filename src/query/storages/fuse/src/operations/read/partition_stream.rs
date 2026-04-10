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
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
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
use crate::operations::read::runtime_filter_wait::wait_runtime_filters;

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

struct RuntimeFilterWaiter {
    ctx: Arc<dyn TableContext>,
    scan_id: IndexType,
    ready: Option<Vec<Arc<RuntimeFilterReady>>>,
}

pub struct PartitionStreamSource {
    id: NodeIndex,
    worker_id: usize,
    tasks: SyncTaskSet,
    output: Arc<OutputPort>,
    stream: Arc<dyn PartitionStream>,
    handle: Option<SyncTaskHandle<'static, Result<Option<Vec<PartInfoPtr>>>>>,
    runtime_filter_waiter: Option<RuntimeFilterWaiter>,
}

impl PartitionStreamSource {
    pub fn create(
        worker_id: usize,
        waker: Arc<ExecutorWaker>,
        output: Arc<OutputPort>,
        stream: Arc<dyn PartitionStream>,
        ctx: Arc<dyn TableContext>,
        scan_id: IndexType,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            output,
            stream,
            worker_id,
            id: Default::default(),
            tasks: SyncTaskSet::new(worker_id, waker),
            handle: None,
            runtime_filter_waiter: Some(RuntimeFilterWaiter {
                ctx,
                scan_id,
                ready: None,
            }),
        })))
    }

    fn close(&mut self) {
        self.stream = Arc::new(DummyPartitionStream);
        self.handle = None;
        self.runtime_filter_waiter = None;
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

        // Runtime filter wait phase
        if let Some(waiter) = &mut self.runtime_filter_waiter {
            if waiter.ready.is_none() {
                let ready = waiter.ctx.get_runtime_filter_ready(waiter.scan_id);
                if ready.is_empty() {
                    self.runtime_filter_waiter = None;
                } else {
                    waiter.ready = Some(ready);
                    return Ok(Event::Async);
                }
            } else {
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

        if let Some(mut handle) = self.handle.take() {
            return match handle.poll(matches!(cause, EventCause::Other)) {
                Poll::Ready(Ok(Some(parts))) => {
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
        if let Some(waiter) = self.runtime_filter_waiter.take() {
            if let Some(ready) = &waiter.ready {
                log::info!(
                    "RUNTIME-FILTER: scan_id={} waiting for {} runtime filters",
                    waiter.scan_id,
                    ready.len()
                );
                wait_runtime_filters(
                    waiter.scan_id,
                    &self.output,
                    waiter.ctx.get_abort_notify(),
                    ready,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}
