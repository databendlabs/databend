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
    max_batch_size: usize,
}

impl ReceiverPartitionStream {
    pub fn with_batch_size(receiver: Receiver<Result<PartInfoPtr>>, max_batch_size: usize) -> Self {
        Self {
            receiver,
            max_batch_size: max_batch_size.max(1),
        }
    }
}

#[async_trait::async_trait]
impl PartitionStream for ReceiverPartitionStream {
    async fn fetch(&self, _id: usize) -> Result<Option<Vec<PartInfoPtr>>> {
        let first = match self.receiver.recv().await {
            Ok(Ok(part)) => part,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(None),
        };

        let mut parts = Vec::with_capacity(self.max_batch_size);
        parts.push(first);
        while parts.len() < self.max_batch_size {
            match self.receiver.try_recv() {
                Ok(Ok(part)) => parts.push(part),
                Ok(Err(e)) => return Err(e),
                Err(_) => break,
            }
        }

        Ok(Some(parts))
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

        if let Some(mut waiter) = self.runtime_filter_waiter.take() {
            if waiter.ready.is_none() {
                let ready = waiter.ctx.get_runtime_filter_ready(waiter.scan_id);
                if !ready.is_empty() {
                    waiter.ready = Some(ready);
                    self.runtime_filter_waiter = Some(waiter);
                    return Ok(Event::Async);
                }
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

#[cfg(test)]
mod tests {
    use databend_common_exception::ErrorCode;

    use super::*;
    use crate::FuseLazyPartInfo;

    #[tokio::test]
    async fn test_receiver_partition_stream_batches_parts() -> Result<()> {
        let (tx, rx) = async_channel::unbounded();
        for index in 0..3 {
            tx.send(Ok(FuseLazyPartInfo::create(
                index,
                (format!("segment-{index}"), 1),
            )))
            .await
            .unwrap();
        }
        drop(tx);

        let stream = ReceiverPartitionStream::with_batch_size(rx, 2);
        assert_eq!(stream.fetch(0).await?.unwrap().len(), 2);
        assert_eq!(stream.fetch(0).await?.unwrap().len(), 1);
        assert!(stream.fetch(0).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_receiver_partition_stream_forwards_errors() {
        let (tx, rx) = async_channel::unbounded();
        tx.send(Err(ErrorCode::Internal("prune failed")))
            .await
            .unwrap();
        drop(tx);

        let stream = ReceiverPartitionStream::with_batch_size(rx, 2);
        assert!(stream.fetch(0).await.is_err());
    }

    #[tokio::test]
    async fn test_receiver_partition_stream_empty_channel_finishes() -> Result<()> {
        let (tx, rx) = async_channel::unbounded();
        drop(tx);

        let stream = ReceiverPartitionStream::with_batch_size(rx, 8);
        assert!(stream.fetch(0).await?.is_none());
        Ok(())
    }
}
