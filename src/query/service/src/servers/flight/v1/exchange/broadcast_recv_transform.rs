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

//! Receive side of broadcast and hash exchanges. Merges the blocks from its input
//! port (the local sender's own partition) with those from its InboundChannel.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Poll;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use petgraph::graph::NodeIndex;

use crate::servers::flight::v1::network::InboundChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;

pub struct ExchangeRecvTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    id: NodeIndex,
    tasks: SyncTaskSet,
    receiver: Arc<dyn InboundChannel>,
    handle: Option<SyncTaskHandle<'static, Result<Option<DataBlock>>>>,
    coalescer: BlockCoalescer,
}

impl ExchangeRecvTransform {
    pub fn create_item(
        worker_id: usize,
        receiver: Arc<dyn InboundChannel>,
        waker: Arc<ExecutorWaker>,
        coalesce: Option<CoalesceThreshold>,
    ) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        let processor = ProcessorPtr::create(Box::new(Self {
            receiver,
            input: input.clone(),
            output: output.clone(),
            tasks: SyncTaskSet::new(worker_id, waker),

            handle: None,
            id: Default::default(),
            coalescer: BlockCoalescer::create(coalesce),
        }));

        PipeItem::create(processor, vec![input], vec![output])
    }
}

impl Processor for ExchangeRecvTransform {
    fn name(&self) -> String {
        String::from("ExchangeRecvTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            self.receiver.close();

            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        loop {
            if let Some(data_block) = self.coalescer.pop() {
                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }

            if self.handle.is_none() && !self.receiver.is_closed() {
                let receiver = self.receiver.clone();
                let recv_fut = Box::pin(async move { receiver.recv().await });
                self.handle = Some(self.tasks.spawn(self.id, recv_fut));
            }

            if let Some(mut handle) = self.handle.take() {
                match handle.poll(matches!(cause, EventCause::Other)) {
                    Poll::Ready(Ok(None)) => {}
                    Poll::Ready(Ok(Some(data_block))) => {
                        self.coalescer.push(data_block)?;
                        continue;
                    }
                    Poll::Ready(Err(cause)) => {
                        self.receiver.close();
                        return Err(cause);
                    }
                    Poll::Pending => {
                        self.handle = Some(handle);
                    }
                };
            }

            if self.input.has_data() {
                let data_block = self.input.pull_data().unwrap()?;
                self.coalescer.push(data_block)?;
                continue;
            }

            if self.input.is_finished() && self.handle.is_none() {
                if self.coalescer.flush()? {
                    continue;
                }

                self.output.finish();
                self.receiver.close();
                return Ok(Event::Finished);
            }

            self.input.set_need_data();
            return Ok(Event::NeedData);
        }
    }

    fn details_status(&self) -> Option<String> {
        Some(format!(
            "handle_pending={}, coalescing_rows={}, coalescing_bytes={}",
            self.handle.is_some(),
            self.coalescer.rows,
            self.coalescer.bytes,
        ))
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}

/// Target size of the blocks a receiver outputs.
#[derive(Clone, Copy, Debug)]
pub struct CoalesceThreshold {
    pub rows: usize,
    pub bytes: usize,
}

impl CoalesceThreshold {
    pub fn create(rows: usize, bytes: usize) -> CoalesceThreshold {
        CoalesceThreshold {
            rows: if rows == 0 { usize::MAX } else { rows },
            bytes: if bytes == 0 { usize::MAX } else { bytes },
        }
    }
}

/// Concatenates small blocks up to a threshold. Blocks of at least half the
/// threshold, and blocks carrying meta, are passed through without copying.
pub(super) struct BlockCoalescer {
    threshold: Option<CoalesceThreshold>,
    blocks: Vec<DataBlock>,
    rows: usize,
    bytes: usize,
    ready: VecDeque<DataBlock>,
}

impl BlockCoalescer {
    pub(super) fn create(threshold: Option<CoalesceThreshold>) -> BlockCoalescer {
        BlockCoalescer {
            threshold,
            blocks: vec![],
            rows: 0,
            bytes: 0,
            ready: VecDeque::new(),
        }
    }

    pub(super) fn push(&mut self, block: DataBlock) -> Result<()> {
        let Some(threshold) = self.threshold else {
            self.ready.push_back(block);
            return Ok(());
        };

        if block.get_meta().is_some() || block.num_columns() == 0 {
            self.flush()?;
            self.ready.push_back(block);
            return Ok(());
        }

        if block.is_empty() {
            return Ok(());
        }

        let rows = block.num_rows();
        let bytes = block.memory_size();

        if rows.saturating_mul(2) >= threshold.rows || bytes.saturating_mul(2) >= threshold.bytes {
            self.flush()?;
            self.ready.push_back(block);
            return Ok(());
        }

        self.rows += rows;
        self.bytes += bytes;
        self.blocks.push(block);

        if self.rows >= threshold.rows || self.bytes >= threshold.bytes {
            self.flush()?;
        }

        Ok(())
    }

    /// Move the buffered blocks to the output queue. Returns whether any were moved.
    pub(super) fn flush(&mut self) -> Result<bool> {
        if self.blocks.is_empty() {
            return Ok(false);
        }

        let blocks = std::mem::take(&mut self.blocks);
        self.rows = 0;
        self.bytes = 0;

        self.ready.push_back(match blocks.len() {
            1 => blocks.into_iter().next().unwrap(),
            _ => DataBlock::concat(&blocks)?,
        });
        Ok(true)
    }

    pub(super) fn pop(&mut self) -> Option<DataBlock> {
        self.ready.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;

    use super::*;

    fn block(rows: usize) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32; rows])])
    }

    fn drain(coalescer: &mut BlockCoalescer) -> Vec<usize> {
        std::iter::from_fn(|| coalescer.pop())
            .map(|x| x.num_rows())
            .collect()
    }

    #[test]
    fn test_coalescer_merges_small_blocks() -> Result<()> {
        let mut coalescer =
            BlockCoalescer::create(Some(CoalesceThreshold::create(100, usize::MAX)));

        for _ in 0..7 {
            coalescer.push(block(30))?;
        }
        assert_eq!(drain(&mut coalescer), vec![120]);

        assert!(coalescer.flush()?);
        assert_eq!(drain(&mut coalescer), vec![90]);
        assert!(!coalescer.flush()?);
        Ok(())
    }

    #[test]
    fn test_coalescer_passes_large_blocks_through() -> Result<()> {
        let mut coalescer =
            BlockCoalescer::create(Some(CoalesceThreshold::create(100, usize::MAX)));

        coalescer.push(block(10))?;
        coalescer.push(block(50))?;
        coalescer.push(block(0))?;
        coalescer.push(block(10))?;
        coalescer.flush()?;
        assert_eq!(drain(&mut coalescer), vec![10, 50, 10]);
        Ok(())
    }

    #[test]
    fn test_coalescer_disabled() -> Result<()> {
        let mut coalescer = BlockCoalescer::create(None);
        coalescer.push(block(1))?;
        coalescer.push(block(2))?;
        assert!(!coalescer.flush()?);
        assert_eq!(drain(&mut coalescer), vec![1, 2]);
        Ok(())
    }
}
