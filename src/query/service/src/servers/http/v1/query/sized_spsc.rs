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

//! A channel bounded with the sum of sizes associate with items instead of count of items.
//!
//! other features:
//! 1. it is SPSC, enough for now.
//! 2. receive can check status of channel with fn is_empty().

use std::cmp::min;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::Notify;
use databend_common_base::base::WatchNotify;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::FormatSettings;
use fastrace::future::FutureExt;
use fastrace::Span;
use log::debug;
use log::info;

use super::blocks_serializer::BlocksCollector;
use super::blocks_serializer::BlocksSerializer;

pub struct PageBuilder {
    pub collector: BlocksCollector,
    pub remain_rows: usize,
    pub remain_size: usize,
}

impl PageBuilder {
    fn new(max_rows: usize) -> Self {
        Self {
            collector: BlocksCollector::new(),
            remain_size: 10 * 1024 * 1024,
            remain_rows: max_rows,
        }
    }

    fn has_capacity(&self) -> bool {
        self.remain_rows > 0 && self.remain_size > 0
    }

    fn append_full_block(&mut self, block: DataBlock) {
        let memory_size = block.memory_size();
        let num_rows = block.num_rows();

        self.remain_size -= min(self.remain_size, memory_size);
        self.remain_rows -= num_rows;

        self.collector.append_block(block);
    }

    fn append_partial_block(&mut self, block: DataBlock, take_rows: usize) -> DataBlock {
        self.collector.append_block(block.slice(0..take_rows));

        block.slice(take_rows..block.num_rows())
    }

    fn calculate_take_rows(&self, block_rows: usize, memory_size: usize) -> usize {
        min(
            self.remain_rows,
            if memory_size > self.remain_size {
                (self.remain_size * block_rows) / memory_size
            } else {
                block_rows
            },
        )
        .max(1)
    }

    fn into_serializer(self, format_settings: FormatSettings) -> BlocksSerializer {
        self.collector.into_serializer(format_settings)
    }
}

struct SizedChannelInner {
    max_size: usize,
    values: VecDeque<DataBlock>,
    is_recv_stopped: bool,
    is_send_stopped: bool,
}

pub fn sized_spsc(max_size: usize) -> (SizedChannelSender, SizedChannelReceiver) {
    let chan = Arc::new(SizedChannel::create(max_size));
    let cloned = chan.clone();
    (SizedChannelSender { chan }, SizedChannelReceiver {
        chan: cloned,
    })
}

impl SizedChannelInner {
    fn create(max_size: usize) -> Self {
        SizedChannelInner {
            max_size,
            values: Default::default(),
            is_recv_stopped: false,
            is_send_stopped: false,
        }
    }

    fn size(&self) -> usize {
        self.values.iter().map(|x| x.num_rows()).sum::<usize>()
    }

    fn try_send(&mut self, value: DataBlock) -> Result<(), Option<DataBlock>> {
        let current_size = self.size();
        let value_size = value.num_rows();
        if self.is_recv_stopped || self.is_send_stopped {
            Err(None)
        } else if current_size + value_size <= self.max_size || current_size == 0 {
            self.values.push_back(value);
            Ok(())
        } else {
            Err(Some(value))
        }
    }

    fn is_close(&self) -> bool {
        self.values.is_empty() && self.is_send_stopped
    }

    fn stop_send(&mut self) {
        self.is_send_stopped = true
    }

    fn stop_recv(&mut self) {
        self.is_recv_stopped = true
    }

    fn take_block_once(&mut self, builder: &mut PageBuilder) {
        let Some(block) = self.values.pop_front() else {
            return;
        };

        if block.is_empty() {
            return;
        }

        let take_rows = builder.calculate_take_rows(block.num_rows(), block.memory_size());
        if take_rows < block.num_rows() {
            builder.remain_rows = 0;
            let remaining_block = builder.append_partial_block(block, take_rows);
            self.values.push_front(remaining_block);
        } else {
            builder.append_full_block(block);
        }
    }

    fn take_block(&mut self, builder: &mut PageBuilder) -> bool {
        while builder.has_capacity() && !self.values.is_empty() {
            self.take_block_once(builder)
        }
        !self.values.is_empty()
    }
}

struct SizedChannel {
    inner: Mutex<SizedChannelInner>,
    notify_on_sent: Notify,
    notify_on_recv: Notify,

    is_plan_ready: WatchNotify,
    format_settings: Mutex<Option<FormatSettings>>,
}

impl SizedChannel {
    fn create(max_size: usize) -> Self {
        SizedChannel {
            inner: Mutex::new(SizedChannelInner::create(max_size)),
            notify_on_sent: Default::default(),
            notify_on_recv: Default::default(),
            is_plan_ready: WatchNotify::new(),
            format_settings: Mutex::new(None),
        }
    }

    fn try_send(&self, value: DataBlock) -> Result<(), Option<DataBlock>> {
        self.inner.lock().unwrap().try_send(value)
    }

    #[fastrace::trace(name = "SizedChannel::try_take_block")]
    fn try_take_block(&self, builder: &mut PageBuilder) -> bool {
        let has_more = self.inner.lock().unwrap().take_block(builder);
        self.notify_on_recv.notify_one();
        has_more
    }

    #[async_backtrace::framed]
    async fn send(&self, value: DataBlock) -> bool {
        let mut to_send = value;
        loop {
            match self.try_send(to_send) {
                Ok(_) => {
                    self.notify_on_sent.notify_one();
                    return true;
                }
                Err(None) => return false,
                Err(Some(v)) => {
                    to_send = v;
                    self.notify_on_recv.notified().await;
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn recv(&self) -> bool {
        loop {
            {
                let g = self.inner.lock().unwrap();
                if !g.values.is_empty() {
                    return true;
                }
                if g.is_send_stopped {
                    return false;
                }
            }
            self.notify_on_sent.notified().await;
        }
    }

    pub fn is_close(&self) -> bool {
        let guard = self.inner.lock().unwrap();
        guard.is_close()
    }

    pub fn stop_send(&self) {
        {
            let mut guard = self.inner.lock().unwrap();
            guard.stop_send()
        }
        self.notify_on_sent.notify_one();
    }

    pub fn stop_recv(&self) {
        {
            let mut guard = self.inner.lock().unwrap();
            guard.stop_recv()
        }
        self.notify_on_recv.notify_one();
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Wait {
    Async,
    Deadline(Instant),
}

pub struct SizedChannelReceiver {
    chan: Arc<SizedChannel>,
}

impl SizedChannelReceiver {
    pub fn close(&self) {
        self.chan.stop_recv()
    }

    #[async_backtrace::framed]
    pub async fn collect_new_page(
        &mut self,
        max_rows_per_page: usize,
        tp: &Wait,
    ) -> Result<(BlocksSerializer, bool), ErrorCode> {
        let mut builder = PageBuilder::new(max_rows_per_page);

        while builder.has_capacity() {
            match tp {
                Wait::Async => self.chan.try_take_block(&mut builder),
                Wait::Deadline(t) => {
                    let d = match t.checked_duration_since(Instant::now()) {
                        Some(d) if !d.is_zero() => d,
                        _ => {
                            // timeout() will return Ok if the future completes immediately
                            break;
                        }
                    };
                    match tokio::time::timeout(d, self.chan.recv()).await {
                        Ok(true) => {
                            self.chan.try_take_block(&mut builder);
                            debug!("[HTTP-QUERY] Appended new data block");
                        }
                        Ok(false) => {
                            info!("[HTTP-QUERY] Reached end of data blocks");
                            break;
                        }
                        Err(_) => {
                            debug!("[HTTP-QUERY] Long polling timeout reached");
                            break;
                        }
                    }
                }
            }
        }

        // try to report 'no more data' earlier to client to avoid unnecessary http call
        let block_end = self.chan.is_close();
        Ok((
            builder.into_serializer(self.chan.format_settings.lock().unwrap().clone().unwrap()),
            block_end,
        ))
    }
}

#[derive(Clone)]
pub struct SizedChannelSender {
    chan: Arc<SizedChannel>,
}

impl SizedChannelSender {
    #[async_backtrace::framed]
    pub async fn send(&self, value: DataBlock) -> bool {
        self.chan.send(value).await
    }

    pub fn close(&self) {
        self.chan.stop_send()
    }

    pub fn closer(&self) -> SizedChannelSenderCloser {
        SizedChannelSenderCloser {
            chan: self.chan.clone(),
        }
    }

    pub fn plan_ready(&self, format_settings: FormatSettings) {
        assert!(!self.chan.is_plan_ready.has_notified());
        *self.chan.format_settings.lock().unwrap() = Some(format_settings);
        self.chan.is_plan_ready.notify_waiters();
    }
}

pub struct SizedChannelSenderCloser {
    chan: Arc<SizedChannel>,
}

impl SizedChannelSenderCloser {
    pub fn close(&self) {
        self.chan.stop_send()
    }
}
