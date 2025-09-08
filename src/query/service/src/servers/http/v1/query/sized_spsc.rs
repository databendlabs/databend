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

use std::cmp::min;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::result;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::Notify;
use databend_common_base::base::WatchNotify;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::FormatSettings;
use databend_common_pipeline_transforms::traits::DataBlockSpill;
use databend_common_pipeline_transforms::traits::Location;
use log::debug;
use log::info;

use super::blocks_serializer::BlocksCollector;
use super::blocks_serializer::BlocksSerializer;

pub fn sized_spsc<S>(
    max_size: usize,
    page_size: usize,
) -> (SizedChannelSender<S>, SizedChannelReceiver<S>)
where
    S: DataBlockSpill,
{
    let chan = Arc::new(SizedChannel::create(max_size, page_size));
    let sender = SizedChannelSender { chan: chan.clone() };
    let receiver = SizedChannelReceiver { chan };
    (sender, receiver)
}

enum Page {
    Memory(Vec<DataBlock>),
    Spilled(SpillableBlock),
}

struct SizedChannelBuffer {
    max_size: usize,
    page_size: usize,
    pages: VecDeque<Page>,
    current_page: Option<PageBuilder>,
    is_recv_stopped: bool,
    is_send_stopped: bool,
}

impl SizedChannelBuffer {
    fn create(max_size: usize, page_size: usize) -> Self {
        SizedChannelBuffer {
            max_size,
            page_size,
            pages: Default::default(),
            current_page: None,
            is_recv_stopped: false,
            is_send_stopped: false,
        }
    }

    fn pages_rows(&self) -> usize {
        self.pages
            .iter()
            .map(|page| match page {
                Page::Memory(blocks) => blocks.iter().map(DataBlock::num_rows).sum::<usize>(),
                Page::Spilled(_) => 0,
            })
            .sum::<usize>()
    }

    fn try_add_block(&mut self, mut block: DataBlock) -> result::Result<(), SendFail> {
        if self.is_recv_stopped || self.is_send_stopped {
            return Err(SendFail::Closed);
        }

        loop {
            let page_builder = self
                .current_page
                .get_or_insert_with(|| PageBuilder::new(self.page_size));

            let remain = page_builder.try_append_block(block);
            if !page_builder.has_capacity() {
                let rows = page_builder.num_rows();
                let page = self.current_page.take().unwrap().into_page();
                if self.pages_rows() + rows > self.max_size {
                    return Err(SendFail::Full { page, remain });
                }
                self.pages.push_back(Page::Memory(page));
            }
            match remain {
                Some(remain) => block = remain,
                None => return Ok(()),
            }
        }
    }

    fn try_add_page(&mut self, page: Page) -> result::Result<(), SendFail> {
        assert!(self.current_page.is_none());

        if self.is_recv_stopped || self.is_send_stopped {
            return Err(SendFail::Closed);
        }

        match page {
            Page::Memory(blocks) => {
                let rows = blocks.iter().map(DataBlock::num_rows).sum::<usize>();
                if self.pages_rows() + rows > self.max_size {
                    return Err(SendFail::Full {
                        page: blocks,
                        remain: None,
                    });
                };
                self.pages.push_back(Page::Memory(blocks))
            }
            page @ Page::Spilled(_) => self.pages.push_back(page),
        };
        Ok(())
    }

    fn is_close(&self) -> bool {
        self.current_page
            .as_ref()
            .map(PageBuilder::is_empty)
            .unwrap_or(true)
            && self.pages.is_empty()
            && self.is_send_stopped
    }

    fn stop_send(&mut self) {
        self.is_send_stopped = true
    }

    fn stop_recv(&mut self) {
        self.is_recv_stopped = true
    }

    fn flush_current_page(&mut self) {
        if let Some(page_builder) = self.current_page.take() {
            let blocks = page_builder.into_page();
            if !blocks.is_empty() {
                self.pages.push_back(Page::Memory(blocks));
            }
        }
    }

    fn take_page(&mut self) -> Option<Page> {
        self.pages.pop_front()
    }
}

enum SendFail {
    Closed,
    Full {
        page: Vec<DataBlock>,
        remain: Option<DataBlock>,
    },
}

pub struct PageBuilder {
    pub blocks: Vec<DataBlock>,
    pub remain_rows: usize,
    pub remain_size: usize,
}

impl PageBuilder {
    fn new(max_rows: usize) -> Self {
        Self {
            blocks: Vec::new(),
            remain_size: 10 * 1024 * 1024,
            remain_rows: max_rows,
        }
    }

    fn has_capacity(&self) -> bool {
        self.remain_rows > 0 && self.remain_size > 0
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn num_rows(&self) -> usize {
        self.blocks.iter().map(DataBlock::num_rows).sum()
    }

    fn calculate_take_rows(&self, block: &DataBlock) -> usize {
        let block_rows = block.num_rows();
        let memory_size = block
            .columns()
            .iter()
            .map(|entry| match entry {
                BlockEntry::Const(scalar, _, n) => *n * scalar.as_ref().memory_size(),
                BlockEntry::Column(column) => column.memory_size(),
            })
            .sum::<usize>();

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

    fn append_full_block(&mut self, block: DataBlock) {
        let memory_size = block.memory_size();
        let num_rows = block.num_rows();

        self.remain_size -= min(self.remain_size, memory_size);
        self.remain_rows -= num_rows;

        self.blocks.push(block);
    }

    fn try_append_block(&mut self, block: DataBlock) -> Option<DataBlock> {
        let take_rows = self.calculate_take_rows(&block);
        let total = block.num_rows();
        if take_rows < block.num_rows() {
            self.remain_rows = 0;
            self.append_full_block(block.slice(0..take_rows));
            Some(block.slice(take_rows..total))
        } else {
            self.append_full_block(block);
            None
        }
    }

    fn into_page(self) -> Vec<DataBlock> {
        self.blocks
    }
}

struct SizedChannel<S> {
    buffer: Mutex<SizedChannelBuffer>,
    notify_on_sent: Notify,
    notify_on_recv: Notify,

    is_plan_ready: WatchNotify,
    format_settings: Mutex<Option<FormatSettings>>,
    spiller: Mutex<Option<S>>,
}

impl<S> SizedChannel<S>
where S: DataBlockSpill
{
    fn create(max_size: usize, page_size: usize) -> Self {
        SizedChannel {
            buffer: Mutex::new(SizedChannelBuffer::create(max_size, page_size)),
            notify_on_sent: Default::default(),
            notify_on_recv: Default::default(),
            is_plan_ready: WatchNotify::new(),
            format_settings: Mutex::new(None),
            spiller: Mutex::new(None),
        }
    }

    #[fastrace::trace(name = "SizedChannel::try_take_page")]
    async fn try_take_page(&self) -> Result<Option<BlocksSerializer>> {
        let format_settings = self
            .format_settings
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .unwrap_or_default();
        let page = self.buffer.lock().unwrap().take_page();
        match page {
            Some(Page::Memory(blocks)) => {
                let mut collector = BlocksCollector::new();
                for block in blocks {
                    collector.append_block(block);
                }
                Ok(Some(collector.into_serializer(format_settings)))
            }
            Some(Page::Spilled(_)) => {
                // TODO: Implement restoration from spilled page
                unimplemented!("Restoration from spilled page not implemented yet")
            }
            None => Ok(None),
        }
    }

    #[async_backtrace::framed]
    async fn send(&self, mut block: DataBlock) -> Result<bool> {
        loop {
            let result = self.buffer.lock().unwrap().try_add_block(block);
            match result {
                Ok(_) => {
                    self.notify_on_sent.notify_one();
                    return Ok(true);
                }
                Err(SendFail::Closed) => {
                    self.notify_on_sent.notify_one();
                    return Ok(false);
                }
                Err(SendFail::Full { page, remain }) => {
                    let mut to_add = self.try_spill_page(page).await?;
                    loop {
                        let result = self.buffer.lock().unwrap().try_add_page(to_add);
                        match result {
                            Ok(_) => break,
                            Err(SendFail::Closed) => return Ok(false),
                            Err(SendFail::Full { page, .. }) => {
                                to_add = Page::Memory(page);
                                self.notify_on_recv.notified().await;
                            }
                        }
                    }

                    match remain {
                        Some(remain) => {
                            block = remain;
                        }
                        None => return Ok(true),
                    }
                }
            }
        }
    }

    async fn try_spill_page(&self, page: Vec<DataBlock>) -> Result<Page> {
        if !self.should_spill() {
            return Ok(Page::Memory(page));
        }
        let spiller = self.spiller.lock().unwrap().clone();
        let Some(spiller) = spiller.as_ref() else {
            return Ok(Page::Memory(page));
        };
        let spilled = SpillableBlock::spill_page(page, spiller).await?;
        Ok(Page::Spilled(spilled))
    }

    #[async_backtrace::framed]
    async fn recv(&self) -> bool {
        loop {
            {
                let g = self.buffer.lock().unwrap();
                if !g.pages.is_empty() {
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
        self.buffer.lock().unwrap().is_close()
    }

    pub fn stop_send(&self) {
        {
            let mut guard = self.buffer.lock().unwrap();
            guard.flush_current_page();
            guard.stop_send()
        }
        self.notify_on_sent.notify_one();
    }

    pub fn stop_recv(&self) {
        {
            let mut guard = self.buffer.lock().unwrap();
            guard.stop_recv()
        }
        self.notify_on_recv.notify_one();
    }

    fn should_spill(&self) -> bool {
        true
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Wait {
    Async,
    Deadline(Instant),
}

pub struct SizedChannelReceiver<S> {
    chan: Arc<SizedChannel<S>>,
}

impl<S> SizedChannelReceiver<S>
where S: DataBlockSpill
{
    pub fn close(&self) {
        self.chan.stop_recv();
        self.chan.spiller.lock().unwrap().take(); // release session
    }

    #[async_backtrace::framed]
    pub async fn collect_new_page(
        &mut self,
        _max_rows_per_page: usize, // Now ignored since pages are pre-built
        tp: &Wait,
    ) -> Result<(BlocksSerializer, bool)> {
        let page = match tp {
            Wait::Async => self.chan.try_take_page().await?,
            Wait::Deadline(t) => {
                let d = match t.checked_duration_since(Instant::now()) {
                    Some(d) if !d.is_zero() => d,
                    _ => {
                        // timeout() will return Ok if the future completes immediately
                        return Ok((BlocksSerializer::empty(), self.chan.is_close()));
                    }
                };
                match tokio::time::timeout(d, self.chan.recv()).await {
                    Ok(true) => self.chan.try_take_page().await?,
                    Ok(false) => {
                        info!("[HTTP-QUERY] Reached end of data blocks");
                        return Ok((BlocksSerializer::empty(), true));
                    }
                    Err(_) => {
                        debug!("[HTTP-QUERY] Long polling timeout reached");
                        return Ok((BlocksSerializer::empty(), self.chan.is_close()));
                    }
                }
            }
        };

        let page = match page {
            Some(page) => page,
            None => BlocksSerializer::empty(),
        };

        // try to report 'no more data' earlier to client to avoid unnecessary http call
        let block_end = self.chan.is_close();
        Ok((page, block_end))
    }
}

#[derive(Clone)]
pub struct SizedChannelSender<S> {
    chan: Arc<SizedChannel<S>>,
}

impl<S> SizedChannelSender<S>
where S: DataBlockSpill
{
    #[async_backtrace::framed]
    pub async fn send(&self, value: DataBlock) -> Result<bool> {
        self.chan.send(value).await
    }

    pub fn close(&self) {
        self.chan.stop_send()
    }

    pub fn closer(&self) -> SizedChannelSenderCloser<S> {
        SizedChannelSenderCloser {
            chan: self.chan.clone(),
        }
    }

    pub fn plan_ready(&mut self, format_settings: FormatSettings, spiller: Option<S>) {
        assert!(!self.chan.is_plan_ready.has_notified());
        *self.chan.format_settings.lock().unwrap() = Some(format_settings);
        *self.chan.spiller.lock().unwrap() = spiller;
        self.chan.is_plan_ready.notify_waiters();
    }
}

pub struct SizedChannelSenderCloser<S> {
    chan: Arc<SizedChannel<S>>,
}

impl<S> SizedChannelSenderCloser<S>
where S: DataBlockSpill
{
    pub fn close(self) {
        self.chan.stop_send()
    }
}

struct SpillableBlock {
    data: Option<DataBlock>,
    /// [SpillableBlock::slice] does not reallocate memory, so memorysize remains unchanged
    memory_size: usize,
    rows: usize,
    location: Option<Location>,
    processed: usize,
}

impl SpillableBlock {
    async fn spill_page(page: Vec<DataBlock>, spiller: &impl DataBlockSpill) -> Result<Self> {
        let rows = page.iter().map(DataBlock::num_rows).sum();
        let memory_size = page.iter().map(DataBlock::memory_size).sum();
        let location = spiller.merge_and_spill(page).await?;
        Ok(Self {
            location: Some(location),
            processed: 0,
            rows,
            memory_size,
            data: None,
        })
    }

    fn slice(&mut self, pos: usize) -> DataBlock {
        let data = self.data.as_ref().unwrap();

        let left = data.slice(0..pos);
        let right = data.slice(pos..data.num_rows());

        self.rows = right.num_rows();
        self.data = Some(right);
        if self.location.is_some() {
            self.processed += pos;
        }
        left
    }

    async fn spill<S>(&mut self, spiller: &S) -> Result<()>
    where S: DataBlockSpill {
        let data = self.data.take().unwrap();
        if self.location.is_none() {
            let location = spiller.spill(data).await?;
            self.location = Some(location);
        }
        Ok(())
    }

    fn restore(&mut self, location: &Location, data: DataBlock) {
        assert_eq!(self.location.as_ref(), Some(location));
        self.data = Some(data)
    }
}

impl Debug for SpillableBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillableBlock")
            .field("has_data", &self.data.is_some())
            .field("memory_size", &self.memory_size)
            .field("rows", &self.rows)
            .field("location", &self.location)
            .field("processed", &self.processed)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use databend_common_exception::ErrorCode;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::FromData;
    use databend_common_io::prelude::FormatSettings;
    use databend_common_pipeline_transforms::traits::DataBlockSpill;
    use databend_common_pipeline_transforms::traits::Location;

    use super::*;

    #[derive(Clone)]
    struct MockSpiller {
        storage: Arc<Mutex<HashMap<String, DataBlock>>>,
    }

    impl MockSpiller {
        fn new() -> Self {
            Self {
                storage: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl DataBlockSpill for MockSpiller {
        async fn spill(&self, data_block: DataBlock) -> Result<Location> {
            let key = format!("block_{}", rand::random::<u64>());
            self.storage.lock().unwrap().insert(key.clone(), data_block);
            Ok(Location::Remote(key))
        }

        async fn merge_and_spill(&self, _data_block: Vec<DataBlock>) -> Result<Location> {
            unimplemented!()
        }

        async fn restore(&self, location: &Location) -> Result<DataBlock> {
            match location {
                Location::Remote(key) => {
                    let storage = self.storage.lock().unwrap();
                    storage
                        .get(key)
                        .cloned()
                        .ok_or_else(|| ErrorCode::Internal("Block not found in mock spiller"))
                }
                _ => Err(ErrorCode::Internal("Unsupported location type")),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sized_spsc_channel() {
        let (mut sender, mut receiver) = sized_spsc::<MockSpiller>(2, 10);

        let test_data = vec![
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2, 3])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![4, 5, 6])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![7, 8, 9])]),
        ];

        let sender_data = test_data.clone();
        let send_task = databend_common_base::runtime::spawn(async move {
            let format_settings = FormatSettings::default();
            let spiller = MockSpiller::new();
            sender.plan_ready(format_settings, Some(spiller));

            for block in sender_data {
                sender.send(block).await.unwrap();
            }
            sender.close();
        });

        let mut received_blocks = Vec::new();
        loop {
            let (serializer, is_end) = receiver.collect_new_page(10, &Wait::Async).await.unwrap();

            if serializer.num_rows() > 0 {
                received_blocks.push(serializer.num_rows());
            }

            if is_end {
                break;
            }
        }

        send_task.await.unwrap();

        let expected_total_rows: usize = test_data.iter().map(|b| b.num_rows()).sum();
        let received_total_rows: usize = received_blocks.iter().sum();

        assert_eq!(expected_total_rows, received_total_rows);
        assert!(!received_blocks.is_empty());
    }
}
