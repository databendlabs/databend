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
use std::fmt::Debug;
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
    Spilled(Location),
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
            current_page: Some(PageBuilder::new(page_size)),
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
            let page_builder = self.current_page.as_mut().unwrap();

            let remain = page_builder.try_append_block(block);
            if !page_builder.has_capacity() {
                let rows = page_builder.num_rows();

                if self.pages_rows() + rows > self.max_size {
                    return Err(SendFail::Full {
                        page: self.current_page.take().unwrap().into_page(),
                        remain,
                    });
                }
                self.pages.push_back(Page::Memory(
                    self.current_page
                        .replace(PageBuilder::new(self.page_size))
                        .unwrap()
                        .into_page(),
                ));
            }
            match remain {
                Some(remain) => block = remain,
                None => return Ok(()),
            }
        }
    }

    fn try_add_page(&mut self, page: Page) -> result::Result<(), SendFail> {
        if self.is_recv_stopped || self.is_send_stopped {
            return Err(SendFail::Closed);
        }

        assert!(self.current_page.is_none());

        match page {
            page @ Page::Spilled(_) => self.pages.push_back(page),
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
        };

        self.current_page = Some(PageBuilder::new(self.page_size));
        Ok(())
    }

    fn stop_send(&mut self) {
        self.is_send_stopped = true
    }

    fn stop_recv(&mut self) {
        self.is_recv_stopped = true
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

    fn num_rows(&self) -> usize {
        self.blocks.iter().map(DataBlock::num_rows).sum()
    }

    fn calculate_take_rows(&self, block: &DataBlock) -> (usize, usize) {
        let block_rows = block.num_rows();
        let memory_size = block
            .columns()
            .iter()
            .map(|entry| match entry {
                BlockEntry::Const(scalar, _, n) => *n * scalar.as_ref().memory_size(),
                BlockEntry::Column(column) => column.memory_size(),
            })
            .sum::<usize>();

        (
            min(
                self.remain_rows,
                if memory_size > self.remain_size {
                    (self.remain_size * block_rows) / memory_size
                } else {
                    block_rows
                },
            )
            .max(1),
            memory_size,
        )
    }

    fn try_append_block(&mut self, block: DataBlock) -> Option<DataBlock> {
        assert!(self.has_capacity());
        let (take_rows, memory_size) = self.calculate_take_rows(&block);
        let total = block.num_rows();
        if take_rows < total {
            self.remain_size = 0;
            self.remain_rows -= total;
            self.blocks.push(block.slice(0..take_rows));
            Some(block.slice(take_rows..total))
        } else {
            self.remain_size -= min(self.remain_size, memory_size);
            self.remain_rows -= total;

            self.blocks.push(block);
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

    async fn try_spill_page(&self, page: Vec<DataBlock>) -> Result<Page> {
        if !self.should_spill() {
            return Ok(Page::Memory(page));
        }
        let spiller = self.spiller.lock().unwrap().clone();
        let Some(spiller) = spiller.as_ref() else {
            return Ok(Page::Memory(page));
        };
        let location = spiller.merge_and_spill(page).await?;
        Ok(Page::Spilled(location))
    }

    #[async_backtrace::framed]
    async fn recv(&self) -> bool {
        loop {
            {
                let buffer = self.buffer.lock().unwrap();
                if !buffer.pages.is_empty() {
                    return true;
                }
                if buffer.is_send_stopped {
                    return false;
                }
            }
            self.notify_on_sent.notified().await;
        }
    }

    fn is_close(&self) -> bool {
        let buffer = self.buffer.lock().unwrap();
        buffer.is_send_stopped && buffer.pages.is_empty()
    }

    fn stop_recv(&self) {
        self.buffer.lock().unwrap().stop_recv();
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
    pub fn close(&mut self) -> Option<S> {
        self.chan.stop_recv();
        {
            let mut buffer = self.chan.buffer.lock().unwrap();
            buffer.current_page = None;
            buffer.pages.clear();
        }
        self.chan.spiller.lock().unwrap().take()
    }

    #[async_backtrace::framed]
    pub async fn next_page(&mut self, tp: &Wait) -> Result<(BlocksSerializer, bool)> {
        let page = match tp {
            Wait::Async => self.try_take_page().await?,
            Wait::Deadline(t) => {
                let d = match t.checked_duration_since(Instant::now()) {
                    Some(d) if !d.is_zero() => d,
                    _ => {
                        // timeout() will return Ok if the future completes immediately
                        return Ok((BlocksSerializer::empty(), self.chan.is_close()));
                    }
                };
                match tokio::time::timeout(d, self.chan.recv()).await {
                    Ok(true) => self.try_take_page().await?,
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

    #[fastrace::trace(name = "SizedChannelReceiver::try_take_page")]
    async fn try_take_page(&mut self) -> Result<Option<BlocksSerializer>> {
        let page = self.chan.buffer.lock().unwrap().take_page();
        let collector = match page {
            None => return Ok(None),
            Some(Page::Memory(page)) => {
                let mut collector = BlocksCollector::new();
                for block in page {
                    collector.append_block(block);
                }
                collector
            }
            Some(Page::Spilled(location)) => {
                let spiller = self.chan.spiller.lock().unwrap().clone().unwrap();
                let block = spiller.restore(&location).await?;
                let mut collector = BlocksCollector::new();
                collector.append_block(block);
                collector
            }
        };
        self.chan.notify_on_recv.notify_one();

        let format_settings = self
            .chan
            .format_settings
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default();
        Ok(Some(collector.into_serializer(format_settings)))
    }
}

pub struct SizedChannelSender<S> {
    chan: Arc<SizedChannel<S>>,
}

impl<S> SizedChannelSender<S>
where S: DataBlockSpill
{
    #[async_backtrace::framed]
    pub async fn send(&mut self, mut block: DataBlock) -> Result<bool> {
        loop {
            let result = self.chan.buffer.lock().unwrap().try_add_block(block);
            match result {
                Ok(_) => {
                    self.chan.notify_on_sent.notify_one();
                    return Ok(true);
                }
                Err(SendFail::Closed) => {
                    self.chan.notify_on_sent.notify_one();
                    return Ok(false);
                }
                Err(SendFail::Full { page, remain }) => {
                    let mut to_add = self.chan.try_spill_page(page).await?;
                    loop {
                        let result = self.chan.buffer.lock().unwrap().try_add_page(to_add);
                        match result {
                            Ok(_) => break,
                            Err(SendFail::Closed) => return Ok(false),
                            Err(SendFail::Full { page, .. }) => {
                                to_add = Page::Memory(page);
                                self.chan.notify_on_recv.notified().await;
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

    pub fn abort(&mut self) {
        self.chan.buffer.lock().unwrap().stop_send();
        self.chan.notify_on_sent.notify_one();
    }

    pub fn finish(self) {
        {
            let mut buffer = self.chan.buffer.lock().unwrap();
            let builder = buffer.current_page.take().unwrap();
            buffer.pages.push_back(Page::Memory(builder.into_page()));
            buffer.stop_send();
        }
        self.chan.notify_on_sent.notify_one();
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
    pub fn abort(self) {
        self.chan.buffer.lock().unwrap().stop_send();
        self.chan.notify_on_sent.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use databend_common_exception::ErrorCode;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::Number;
    use databend_common_expression::types::NumberType;
    use databend_common_expression::FromData;
    use databend_common_io::prelude::FormatSettings;
    use databend_common_pipeline_transforms::traits::DataBlockSpill;
    use databend_common_pipeline_transforms::traits::Location;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    use super::*;

    #[derive(Clone, Default)]
    struct MockSpiller {
        storage: Arc<Mutex<HashMap<String, DataBlock>>>,
    }

    #[async_trait::async_trait]
    impl DataBlockSpill for MockSpiller {
        async fn spill(&self, data_block: DataBlock) -> Result<Location> {
            let key = format!("block_{}", rand::random::<u64>());
            self.storage.lock().unwrap().insert(key.clone(), data_block);
            Ok(Location::Remote(key))
        }

        async fn merge_and_spill(&self, data_block: Vec<DataBlock>) -> Result<Location> {
            self.spill(DataBlock::concat(&data_block)?).await
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
        let (mut sender, mut receiver) = sized_spsc::<MockSpiller>(5, 4);

        let test_data = vec![
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2, 3])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![4, 5, 6])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![7, 8, 9])]),
        ];

        let sender_data = test_data.clone();
        let send_task = databend_common_base::runtime::spawn(async move {
            let format_settings = FormatSettings::default();
            let spiller = MockSpiller::default();
            sender.plan_ready(format_settings, Some(spiller));

            for block in sender_data {
                sender.send(block).await.unwrap();
            }
            sender.finish();
        });

        let mut received_blocks_size = Vec::new();
        loop {
            let (serializer, is_end) = receiver.next_page(&Wait::Async).await.unwrap();

            if serializer.num_rows() > 0 {
                received_blocks_size.push(serializer.num_rows());
            }

            if is_end {
                break;
            }
        }

        let storage = receiver.close().unwrap().storage;
        assert_eq!(storage.lock().unwrap().len(), 1);

        send_task.await.unwrap();
        assert_eq!(received_blocks_size, vec![4, 4, 1]);
    }

    fn data_block_strategy<N>(len: usize) -> impl Strategy<Value = DataBlock>
    where
        N: Number + Arbitrary + 'static,
        NumberType<N>: FromData<N>,
    {
        prop::collection::vec(any::<N>(), len)
            .prop_map(|data| DataBlock::new_from_columns(vec![NumberType::<N>::from_data(data)]))
    }

    fn data_block_vec_strategy() -> impl Strategy<Value = Vec<DataBlock>> {
        let num_rows_strategy = 0..30_usize;
        let num_blocks_strategy = 0..100_usize;
        (num_blocks_strategy, num_rows_strategy).prop_flat_map(|(num_blocks, num_rows)| {
            prop::collection::vec(data_block_strategy::<i32>(num_rows), num_blocks)
        })
    }

    async fn delay() {
        if rand::thread_rng().gen_bool(0.7) {
            let delay = rand::thread_rng().gen_range(0..1000);
            tokio::time::sleep(Duration::from_micros(delay)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sized_spsc_channel_fuzz() {
        let mut runner = TestRunner::default();
        for _ in 0..100 {
            let (has_spiller, max_size, page_size, test_data) = (
                any::<bool>(),
                10_usize..20,
                5_usize..8,
                data_block_vec_strategy(),
            )
                .new_tree(&mut runner)
                .unwrap()
                .current();

            let (mut sender, mut receiver) = sized_spsc::<MockSpiller>(max_size, page_size);

            let sender_data = test_data.clone();
            let send_task = databend_common_base::runtime::spawn(async move {
                let format_settings = FormatSettings::default();
                sender.plan_ready(format_settings, has_spiller.then(MockSpiller::default));

                for block in sender_data {
                    delay().await;
                    sender.send(block).await.unwrap();
                    delay().await;
                }
                sender.finish();
            });

            let mut received_blocks_size = Vec::new();
            loop {
                delay().await;
                let wait = Wait::Deadline(Instant::now() + Duration::from_millis(50));
                let (serializer, is_end) = receiver.next_page(&wait).await.unwrap();
                if !serializer.is_empty() {
                    received_blocks_size.push(serializer.num_rows());
                }
                if is_end {
                    break;
                }
            }

            send_task.await.unwrap();

            if has_spiller {
                let _ = receiver.close().unwrap().storage;
            } else {
                assert!(receiver.close().is_none());
            }

            let total = test_data.iter().map(|data| data.num_rows()).sum::<usize>();

            assert_eq!(received_blocks_size.iter().sum::<usize>(), total);
        }
    }
}
