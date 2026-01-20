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

use databend_common_base::base::WatchNotify;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::FormatSettings;
use databend_common_pipeline_transforms::traits::DataBlockSpill;
use databend_common_pipeline_transforms::traits::Location;
use log::debug;
use log::info;
use tokio::sync::Notify;

use super::blocks_serializer::BlocksCollector;
use super::blocks_serializer::BlocksSerializer;

pub fn sized_spsc<S>(
    max_size: usize,
    page_size: usize,
) -> (SizedChannelSender<S>, SizedChannelReceiver<S>)
where
    S: DataBlockSpill,
{
    let chan = Arc::new(SizedChannel::new(max_size, page_size));
    let sender = SizedChannelSender { chan: chan.clone() };
    let receiver = SizedChannelReceiver { chan };
    (sender, receiver)
}

enum Page {
    Memory(Vec<DataBlock>),
    Spilled(Location),
}

struct SizedChannelBuffer {
    max_rows: usize,
    page_rows: usize,
    pages: VecDeque<Page>,

    /// The current_page gets moved outside the lock to spilling and then moved back in, or cleared on close.
    /// There's a lot of unwrap here to make sure there's no unintended behavior that's not by design.
    current_page: Option<PageBuilder>,
    is_recv_stopped: bool,
    is_send_stopped: bool,
}

impl SizedChannelBuffer {
    fn new(max_rows: usize, page_rows: usize) -> Self {
        SizedChannelBuffer {
            max_rows: max_rows.max(page_rows),
            page_rows,
            pages: Default::default(),
            current_page: Some(PageBuilder::new(page_rows)),
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
            .sum()
    }

    fn is_pages_full(&self, reserve: usize) -> bool {
        self.pages_rows() + reserve > self.max_rows
    }

    fn try_add_block(&mut self, mut block: DataBlock) -> result::Result<(), SendFail> {
        if self.is_recv_stopped || self.is_send_stopped {
            return Err(SendFail::Closed);
        }

        loop {
            let page_builder = self.current_page.as_mut().expect("current_page has taken");

            let remain = page_builder.try_append_block(block);
            if !page_builder.has_capacity() {
                let rows = page_builder.num_rows();
                if self.is_pages_full(rows) {
                    return Err(SendFail::Full {
                        page: self
                            .current_page
                            .take()
                            .expect("current_page has taken")
                            .into_page(),
                        remain,
                    });
                }
                let page = self
                    .current_page
                    .replace(PageBuilder::new(self.page_rows))
                    .expect("current_page has taken")
                    .into_page();
                self.pages.push_back(Page::Memory(page));
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
            Page::Memory(page) => {
                let rows = page.iter().map(DataBlock::num_rows).sum();
                if self.is_pages_full(rows) {
                    return Err(SendFail::Full { page, remain: None });
                };
                self.pages.push_back(Page::Memory(page))
            }
        };

        self.current_page = Some(PageBuilder::new(self.page_rows));
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

struct PageBuilder {
    blocks: Vec<DataBlock>,
    remain_rows: usize,
    remain_size: usize,
}

impl PageBuilder {
    fn new(max_rows: usize) -> Self {
        Self {
            blocks: Vec::new(),
            remain_size: 4 * 1024 * 1024,
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
                BlockEntry::Column(column) => column.memory_size(true),
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
        let total_rows = block.num_rows();
        if take_rows < total_rows {
            self.remain_size = 0;
            self.remain_rows -= take_rows;
            self.blocks.push(block.slice(0..take_rows));
            Some(block.slice(take_rows..total_rows))
        } else {
            self.remain_size -= min(self.remain_size, memory_size);
            self.remain_rows -= total_rows;
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
    fn new(max_rows: usize, page_rows: usize) -> Self {
        SizedChannel {
            buffer: Mutex::new(SizedChannelBuffer::new(max_rows, page_rows)),
            notify_on_sent: Default::default(),
            notify_on_recv: Default::default(),
            is_plan_ready: WatchNotify::new(),
            format_settings: Default::default(),
            spiller: Default::default(),
        }
    }

    async fn try_spill_page(&self, page: Vec<DataBlock>) -> Result<Page> {
        let rows_count = page.iter().map(|b| b.num_rows()).sum::<usize>();
        let memory_bytes = page.iter().map(|b| b.memory_size()).sum::<usize>();

        if !self.should_spill() {
            log::info!(
                target: "result-set-spill",
                "[RESULT-SET-SPILL] Spill disabled, page kept in memory blocks={}, rows={}, memory_bytes={}",
                page.len(), rows_count, memory_bytes
            );
            return Ok(Page::Memory(page));
        }
        let spiller = self.spiller.lock().unwrap().clone();
        let Some(spiller) = spiller.as_ref() else {
            log::warn!(
                target: "result-set-spill",
                "[RESULT-SET-SPILL] No spiller configured, page kept in memory blocks={}, rows={}, memory_bytes={}",
                page.len(), rows_count, memory_bytes
            );
            return Ok(Page::Memory(page));
        };

        let start_time = std::time::Instant::now();

        log::info!(
            target: "result-set-spill",
            "[RESULT-SET-SPILL] Starting spill to disk blocks={}, rows={}, memory_bytes={}",
            page.len(), rows_count, memory_bytes
        );

        let location = match spiller.merge_and_spill(page).await {
            Ok(location) => location,
            Err(e) => {
                log::error!(
                    target: "result-set-spill",
                    "[RESULT-SET-SPILL] Spill failed error={:?}",
                    e
                );
                return Err(e);
            }
        };
        let duration_ms = start_time.elapsed().as_millis();

        log::info!(
            target: "result-set-spill",
            "[RESULT-SET-SPILL] Spill completed rows={}, duration_ms={}, location={:?}",
            rows_count, duration_ms, location
        );

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

    fn should_spill(&self) -> bool {
        // todo: expected to be controlled externally
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
        {
            let mut buffer = self.chan.buffer.lock().unwrap();
            buffer.stop_recv();
            buffer.current_page = None;
            buffer.pages.clear();
        }
        let spiller = self.chan.spiller.lock().unwrap().take();
        self.chan.notify_on_recv.notify_one();
        spiller
    }

    #[async_backtrace::framed]
    pub async fn next_page(&mut self, tp: &Wait) -> Result<(BlocksSerializer, bool)> {
        let page = match tp {
            Wait::Async => self.try_take_page().await?,
            Wait::Deadline(t) => {
                match tokio::time::timeout_at((*t).into(), self.chan.recv()).await {
                    Ok(true) => self.try_take_page().await?,
                    Ok(false) => {
                        info!("Reached end of data blocks");
                        return Ok((BlocksSerializer::empty(), true));
                    }
                    Err(_) => {
                        debug!("Long polling timeout reached");
                        return Ok((BlocksSerializer::empty(), self.chan.is_close()));
                    }
                }
            }
        };

        // try to report 'no more data' earlier to client to avoid unnecessary http call
        Ok((
            page.unwrap_or_else(BlocksSerializer::empty),
            self.chan.is_close(),
        ))
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
                let start_time = std::time::Instant::now();

                log::info!(
                    target: "result-set-spill",
                    "[RESULT-SET-SPILL] Restoring from disk location={:?}",
                    location
                );

                let spiller = self.chan.spiller.lock().unwrap().clone().unwrap();
                let block = spiller.restore(&location).await?;
                let rows_count = block.num_rows();
                let memory_bytes = block.memory_size();
                let duration_ms = start_time.elapsed().as_millis();

                log::info!(
                    target: "result-set-spill",
                    "[RESULT-SET-SPILL] Restore completed rows={}, memory_bytes={}, duration_ms={}",
                    rows_count, memory_bytes, duration_ms
                );

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
                Err(SendFail::Closed) => return Ok(false),
                Ok(_) => {
                    self.chan.notify_on_sent.notify_one();
                    return Ok(true);
                }
                Err(SendFail::Full { page, remain }) => {
                    let page_rows = page.iter().map(|b| b.num_rows()).sum::<usize>();

                    log::info!(
                        target: "result-set-spill",
                        "[RESULT-SET-SPILL] Buffer full page_blocks={}, page_rows={}, has_remain={}",
                        page.len(), page_rows, remain.is_some()
                    );

                    self.chan.notify_on_sent.notify_one();
                    let mut to_add = self.chan.try_spill_page(page).await?;
                    loop {
                        let result = self.chan.buffer.lock().unwrap().try_add_page(to_add);
                        match result {
                            Err(SendFail::Closed) => return Ok(false),
                            Ok(_) => {
                                self.chan.notify_on_sent.notify_one();
                                break;
                            }
                            Err(SendFail::Full { page, .. }) => {
                                to_add = Page::Memory(page);
                                self.chan.notify_on_recv.notified().await;
                            }
                        }
                    }

                    let Some(remain) = remain else {
                        return Ok(true);
                    };
                    block = remain;
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
            if !buffer.is_recv_stopped && !buffer.is_send_stopped {
                let page = buffer
                    .current_page
                    .take()
                    .expect("current_page has taken")
                    .into_page();
                buffer.pages.push_back(Page::Memory(page));
            }
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
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::Number;
    use databend_common_expression::types::NumberType;
    use databend_common_io::prelude::FormatSettings;
    use databend_common_pipeline_transforms::traits::DataBlockSpill;
    use databend_common_pipeline_transforms::traits::Location;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use rand::Rng;

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
    async fn test_spsc_channel() {
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

        let _ = receiver.close().unwrap().storage;

        send_task.await.unwrap();
        assert_eq!(received_blocks_size, vec![4, 4, 1]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_spsc_slow() {
        let (mut sender, mut receiver) = sized_spsc::<MockSpiller>(5, 4);

        let test_data = vec![DataBlock::new_from_columns(vec![Int32Type::from_data(
            vec![1, 2, 3],
        )])];

        let wait = Arc::new(Notify::new());

        let sender_wait = wait.clone();
        let sender_data = test_data.clone();
        let send_task = databend_common_base::runtime::spawn(async move {
            let format_settings = FormatSettings::default();
            sender.plan_ready(format_settings, None);

            sender_wait.notified().await;

            for block in sender_data {
                sender.send(block).await.unwrap();
            }
            sender.finish();
        });

        for _ in 0..10 {
            let deadline = Instant::now() + Duration::from_millis(1);
            let (serializer, _) = receiver.next_page(&Wait::Deadline(deadline)).await.unwrap();
            assert_eq!(serializer.num_rows(), 0);
        }

        wait.notify_one();

        let (serializer, _) = receiver
            .next_page(&Wait::Deadline(Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap();
        let _ = receiver.close();
        send_task.await.unwrap();

        assert_eq!(serializer.num_rows(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_spsc_abort() {
        let (mut sender, mut receiver) = sized_spsc::<MockSpiller>(5, 4);

        let test_data = vec![
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2, 3])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![4, 5, 6, 7, 8])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![7, 8, 9])]),
        ];

        let sender_data = test_data.clone();
        let send_task = databend_common_base::runtime::spawn(async move {
            let format_settings = FormatSettings::default();
            sender.plan_ready(format_settings, None);

            for (i, block) in sender_data.into_iter().enumerate() {
                sender.send(block).await.unwrap();
                if i == 3 {
                    sender.abort();
                    return;
                }
            }
        });

        let mut received_blocks_size = Vec::new();
        loop {
            let (serializer, is_end) = receiver
                .next_page(&Wait::Deadline(Instant::now() + Duration::from_secs(1)))
                .await
                .unwrap();

            if serializer.num_rows() > 0 {
                received_blocks_size.push(serializer.num_rows());
            }

            if is_end {
                break;
            }
        }

        send_task.await.unwrap();

        assert_eq!(received_blocks_size, vec![4, 4])
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
    async fn test_spsc_channel_fuzz() {
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
