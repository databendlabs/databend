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

    fn into_serializer(self, format_settings: FormatSettings) -> BlocksSerializer {
        self.collector.into_serializer(format_settings)
    }
}

struct SizedChannelBuffer {
    max_size: usize,
    values: VecDeque<SpillableBlock>,
    is_recv_stopped: bool,
    is_send_stopped: bool,
}

pub fn sized_spsc<S>(max_size: usize) -> (SizedChannelSender<S>, SizedChannelReceiver<S>)
where S: DataBlockSpill {
    let chan = Arc::new(SizedChannel::create(max_size));
    let sender = SizedChannelSender { chan: chan.clone() };
    let receiver = SizedChannelReceiver { chan };
    (sender, receiver)
}

impl SizedChannelBuffer {
    fn create(max_size: usize) -> Self {
        SizedChannelBuffer {
            max_size,
            values: Default::default(),
            is_recv_stopped: false,
            is_send_stopped: false,
        }
    }

    fn size(&self) -> usize {
        self.values
            .iter()
            .map(|x| x.data.as_ref().map(DataBlock::num_rows).unwrap_or_default())
            .sum::<usize>()
    }

    fn try_send(&mut self, value: SpillableBlock) -> result::Result<(), Option<SpillableBlock>> {
        if self.is_recv_stopped || self.is_send_stopped {
            return Err(None);
        }

        match &value.data {
            Some(data) => {
                let value_size = data.num_rows();
                let current_size = self.size();
                if current_size + value_size <= self.max_size || current_size == 0 {
                    self.values.push_back(value);
                    Ok(())
                } else {
                    Err(Some(value))
                }
            }
            None => {
                self.values.push_back(value);
                Ok(())
            }
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

    fn take_block_once(&mut self, builder: &mut PageBuilder) -> result::Result<(), Location> {
        let Some(block) = self.values.front_mut() else {
            return Ok(());
        };
        let Some(data) = &block.data else {
            return Err(block.location.clone().unwrap());
        };

        let take_rows = builder.calculate_take_rows(data);
        if take_rows < data.num_rows() {
            builder.remain_rows = 0;
            builder.collector.append_block(block.slice(take_rows));
        } else {
            let data = block.data.take().unwrap();
            self.values.pop_front();
            builder.append_full_block(data);
        }
        Ok(())
    }

    fn take_block(&mut self, builder: &mut PageBuilder) -> result::Result<bool, Location> {
        while builder.has_capacity() && !self.values.is_empty() {
            self.take_block_once(builder)?;
        }
        Ok(!self.values.is_empty())
    }

    fn restore_first(&mut self, location: &Location, data: DataBlock) {
        self.values.front_mut().unwrap().restore(location, data);
    }
}

struct SizedChannel<S> {
    inner: Mutex<SizedChannelBuffer>,
    notify_on_sent: Notify,
    notify_on_recv: Notify,

    is_plan_ready: WatchNotify,
    format_settings: Mutex<Option<FormatSettings>>,
    spiller: Mutex<Option<S>>,
}

impl<S> SizedChannel<S>
where S: DataBlockSpill
{
    fn create(max_size: usize) -> Self {
        SizedChannel {
            inner: Mutex::new(SizedChannelBuffer::create(max_size)),
            notify_on_sent: Default::default(),
            notify_on_recv: Default::default(),
            is_plan_ready: WatchNotify::new(),
            format_settings: Mutex::new(None),
            spiller: Mutex::new(None),
        }
    }

    #[fastrace::trace(name = "SizedChannel::try_take_block")]
    async fn try_take_block(&self, builder: &mut PageBuilder) -> Result<bool> {
        let location = match self.inner.lock().unwrap().take_block(builder) {
            Err(location) => location.clone(),
            Ok(has_more) => {
                return Ok(has_more);
            }
        };

        let spiller = self.spiller.lock().unwrap().clone().unwrap();
        let data = spiller.restore(&location).await?;

        self.inner.lock().unwrap().restore_first(&location, data);
        Ok(true)
    }

    #[async_backtrace::framed]
    async fn send(&self, value: DataBlock) -> Result<bool> {
        let mut to_send = SpillableBlock::new(value);
        loop {
            let result = self.inner.lock().unwrap().try_send(to_send);
            match result {
                Ok(_) => {
                    self.notify_on_sent.notify_one();
                    return Ok(true);
                }
                Err(None) => return Ok(false),
                Err(Some(v)) => {
                    to_send = v;
                    if self.should_spill() {
                        let spiller = self.spiller.lock().unwrap().clone();
                        if let Some(spiller) = spiller.as_ref() {
                            to_send.spill(spiller).await?;
                            continue;
                        }
                    }
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
        max_rows_per_page: usize,
        tp: &Wait,
    ) -> Result<(BlocksSerializer, bool)> {
        let mut builder = PageBuilder::new(max_rows_per_page);

        while builder.has_capacity() {
            match tp {
                Wait::Async => {
                    if !self.chan.try_take_block(&mut builder).await? {
                        break;
                    }
                }
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
                            self.chan.try_take_block(&mut builder).await?;
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

        let format = self.chan.format_settings.lock().unwrap().clone();
        let format = format.unwrap_or_else(|| {
            assert!(builder.collector.num_rows() == 0);
            FormatSettings::default()
        });

        // try to report 'no more data' earlier to client to avoid unnecessary http call
        let block_end = self.chan.is_close();
        Ok((builder.into_serializer(format), block_end))
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
    fn new(data: DataBlock) -> Self {
        Self {
            location: None,
            processed: 0,
            rows: data.num_rows(),
            memory_size: data.memory_size(),
            data: Some(data),
        }
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
        let (mut sender, mut receiver) = sized_spsc::<MockSpiller>(2);

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
