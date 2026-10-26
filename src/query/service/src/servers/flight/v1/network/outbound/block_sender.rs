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

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use arrow_flight::FlightData;
use arrow_ipc::writer::IpcWriteOptions;
use async_channel::Sender;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_settings::FlightCompression;
use tokio::sync::Semaphore;

use super::super::inbound::exchange_queue::ExchangeReceiveChannels;
use super::super::inbound::exchange_queue::LocalQueueItem;
use super::super::inbound::exchange_queue::QueueItem;
use super::super::wire_format::make_ipc_options;
use super::super::wire_format::serialize_block;
use super::exchange_send_buffer::ExchangeSendBuffer;

/// Deliver blocks locally without serialization or through a remote send buffer.
#[async_trait::async_trait]
pub trait BlockSender: Send + Sync {
    fn close(&self);

    fn is_closed(&self) -> bool;

    async fn send_block(&self, block: DataBlock) -> Result<()>;
}

pub struct LocalBlockSender {
    semaphore: Arc<Semaphore>,
    sender: Sender<QueueItem>,
    sender_count: Arc<AtomicUsize>,
    max_bytes_local_channel: usize,
}

#[async_trait::async_trait]
impl BlockSender for LocalBlockSender {
    fn close(&self) {}

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    async fn send_block(&self, block: DataBlock) -> Result<()> {
        let size = block.memory_size();
        let size = std::cmp::min(size, self.max_bytes_local_channel);

        let semaphore = self.semaphore.clone();
        if let Ok(x) = semaphore.try_acquire_many_owned(size as u32) {
            let item = LocalQueueItem::create(block, x);

            if let Err(cause) = self.sender.try_send(item) {
                if cause.is_full() {
                    unreachable!("Logical error, local channel quota queue is full");
                }
            }

            return Ok(());
        }

        let semaphore = self.semaphore.clone();
        let Ok(x) = semaphore.acquire_many_owned(size as u32).await else {
            return Err(ErrorCode::Internal(
                "Logical error, inbound quota semaphore is closed.",
            ));
        };

        let item = LocalQueueItem::create(block, x);
        if let Err(cause) = self.sender.try_send(item) {
            if cause.is_full() {
                unreachable!("Logical error, local channel quota queue is full");
            }
        }

        Ok(())
    }
}

impl Drop for LocalBlockSender {
    fn drop(&mut self) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.sender.close();
        }
    }
}

/// Max bytes for local channel backpressure (20 MB).
pub const LOCAL_CHANNEL_MAX_BYTES: usize = 20 * 1024 * 1024;

pub fn create_local_block_senders(
    channel_set: &ExchangeReceiveChannels,
) -> Vec<Arc<dyn BlockSender>> {
    let semaphore = Arc::new(Semaphore::new(LOCAL_CHANNEL_MAX_BYTES));

    let mut outbound = Vec::<Arc<dyn BlockSender>>::with_capacity(channel_set.channels.len());
    for channel in channel_set.channels.iter() {
        channel.sender_count.fetch_add(1, Ordering::AcqRel);

        outbound.push(Arc::new(LocalBlockSender {
            semaphore: semaphore.clone(),
            sender: channel.sender.clone(),
            sender_count: channel.sender_count.clone(),
            max_bytes_local_channel: LOCAL_CHANNEL_MAX_BYTES,
        }));
    }

    outbound
}
// ---------------------------------------------------------------------------
// RemoteBlockSender — sends via ExchangeSendBuffer + PingPongTransport
// ---------------------------------------------------------------------------

/// Remote exchange channel that serializes DataBlock to FlightData
/// and sends through ExchangeSendBuffer.
pub struct RemoteBlockSender {
    dest_idx: usize,
    channel_id: usize,
    buffer: Arc<ExchangeSendBuffer>,
    ipc_options: IpcWriteOptions,
}

impl RemoteBlockSender {
    pub fn create(
        dest_idx: usize,
        channel_id: usize,
        buffer: Arc<ExchangeSendBuffer>,
        compression: Option<FlightCompression>,
    ) -> Result<Arc<dyn BlockSender>> {
        Ok(Arc::new(Self {
            dest_idx,
            channel_id,
            buffer,
            ipc_options: make_ipc_options(compression)?,
        }))
    }
}

#[async_trait::async_trait]
impl BlockSender for RemoteBlockSender {
    fn close(&self) {}

    fn is_closed(&self) -> bool {
        self.buffer.is_closed(self.dest_idx)
    }

    async fn send_block(&self, block: DataBlock) -> Result<()> {
        Profile::record_usize_profile(ProfileStatisticsName::ExchangeRows, block.num_rows());
        Profile::record_usize_profile(ProfileStatisticsName::ExchangeBytes, block.memory_size());

        let flight_data_list = serialize_block(block, &self.ipc_options, None)?;

        let tid_prefix = (self.channel_id as u16).to_le_bytes();

        for flight_data in flight_data_list {
            let mut metadata = tid_prefix.to_vec();
            metadata.extend_from_slice(&flight_data.app_metadata);
            let flight_data = FlightData {
                app_metadata: metadata.into(),
                ..flight_data
            };

            self.buffer
                .add_data(self.channel_id, self.dest_idx, flight_data)
                .await?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RoundRobinBlockSender — round-robin across thread senders for one node
// ---------------------------------------------------------------------------

/// Distributes blocks across a node's thread senders in round-robin order.
pub struct RoundRobinBlockSender {
    channels: Vec<Arc<dyn BlockSender>>,
    next_idx: AtomicUsize,
}

impl RoundRobinBlockSender {
    pub fn create(channels: Vec<Arc<dyn BlockSender>>) -> Arc<dyn BlockSender> {
        Arc::new(Self {
            channels,
            next_idx: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl BlockSender for RoundRobinBlockSender {
    fn close(&self) {
        for ch in &self.channels {
            ch.close();
        }
    }

    fn is_closed(&self) -> bool {
        self.channels.iter().all(|ch| ch.is_closed())
    }

    async fn send_block(&self, block: DataBlock) -> Result<()> {
        if self.channels.is_empty() {
            return Ok(());
        }

        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        self.channels[idx].send_block(block).await
    }
}

pub struct DummyBlockSender;

impl DummyBlockSender {
    pub fn create() -> Arc<dyn BlockSender> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl BlockSender for DummyBlockSender {
    fn close(&self) {}

    fn is_closed(&self) -> bool {
        true
    }

    async fn send_block(&self, _block: DataBlock) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use arrow_flight::FlightData;
    use arrow_schema::Schema as ArrowSchema;
    use databend_common_base::runtime::Runtime;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use tonic::Status;

    use super::*;
    use crate::servers::flight::v1::network::outbound::exchange_send_buffer::ExchangeSendBuffer;
    use crate::servers::flight::v1::network::outbound::exchange_send_buffer::ExchangeSendBufferConfig;
    use crate::servers::flight::v1::network::ping_pong_transport::PingPongTransport;
    use crate::servers::flight::v1::network::wire_format::deserialize_flight_data;
    use crate::servers::flight::v1::network::wire_format::strip_tid;

    fn test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::with_worker_threads(2, None).unwrap())
    }

    fn create_mock_exchange(
        num_threads: usize,
    ) -> (
        PingPongTransport,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        let exchange = PingPongTransport::from_stream(
            num_threads,
            send_tx,
            pong_rx,
            "query-node-0",
            "query-node-1",
        );
        (exchange, send_rx, pong_tx)
    }

    fn make_block(rows: usize) -> DataBlock {
        let col = Int32Type::from_data(vec![0i32; rows]);
        DataBlock::new_from_columns(vec![col])
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remote_channel_send_block() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );
        let channel = RemoteBlockSender::create(0, 0, buffer, None).unwrap();

        channel.send_block(make_block(10)).await.unwrap();

        let received = send_rx.recv().await.unwrap();
        let meta = received.app_metadata.to_vec();
        // First 2 bytes: tid=0 as u16 LE
        assert_eq!(&meta[..2], &[0, 0]);
        // Last byte: 0x01 fragment marker
        assert_eq!(*meta.last().unwrap(), 0x01);

        // Cleanup
        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remote_channel_empty_block_no_meta() {
        let rt = test_runtime();
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );
        let channel = RemoteBlockSender::create(0, 0, buffer, None).unwrap();

        // Empty block with no meta should produce no flight data
        channel.send_block(DataBlock::empty()).await.unwrap();
        assert!(send_rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remote_channel_tid_prefix() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(8);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );
        // tid=5
        let channel = RemoteBlockSender::create(0, 5, buffer, None).unwrap();

        channel.send_block(make_block(1)).await.unwrap();

        let received = send_rx.recv().await.unwrap();
        let meta = received.app_metadata.to_vec();
        // First 2 bytes: tid=5 as u16 LE
        assert_eq!(&meta[..2], &[5, 0]);

        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // --- RoundRobinBlockSender tests ---

    struct CountingChannel {
        count: AtomicUsize,
    }

    impl CountingChannel {
        fn create() -> Arc<dyn BlockSender> {
            Arc::new(Self {
                count: AtomicUsize::new(0),
            })
        }

        fn get_count(ch: &Arc<dyn BlockSender>) -> usize {
            let ptr = Arc::as_ptr(ch) as *const CountingChannel;
            unsafe { (*ptr).count.load(Ordering::SeqCst) }
        }
    }

    #[async_trait::async_trait]
    impl BlockSender for CountingChannel {
        fn close(&self) {}
        fn is_closed(&self) -> bool {
            false
        }
        async fn send_block(&self, _block: DataBlock) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_broadcast_round_robin() {
        let ch0 = CountingChannel::create();
        let ch1 = CountingChannel::create();
        let ch2 = CountingChannel::create();
        let channels = vec![ch0.clone(), ch1.clone(), ch2.clone()];
        let broadcast = RoundRobinBlockSender::create(channels);

        // Send 6 blocks — should distribute 2 to each
        for _ in 0..6 {
            broadcast.send_block(make_block(1)).await.unwrap();
        }

        assert_eq!(CountingChannel::get_count(&ch0), 2);
        assert_eq!(CountingChannel::get_count(&ch1), 2);
        assert_eq!(CountingChannel::get_count(&ch2), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remote_channel_roundtrip() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );
        let channel = RemoteBlockSender::create(0, 3, buffer, None).unwrap();

        // Send a block with known data
        let col = Int32Type::from_data(vec![10i32, 20, 30, 40, 50]);
        let original = DataBlock::new_from_columns(vec![col]);
        channel.send_block(original.clone()).await.unwrap();

        // Receive, strip tid, deserialize
        let flight_data = send_rx.recv().await.unwrap();
        let stripped = strip_tid(flight_data);
        let schema = Arc::new(original.infer_schema());
        let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
        let decoded = deserialize_flight_data(stripped, &schema, &arrow_schema).unwrap();

        assert_eq!(decoded.num_rows(), 5);
        assert_eq!(decoded.num_columns(), 1);

        let decoded_val = decoded.columns()[0].value();
        let result_col = decoded_val.as_column().unwrap();
        let original_val = original.columns()[0].value();
        let original_col = original_val.as_column().unwrap();
        assert_eq!(result_col, original_col);

        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
