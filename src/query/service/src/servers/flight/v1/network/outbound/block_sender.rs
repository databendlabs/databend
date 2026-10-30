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

use arrow_ipc::writer::IpcWriteOptions;
use async_channel::Sender;
use async_channel::TrySendError;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_settings::FlightCompression;
use tokio::sync::Semaphore;

use super::super::inbound::exchange_queue::ExchangeReceiveChannels;
use super::super::inbound::exchange_queue::LocalQueueItem;
use super::super::inbound::exchange_queue::QueueItem;
use super::super::wire_format::make_ipc_options;
use super::super::wire_format::serialize_block_for_thread;
use super::exchange_send_buffer::ExchangeSendBuffer;

/// Deliver a block to a destination, locally or over the network.
///
/// Completion means the block has been handed to the local receive queue or the
/// remote send buffer, not that the downstream operator has consumed it. A closed
/// receiver may discard the block. Dropping a sender releases its ownership;
/// other holders can continue sending until they also release their references.
#[async_trait::async_trait]
pub trait BlockSender: Send + Sync {
    fn is_closed(&self) -> bool;

    async fn send_block(&self, block: DataBlock) -> Result<()>;
}

pub struct LocalBlockSender {
    semaphore: Arc<Semaphore>,
    sender: Sender<QueueItem>,
    max_bytes_local_channel: usize,
}

#[async_trait::async_trait]
impl BlockSender for LocalBlockSender {
    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    async fn send_block(&self, block: DataBlock) -> Result<()> {
        let size = block.memory_size().min(self.max_bytes_local_channel);
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(size as u32)
            .await
            .expect("local receive quota is never closed");

        let item = QueueItem::LocalData(LocalQueueItem::new(block, permit));
        match self.sender.try_send(item) {
            Ok(()) | Err(TrySendError::Closed(_)) => Ok(()),
            Err(TrySendError::Full(_)) => unreachable!("local receive queue is unbounded"),
        }
    }
}

/// Max bytes for local channel backpressure (20 MB).
pub const LOCAL_CHANNEL_MAX_BYTES: usize = 20 * 1024 * 1024;

pub fn create_local_block_senders(
    receive_channels: &ExchangeReceiveChannels,
) -> Vec<Arc<LocalBlockSender>> {
    let semaphore = Arc::new(Semaphore::new(LOCAL_CHANNEL_MAX_BYTES));

    let mut outbound = Vec::<Arc<LocalBlockSender>>::with_capacity(receive_channels.senders.len());
    for sender in &receive_channels.senders {
        outbound.push(Arc::new(LocalBlockSender {
            semaphore: semaphore.clone(),
            sender: sender.clone(),
            max_bytes_local_channel: LOCAL_CHANNEL_MAX_BYTES,
        }));
    }

    outbound
}

/// Encode blocks for a fixed remote thread and enqueue them in the shared send buffer.
pub struct RemoteBlockSender {
    dest_idx: usize,
    thread_id: usize,
    buffer: Arc<ExchangeSendBuffer>,
    ipc_options: IpcWriteOptions,
}

impl RemoteBlockSender {
    pub fn create(
        dest_idx: usize,
        thread_id: usize,
        buffer: Arc<ExchangeSendBuffer>,
        compression: Option<FlightCompression>,
    ) -> Result<Arc<dyn BlockSender>> {
        Ok(Arc::new(Self {
            dest_idx,
            thread_id,
            buffer,
            ipc_options: make_ipc_options(compression)?,
        }))
    }
}

#[async_trait::async_trait]
impl BlockSender for RemoteBlockSender {
    fn is_closed(&self) -> bool {
        self.buffer.is_closed(self.dest_idx)
    }

    async fn send_block(&self, block: DataBlock) -> Result<()> {
        Profile::record_usize_profile(ProfileStatisticsName::ExchangeRows, block.num_rows());
        Profile::record_usize_profile(ProfileStatisticsName::ExchangeBytes, block.memory_size());
        let items = serialize_block_for_thread(block, self.thread_id, &self.ipc_options)?;
        self.buffer.enqueue_items(self.dest_idx, items).await
    }
}

/// Distribute whole blocks among a node's local or remote thread senders.
pub struct RoundRobinBlockSender {
    senders: Vec<Arc<dyn BlockSender>>,
    next_idx: AtomicUsize,
}

impl RoundRobinBlockSender {
    pub fn create(senders: Vec<Arc<dyn BlockSender>>) -> Arc<dyn BlockSender> {
        Arc::new(Self {
            senders,
            next_idx: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl BlockSender for RoundRobinBlockSender {
    fn is_closed(&self) -> bool {
        self.senders.iter().all(|ch| ch.is_closed())
    }

    async fn send_block(&self, block: DataBlock) -> Result<()> {
        if self.senders.is_empty() {
            return Ok(());
        }

        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        self.senders[idx].send_block(block).await
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
    async fn test_remote_block_sender_send_block() {
        let rt = Runtime::with_worker_threads(2, None).unwrap();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );
        let sender = RemoteBlockSender::create(0, 0, buffer, None).unwrap();

        sender.send_block(make_block(10)).await.unwrap();

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
    async fn test_remote_block_sender_empty_block_no_meta() {
        let rt = Runtime::with_worker_threads(2, None).unwrap();
        let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );
        let sender = RemoteBlockSender::create(0, 0, buffer, None).unwrap();

        // Empty block with no meta should produce no flight data
        sender.send_block(DataBlock::empty()).await.unwrap();
        assert!(send_rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remote_block_sender_tid_prefix() {
        let rt = Runtime::with_worker_threads(2, None).unwrap();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(8);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );
        // tid=5
        let sender = RemoteBlockSender::create(0, 5, buffer, None).unwrap();

        sender.send_block(make_block(1)).await.unwrap();

        let received = send_rx.recv().await.unwrap();
        let meta = received.app_metadata.to_vec();
        // First 2 bytes: tid=5 as u16 LE
        assert_eq!(&meta[..2], &[5, 0]);

        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // --- RoundRobinBlockSender tests ---

    #[derive(Default)]
    struct CountingBlockSender {
        count: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BlockSender for CountingBlockSender {
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
        let senders = [
            Arc::new(CountingBlockSender::default()),
            Arc::new(CountingBlockSender::default()),
            Arc::new(CountingBlockSender::default()),
        ];
        let broadcast = RoundRobinBlockSender::create(
            senders
                .iter()
                .map(|sender| sender.clone() as Arc<dyn BlockSender>)
                .collect(),
        );

        // Send 6 blocks — should distribute 2 to each
        for _ in 0..6 {
            broadcast.send_block(make_block(1)).await.unwrap();
        }

        for sender in senders {
            assert_eq!(sender.count.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remote_block_sender_roundtrip() {
        let rt = Runtime::with_worker_threads(2, None).unwrap();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(
            ExchangeSendBuffer::create(vec![exchange], ExchangeSendBufferConfig::default(), &rt)
                .unwrap(),
        );
        let sender = RemoteBlockSender::create(0, 3, buffer, None).unwrap();

        // Send a block with known data
        let col = Int32Type::from_data(vec![10i32, 20, 30, 40, 50]);
        let original = DataBlock::new_from_columns(vec![col]);
        sender.send_block(original.clone()).await.unwrap();

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
