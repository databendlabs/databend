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

use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use arrow_ipc::CompressionType;
use arrow_ipc::writer::DictionaryTracker;
use arrow_ipc::writer::IpcDataGenerator;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::BinaryWrite;
use databend_common_io::prelude::bincode_serialize_into_buf;
use databend_common_settings::FlightCompression;

use super::outbound_buffer::ExchangeSinkBuffer;

/// Outbound channel trait for sending data blocks.
/// Supports both local (zero-copy) and remote (serialized) channels.
#[async_trait::async_trait]
pub trait OutboundChannel: Send + Sync {
    fn close(&self);

    fn is_closed(&self) -> bool;

    async fn add_block(&self, block: DataBlock) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Shared serialization helpers
// ---------------------------------------------------------------------------

fn flight_compression(compression: Option<FlightCompression>) -> Option<CompressionType> {
    match compression {
        None => None,
        Some(FlightCompression::Lz4) => Some(CompressionType::LZ4_FRAME),
        Some(FlightCompression::Zstd) => Some(CompressionType::ZSTD),
    }
}

fn make_ipc_options(compression: Option<FlightCompression>) -> Result<IpcWriteOptions> {
    Ok(IpcWriteOptions::default().try_with_compression(flight_compression(compression))?)
}

fn encode_batch(
    batch: &RecordBatch,
    ipc_options: &IpcWriteOptions,
) -> Result<(Vec<FlightData>, FlightData)> {
    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let (encoded_dictionaries, encoded_batch) =
        data_gen.encoded_batch(batch, &mut dictionary_tracker, ipc_options)?;
    let dictionaries: Vec<FlightData> = encoded_dictionaries.into_iter().map(Into::into).collect();
    let batch_data: FlightData = encoded_batch.into();
    Ok((dictionaries, batch_data))
}

fn serialize_to_batches(
    block: DataBlock,
    ipc_options: &IpcWriteOptions,
) -> Result<(Vec<FlightData>, Vec<FlightData>)> {
    if block.is_empty() {
        let empty_batch = RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(0)),
        )?;
        let (dicts, batch) = encode_batch(&empty_batch, ipc_options)?;
        Ok((dicts, vec![batch]))
    } else {
        let schema = block.infer_schema();
        let arrow_schema = ArrowSchema::from(&schema);
        let batch = block.to_record_batch_with_dataschema(&schema)?;
        let _ = &arrow_schema; // used for schema inference, batch carries it
        let (dicts, batch_data) = encode_batch(&batch, ipc_options)?;
        Ok((dicts, vec![batch_data]))
    }
}

/// Serialize a DataBlock into a list of FlightData (dictionaries + fragments).
/// The `descriptor` is attached to each FlightData for routing.
pub fn serialize_block(
    block: DataBlock,
    ipc_options: &IpcWriteOptions,
    descriptor: Option<FlightDescriptor>,
) -> Result<Vec<FlightData>> {
    if block.is_empty() && block.get_meta().is_none() {
        return Ok(vec![]);
    }

    // Build metadata (row count + block meta) before moving block
    let mut meta = vec![];
    meta.write_scalar_own(block.num_rows() as u32)?;
    bincode_serialize_into_buf(&mut meta, &block.get_meta())?;

    let (dict_data, value_data) = serialize_to_batches(block, ipc_options)?;

    let meta_bytes: Bytes = meta.into();
    let mut result = Vec::with_capacity(dict_data.len() + value_data.len());

    // Dictionary data with marker (0x05)
    for mut dict in dict_data {
        let mut app_metadata = dict.app_metadata.to_vec();
        app_metadata.push(0x05);
        dict.app_metadata = app_metadata.into();
        dict.flight_descriptor = descriptor.clone();
        result.push(dict);
    }

    // Fragment data with metadata (0x01)
    for value in value_data {
        let mut metadata = meta_bytes.to_vec();
        metadata.push(0x01);
        result.push(FlightData {
            app_metadata: metadata.into(),
            data_body: value.data_body,
            data_header: value.data_header,
            flight_descriptor: descriptor.clone(),
        });
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// RemoteChannel — sends via ExchangeSinkBuffer + PingPongExchange
// ---------------------------------------------------------------------------

/// Remote exchange channel that serializes DataBlock to FlightData
/// and sends through ExchangeSinkBuffer.
pub struct RemoteChannel {
    dest_idx: usize,
    channel_id: usize,
    buffer: Arc<ExchangeSinkBuffer>,
    ipc_options: IpcWriteOptions,
}

impl RemoteChannel {
    pub fn create(
        dest_idx: usize,
        channel_id: usize,
        buffer: Arc<ExchangeSinkBuffer>,
        compression: Option<FlightCompression>,
    ) -> Result<Arc<dyn OutboundChannel>> {
        Ok(Arc::new(Self {
            dest_idx,
            channel_id,
            buffer,
            ipc_options: make_ipc_options(compression)?,
        }))
    }
}

#[async_trait::async_trait]
impl OutboundChannel for RemoteChannel {
    fn close(&self) {}

    fn is_closed(&self) -> bool {
        false
    }

    async fn add_block(&self, block: DataBlock) -> Result<()> {
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
// RoundRobinChannel — round-robin across multiple RemoteChannels for one node
// ---------------------------------------------------------------------------

/// Wraps multiple OutboundChannels (one per thread on a remote node)
/// and distributes blocks across them in round-robin fashion.
pub struct RoundRobinChannel {
    channels: Vec<Arc<dyn OutboundChannel>>,
    next_idx: AtomicUsize,
}

impl RoundRobinChannel {
    pub fn create(channels: Vec<Arc<dyn OutboundChannel>>) -> Arc<dyn OutboundChannel> {
        Arc::new(Self {
            channels,
            next_idx: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl OutboundChannel for RoundRobinChannel {
    fn close(&self) {
        for ch in &self.channels {
            ch.close();
        }
    }

    fn is_closed(&self) -> bool {
        self.channels.iter().all(|ch| ch.is_closed())
    }

    async fn add_block(&self, block: DataBlock) -> Result<()> {
        if self.channels.is_empty() {
            return Ok(());
        }

        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        self.channels[idx].add_block(block).await
    }
}

pub struct DummyOutboundChannel;

impl DummyOutboundChannel {
    pub fn create() -> Arc<dyn OutboundChannel> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl OutboundChannel for DummyOutboundChannel {
    fn close(&self) {}

    fn is_closed(&self) -> bool {
        true
    }

    async fn add_block(&self, _block: DataBlock) -> Result<()> {
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
    use crate::servers::flight::v1::network::inbound_channel::deserialize_flight_data;
    use crate::servers::flight::v1::network::inbound_channel::strip_tid;
    use crate::servers::flight::v1::network::outbound_buffer::ExchangeBufferConfig;
    use crate::servers::flight::v1::network::outbound_buffer::ExchangeSinkBuffer;
    use crate::servers::flight::v1::network::outbound_transport::PingPongExchange;

    fn test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::with_worker_threads(2, None).unwrap())
    }

    fn create_mock_exchange(
        num_threads: usize,
    ) -> (
        PingPongExchange,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        let exchange = PingPongExchange::from_stream(num_threads, send_tx, pong_rx);
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
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap(),
        );
        let channel = RemoteChannel::create(0, 0, buffer, None).unwrap();

        channel.add_block(make_block(10)).await.unwrap();

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
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap(),
        );
        let channel = RemoteChannel::create(0, 0, buffer, None).unwrap();

        // Empty block with no meta should produce no flight data
        channel.add_block(DataBlock::empty()).await.unwrap();
        assert!(send_rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remote_channel_tid_prefix() {
        let rt = test_runtime();
        let (exchange, send_rx, pong_tx) = create_mock_exchange(8);
        let buffer = Arc::new(
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap(),
        );
        // tid=5
        let channel = RemoteChannel::create(0, 5, buffer, None).unwrap();

        channel.add_block(make_block(1)).await.unwrap();

        let received = send_rx.recv().await.unwrap();
        let meta = received.app_metadata.to_vec();
        // First 2 bytes: tid=5 as u16 LE
        assert_eq!(&meta[..2], &[5, 0]);

        pong_tx.send(Ok(FlightData::default())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // --- RoundRobinChannel tests ---

    struct CountingChannel {
        count: AtomicUsize,
    }

    impl CountingChannel {
        fn create() -> Arc<dyn OutboundChannel> {
            Arc::new(Self {
                count: AtomicUsize::new(0),
            })
        }

        fn get_count(ch: &Arc<dyn OutboundChannel>) -> usize {
            let ptr = Arc::as_ptr(ch) as *const CountingChannel;
            unsafe { (*ptr).count.load(Ordering::SeqCst) }
        }
    }

    #[async_trait::async_trait]
    impl OutboundChannel for CountingChannel {
        fn close(&self) {}
        fn is_closed(&self) -> bool {
            false
        }
        async fn add_block(&self, _block: DataBlock) -> Result<()> {
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
        let broadcast = RoundRobinChannel::create(channels);

        // Send 6 blocks — should distribute 2 to each
        for _ in 0..6 {
            broadcast.add_block(make_block(1)).await.unwrap();
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
            ExchangeSinkBuffer::create(vec![exchange], ExchangeBufferConfig::default(), &rt)
                .unwrap(),
        );
        let channel = RemoteChannel::create(0, 3, buffer, None).unwrap();

        // Send a block with known data
        let col = Int32Type::from_data(vec![10i32, 20, 30, 40, 50]);
        let original = DataBlock::new_from_columns(vec![col]);
        channel.add_block(original.clone()).await.unwrap();

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
