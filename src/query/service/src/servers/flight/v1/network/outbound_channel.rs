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
use std::task::Poll;
use std::task::Waker;

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

/// Exchange channel trait for sending data blocks.
/// Supports both local (zero-copy) and remote (serialized) channels.
pub trait ExchangeChannel: Send + Sync {
    /// Poll to send a data block to this channel.
    ///
    /// Returns:
    /// - `Poll::Ready(Ok(()))`: Data accepted.
    /// - `Poll::Ready(Err(...))`: Channel error (closed, remote error, etc.)
    /// - `Poll::Pending`: Backpressure triggered, waker registered.
    fn poll_send(&self, waker: &Waker, block: DataBlock) -> Poll<Result<()>>;
}

/// Remote exchange channel that serializes DataBlock to FlightData
/// and sends through ExchangeSinkBuffer.
pub struct RemoteChannel {
    /// Destination index in the buffer's remote list
    dest_idx: usize,

    /// Channel ID for this channel (used by ExchangeSinkBuffer)
    channel_id: usize,

    /// Reference to the shared ExchangeSinkBuffer
    buffer: Arc<ExchangeSinkBuffer>,

    /// IPC write options for Arrow serialization (compression settings)
    ipc_options: IpcWriteOptions,
}

impl RemoteChannel {
    pub fn create(
        dest_idx: usize,
        channel_id: usize,
        buffer: Arc<ExchangeSinkBuffer>,
        compression: Option<FlightCompression>,
    ) -> Result<Self> {
        let compression = match compression {
            None => None,
            Some(FlightCompression::Lz4) => Some(CompressionType::LZ4_FRAME),
            Some(FlightCompression::Zstd) => Some(CompressionType::ZSTD),
        };

        Ok(Self {
            dest_idx,
            channel_id,
            buffer,
            ipc_options: IpcWriteOptions::default().try_with_compression(compression)?,
        })
    }

    /// Serialize a DataBlock to FlightData.
    fn serialize_block(&self, block: DataBlock) -> Result<Vec<FlightData>> {
        // Handle empty blocks with no metadata
        if block.is_empty() && block.get_meta().is_none() {
            return Ok(vec![]);
        }

        // Build metadata (row count + block meta)
        let mut meta = vec![];
        meta.write_scalar_own(block.num_rows() as u32)?;
        bincode_serialize_into_buf(&mut meta, &block.get_meta())?;

        // Convert to Arrow RecordBatch and then to FlightData
        let (dict_data, value_data) = if block.is_empty() {
            self.serialize_empty_batch()?
        } else {
            self.serialize_data_batch(block)?
        };

        // Combine dictionaries and values into final FlightData list
        let meta_bytes: Bytes = meta.into();
        let mut result = Vec::with_capacity(dict_data.len() + value_data.len());

        let descriptor = Some(FlightDescriptor {
            r#type: 0,
            cmd: (self.channel_id as u16).to_le_bytes().to_vec().into(),
            path: vec![],
        });

        // Add dictionary data with marker (0x05)
        for mut dict in dict_data {
            let mut app_metadata = dict.app_metadata.to_vec();
            app_metadata.push(0x05);
            dict.app_metadata = app_metadata.into();
            dict.flight_descriptor = descriptor.clone();
            result.push(dict);
        }

        // Add fragment data with metadata (0x01)
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

    fn serialize_empty_batch(&self) -> Result<(Vec<FlightData>, Vec<FlightData>)> {
        let empty_schema = ArrowSchema::empty();
        let empty_batch = RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(0)),
        )?;

        let (dictionaries, batch_data) =
            self.encode_batch_to_flight_data(&empty_schema, &empty_batch)?;
        Ok((dictionaries, vec![batch_data]))
    }

    fn serialize_data_batch(&self, block: DataBlock) -> Result<(Vec<FlightData>, Vec<FlightData>)> {
        let schema = block.infer_schema();
        let arrow_schema = ArrowSchema::from(&schema);
        let batch = block.to_record_batch_with_dataschema(&schema)?;

        let (dictionaries, batch_data) = self.encode_batch_to_flight_data(&arrow_schema, &batch)?;
        Ok((dictionaries, vec![batch_data]))
    }

    fn encode_batch_to_flight_data(
        &self,
        _schema: &ArrowSchema,
        batch: &RecordBatch,
    ) -> Result<(Vec<FlightData>, FlightData)> {
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        let (encoded_dictionaries, encoded_batch) =
            data_gen.encoded_batch(batch, &mut dictionary_tracker, &self.ipc_options)?;

        let dictionaries: Vec<FlightData> =
            encoded_dictionaries.into_iter().map(Into::into).collect();
        let batch_data: FlightData = encoded_batch.into();

        Ok((dictionaries, batch_data))
    }
}

impl ExchangeChannel for RemoteChannel {
    fn poll_send(&self, waker: &Waker, block: DataBlock) -> Poll<Result<()>> {
        // Profile recording
        Profile::record_usize_profile(ProfileStatisticsName::ExchangeRows, block.num_rows());

        // Serialize the block
        let flight_data_list = match self.serialize_block(block) {
            Ok(list) => list,
            Err(e) => return Poll::Ready(Err(e)),
        };

        // Send each FlightData through the buffer
        for flight_data in flight_data_list {
            let bytes = flight_data.data_body.len()
                + flight_data.data_header.len()
                + flight_data.app_metadata.len();
            Profile::record_usize_profile(ProfileStatisticsName::ExchangeBytes, bytes);

            match self
                .buffer
                .poll_send(self.channel_id, self.dest_idx, flight_data, waker)
            {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(()))
    }
}

/// Broadcast channel that wraps multiple channels and sends blocks
/// to them in round-robin fashion (one channel per poll_send call).
pub struct BroadcastChannel {
    channels: Vec<Arc<dyn ExchangeChannel>>,
    next_idx: AtomicUsize,
}

impl BroadcastChannel {
    pub fn create(channels: Vec<Arc<dyn ExchangeChannel>>) -> Self {
        Self {
            channels,
            next_idx: AtomicUsize::new(0),
        }
    }
}

impl ExchangeChannel for BroadcastChannel {
    fn poll_send(&self, waker: &Waker, block: DataBlock) -> Poll<Result<()>> {
        if self.channels.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Round-robin: get current index and increment
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        self.channels[idx].poll_send(waker, block)
    }
}
