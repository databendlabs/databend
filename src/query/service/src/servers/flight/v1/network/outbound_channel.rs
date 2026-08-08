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
use arrow_ipc::writer::CompressionContext;
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

use super::BlockOutboundSet;
use super::SendOutcome;

/// Outbound channel trait for sending data blocks.
/// Supports both local (zero-copy) and remote (serialized) channels.
#[async_trait::async_trait]
pub trait OutboundChannel: Send + Sync {
    fn close(&self);

    fn is_closed(&self) -> bool;

    async fn add_block(&self, block: DataBlock) -> Result<SendOutcome>;
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
    let mut compression_context = CompressionContext::default();
    let (encoded_dictionaries, encoded_batch) = data_gen.encode(
        batch,
        &mut dictionary_tracker,
        ipc_options,
        &mut compression_context,
    )?;
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

pub struct RemoteOutboundChannel {
    dest_idx: usize,
    channel_id: usize,
    outbounds: Arc<BlockOutboundSet>,
    ipc_options: IpcWriteOptions,
}

impl RemoteOutboundChannel {
    pub fn create(
        dest_idx: usize,
        channel_id: usize,
        outbounds: Arc<BlockOutboundSet>,
        compression: Option<FlightCompression>,
    ) -> Result<Arc<dyn OutboundChannel>> {
        Ok(Arc::new(Self {
            dest_idx,
            channel_id,
            outbounds,
            ipc_options: make_ipc_options(compression)?,
        }))
    }
}

#[async_trait::async_trait]
impl OutboundChannel for RemoteOutboundChannel {
    fn close(&self) {}

    fn is_closed(&self) -> bool {
        self.outbounds.is_closed(self.dest_idx)
    }

    async fn add_block(&self, block: DataBlock) -> Result<SendOutcome> {
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

            let outcome = self
                .outbounds
                .send(self.channel_id, self.dest_idx, flight_data)
                .await?;
            if outcome == SendOutcome::ReceiverClosed {
                return Ok(outcome);
            }
        }

        Ok(SendOutcome::Accepted)
    }
}

pub struct RoundRobinOutboundChannel {
    channels: Vec<Arc<dyn OutboundChannel>>,
    next_idx: AtomicUsize,
}

impl RoundRobinOutboundChannel {
    pub fn create(channels: Vec<Arc<dyn OutboundChannel>>) -> Arc<dyn OutboundChannel> {
        Arc::new(Self {
            channels,
            next_idx: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl OutboundChannel for RoundRobinOutboundChannel {
    fn close(&self) {
        for ch in &self.channels {
            ch.close();
        }
    }

    fn is_closed(&self) -> bool {
        self.channels.iter().all(|ch| ch.is_closed())
    }

    async fn add_block(&self, block: DataBlock) -> Result<SendOutcome> {
        if self.channels.is_empty() {
            return Ok(SendOutcome::ReceiverClosed);
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

    async fn add_block(&self, _block: DataBlock) -> Result<SendOutcome> {
        Ok(SendOutcome::ReceiverClosed)
    }
}
