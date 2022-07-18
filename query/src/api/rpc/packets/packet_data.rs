// Copyright 2022 Datafuse Labs.
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

use std::io::Read;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow::io::flight::serialize_batch;
use common_arrow::arrow::io::flight::serialize_schema_to_info;
use common_arrow::arrow::io::ipc::read::deserialize_schema;
use common_arrow::arrow::io::ipc::write::default_ipc_fields;
use common_arrow::arrow::io::ipc::write::WriteOptions;
use common_arrow::arrow::io::ipc::IpcSchema;
use common_arrow::arrow_format::flight::data::FlightData;
use common_base::base::ProgressValues;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::sessions::query_ctx::TableContext;
use crate::sessions::QueryContext;

pub enum FragmentData {
    End(usize),
    Data(usize, FlightData),
}

#[allow(clippy::enum_variant_names)]
pub enum ProgressInfo {
    ScanProgress(ProgressValues),
    WriteProgress(ProgressValues),
    ResultProgress(ProgressValues),
}

pub struct PrecommitBlock(pub DataBlock);

pub enum DataPacket {
    ErrorCode(ErrorCode),
    Progress(ProgressInfo),
    FragmentData(FragmentData),
    PrecommitBlock(PrecommitBlock),
}

pub struct DataPacketStream {
    rx: async_channel::Receiver<DataPacket>,
}

impl DataPacketStream {
    pub fn create(rx: async_channel::Receiver<DataPacket>) -> DataPacketStream {
        DataPacketStream { rx }
    }
}

impl Stream for DataPacketStream {
    type Item = FlightData;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(packet)) => Poll::Ready(Some(FlightData::from(packet))),
        }
    }
}

impl FragmentData {
    pub fn get_fragment_id(&self) -> usize {
        match self {
            FragmentData::End(fragment_id) => *fragment_id,
            FragmentData::Data(fragment_id, _) => *fragment_id,
        }
    }
}

impl ProgressInfo {
    pub fn inc(&self, ctx: &Arc<QueryContext>) -> Result<()> {
        match self {
            ProgressInfo::ScanProgress(values) => ctx.get_scan_progress().incr(values),
            ProgressInfo::WriteProgress(values) => ctx.get_write_progress().incr(values),
            ProgressInfo::ResultProgress(values) => ctx.get_result_progress().incr(values),
        };

        Ok(())
    }
}

impl PrecommitBlock {
    pub fn precommit(&self, ctx: &Arc<QueryContext>) -> Result<()> {
        ctx.push_precommit_block(self.0.clone());
        Ok(())
    }
}

impl From<DataPacket> for FlightData {
    fn from(packet: DataPacket) -> Self {
        match packet {
            DataPacket::ErrorCode(error) => FlightData::from(error),
            DataPacket::Progress(progress_info) => FlightData::from(progress_info),
            DataPacket::FragmentData(fragment_data) => FlightData::from(fragment_data),
            DataPacket::PrecommitBlock(precommit_block) => {
                FlightData::try_from(precommit_block).unwrap_or_else(FlightData::from)
            }
        }
    }
}

impl From<FragmentData> for FlightData {
    fn from(data: FragmentData) -> Self {
        let mut app_metadata = vec![0x01];
        let fragment_id = data.get_fragment_id();
        app_metadata.extend_from_slice(&fragment_id.to_be_bytes());

        match data {
            FragmentData::End(_) => FlightData {
                app_metadata,
                data_body: vec![],
                data_header: vec![],
                flight_descriptor: None,
            },
            FragmentData::Data(_, flight_data) => FlightData {
                app_metadata,
                data_body: flight_data.data_body,
                data_header: flight_data.data_header,
                flight_descriptor: None,
            },
        }
    }
}

impl From<ProgressInfo> for FlightData {
    fn from(info: ProgressInfo) -> Self {
        let (info_type, values) = match info {
            ProgressInfo::ScanProgress(values) => (1_u8, values),
            ProgressInfo::WriteProgress(values) => (2_u8, values),
            ProgressInfo::ResultProgress(values) => (3_u8, values),
        };

        let mut bytes = Vec::with_capacity(16);
        bytes.write_u8(info_type).unwrap();
        bytes.write_u64::<BigEndian>(values.rows as u64).unwrap();
        bytes.write_u64::<BigEndian>(values.bytes as u64).unwrap();

        FlightData {
            data_body: bytes,
            data_header: vec![],
            app_metadata: vec![0x03],
            flight_descriptor: None,
        }
    }
}

impl TryFrom<PrecommitBlock> for FlightData {
    type Error = ErrorCode;
    fn try_from(block: PrecommitBlock) -> Result<Self> {
        let data_block = block.0;
        let arrow_schema = data_block.schema().to_arrow();
        let schema = serialize_schema_to_info(&arrow_schema, None)?;

        // schema_flight
        let options = WriteOptions { compression: None };
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let chunks = data_block.try_into()?;
        let (_dicts, data_flight) = serialize_batch(&chunks, &ipc_fields, &options)?;

        let header_len = schema.len() + data_flight.data_header.len();
        let mut data_header = Vec::with_capacity(header_len + 8);

        data_header.write_u64::<BigEndian>(schema.len() as u64)?;
        data_header.write_u64::<BigEndian>(data_flight.data_header.len() as u64)?;

        data_header.write_all(&schema)?;
        data_header.write_all(&data_flight.data_header)?;

        Ok(FlightData {
            data_header,
            data_body: data_flight.data_body,
            flight_descriptor: None,
            app_metadata: vec![0x04],
        })
    }
}

impl TryFrom<FlightData> for DataPacket {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        if flight_data.app_metadata.is_empty() {
            return Err(ErrorCode::BadBytes("Flight data app metadata is empty."));
        }

        match flight_data.app_metadata[0] {
            0x01 => Ok(DataPacket::FragmentData(FragmentData::try_from(
                flight_data,
            )?)),
            0x02 => Ok(DataPacket::ErrorCode(ErrorCode::try_from(flight_data)?)),
            0x03 => Ok(DataPacket::Progress(ProgressInfo::try_from(flight_data)?)),
            0x04 => Ok(DataPacket::PrecommitBlock(PrecommitBlock::try_from(
                flight_data,
            )?)),
            _ => Err(ErrorCode::BadBytes("Unknown flight data packet type.")),
        }
    }
}

impl TryFrom<FlightData> for FragmentData {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        match flight_data.app_metadata[1..].try_into() {
            Err(_) => Err(ErrorCode::BadBytes("Cannot parse inf usize.")),
            Ok(slice) => Ok(
                match flight_data.data_header.is_empty() && flight_data.data_body.is_empty() {
                    true => FragmentData::End(usize::from_be_bytes(slice)),
                    false => FragmentData::Data(usize::from_be_bytes(slice), FlightData {
                        app_metadata: vec![],
                        flight_descriptor: None,
                        data_body: flight_data.data_body,
                        data_header: flight_data.data_header,
                    }),
                },
            ),
        }
    }
}

impl TryFrom<FlightData> for ProgressInfo {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        let mut data_body = flight_data.data_body.as_slice();
        let info_type = data_body.read_u8()?;
        let rows = data_body.read_u64::<BigEndian>()? as usize;
        let bytes = data_body.read_u64::<BigEndian>()? as usize;

        match info_type {
            1 => Ok(ProgressInfo::ScanProgress(ProgressValues { rows, bytes })),
            2 => Ok(ProgressInfo::WriteProgress(ProgressValues { rows, bytes })),
            3 => Ok(ProgressInfo::ResultProgress(ProgressValues { rows, bytes })),
            _ => Err(ErrorCode::UnImplement(format!(
                "Unimplemented progress info type, {}",
                info_type
            ))),
        }
    }
}

impl TryFrom<FlightData> for PrecommitBlock {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        let mut data_header = flight_data.data_header.as_slice();

        let schema_binary_len = data_header.read_u64::<BigEndian>()? as usize;
        let data_flight_data_header_len = data_header.read_u64::<BigEndian>()? as usize;

        let mut schema_binary = vec![0; schema_binary_len];
        let mut data_flight_data_header = vec![0; data_flight_data_header_len];

        data_header.read_exact(&mut schema_binary)?;
        data_header.read_exact(&mut data_flight_data_header)?;
        let (arrow_schema, _) = deserialize_schema(&schema_binary)?;

        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        let flight = FlightData {
            app_metadata: vec![],
            data_header: data_flight_data_header,
            data_body: flight_data.data_body,
            flight_descriptor: None,
        };

        let chunk = deserialize_batch(
            &flight,
            &arrow_schema.fields,
            &ipc_schema,
            &Default::default(),
        )?;
        let data_schema = Arc::new(DataSchema::from(arrow_schema));
        Ok(PrecommitBlock(DataBlock::from_chunk(&data_schema, &chunk)?))
    }
}
