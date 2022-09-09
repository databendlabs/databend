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

use std::fmt::Debug;
use std::fmt::Formatter;
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

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct FragmentData {
    pub data: FlightData,
}

impl FragmentData {
    pub fn create(data: FlightData) -> FragmentData {
        FragmentData { data }
    }
}

impl Debug for FragmentData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentData").finish()
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum ProgressInfo {
    ScanProgress(ProgressValues),
    WriteProgress(ProgressValues),
    ResultProgress(ProgressValues),
}

#[derive(Debug)]
pub struct PrecommitBlock(pub DataBlock);

#[derive(Debug)]
pub enum DataPacket {
    ErrorCode(ErrorCode),
    FragmentData(FragmentData),
    FetchProgressAndPrecommit,
    ProgressAndPrecommit {
        progress: Vec<ProgressInfo>,
        precommit: Vec<PrecommitBlock>,
    },
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

impl ProgressInfo {
    pub fn inc(&self, ctx: &Arc<QueryContext>) {
        match self {
            ProgressInfo::ScanProgress(values) => ctx.get_scan_progress().incr(values),
            ProgressInfo::WriteProgress(values) => ctx.get_write_progress().incr(values),
            ProgressInfo::ResultProgress(values) => ctx.get_result_progress().incr(values),
        };
    }
}

impl PrecommitBlock {
    pub fn precommit(&self, ctx: &Arc<QueryContext>) {
        ctx.push_precommit_block(self.0.clone());
    }
}

impl From<DataPacket> for FlightData {
    fn from(packet: DataPacket) -> Self {
        match packet {
            DataPacket::ErrorCode(error) => FlightData::from(error),
            DataPacket::FragmentData(fragment_data) => FlightData::from(fragment_data),
            DataPacket::FetchProgressAndPrecommit => FlightData {
                app_metadata: vec![0x03],
                data_body: vec![],
                data_header: vec![],
                flight_descriptor: None,
            },
            DataPacket::ProgressAndPrecommit {
                progress,
                precommit,
            } => {
                let mut data_body = vec![];
                data_body
                    .write_u64::<BigEndian>(progress.len() as u64)
                    .unwrap();
                data_body
                    .write_u64::<BigEndian>(precommit.len() as u64)
                    .unwrap();

                for progress_info in progress {
                    let (info_type, values) = match progress_info {
                        ProgressInfo::ScanProgress(values) => (1_u8, values),
                        ProgressInfo::WriteProgress(values) => (2_u8, values),
                        ProgressInfo::ResultProgress(values) => (3_u8, values),
                    };

                    data_body.write_u8(info_type).unwrap();
                    data_body
                        .write_u64::<BigEndian>(values.rows as u64)
                        .unwrap();
                    data_body
                        .write_u64::<BigEndian>(values.bytes as u64)
                        .unwrap();
                }

                for precommit_block in precommit {
                    precommit_block.write(&mut data_body).unwrap();
                }

                FlightData {
                    data_body,
                    data_header: vec![],
                    flight_descriptor: None,
                    app_metadata: vec![0x04],
                }
            }
        }
    }
}

impl From<FragmentData> for FlightData {
    fn from(data: FragmentData) -> Self {
        FlightData {
            app_metadata: vec![0x01],
            data_body: data.data.data_body,
            data_header: data.data.data_header,
            flight_descriptor: None,
        }
    }
}

impl PrecommitBlock {
    pub fn write<T: Write>(self, bytes: &mut T) -> Result<()> {
        let data_block = self.0;
        let arrow_schema = data_block.schema().to_arrow();
        let schema = serialize_schema_to_info(&arrow_schema, None)?;

        // schema_flight
        let options = WriteOptions { compression: None };
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let chunks = data_block.try_into()?;
        let (_dicts, data_flight) = serialize_batch(&chunks, &ipc_fields, &options)?;

        bytes.write_u64::<BigEndian>(schema.len() as u64)?;
        bytes.write_u64::<BigEndian>(data_flight.data_header.len() as u64)?;
        bytes.write_u64::<BigEndian>(data_flight.data_body.len() as u64)?;

        bytes.write_all(&schema)?;
        bytes.write_all(&data_flight.data_header)?;
        bytes.write_all(&data_flight.data_body)?;
        Ok(())
    }

    pub fn read<T: Read>(bytes: &mut T) -> Result<PrecommitBlock> {
        let schema_len = bytes.read_u64::<BigEndian>()? as usize;
        let header_len = bytes.read_u64::<BigEndian>()? as usize;
        let body_len = bytes.read_u64::<BigEndian>()? as usize;

        let mut schema = vec![0; schema_len];
        let mut flight_header = vec![0; header_len];
        let mut flight_body = vec![0; body_len];

        bytes.read_exact(&mut schema)?;
        bytes.read_exact(&mut flight_header)?;
        bytes.read_exact(&mut flight_body)?;
        let (arrow_schema, _) = deserialize_schema(&schema)?;

        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        let chunk = deserialize_batch(
            &FlightData {
                app_metadata: vec![],
                data_header: flight_header,
                data_body: flight_body,
                flight_descriptor: None,
            },
            &arrow_schema.fields,
            &ipc_schema,
            &Default::default(),
        )?;

        let data_schema = Arc::new(DataSchema::from(arrow_schema));

        Ok(PrecommitBlock(DataBlock::from_chunk(&data_schema, &chunk)?))
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
            0x03 => Ok(DataPacket::FetchProgressAndPrecommit),
            0x04 => {
                let mut bytes = flight_data.data_body.as_slice();
                let progress_size = bytes.read_u64::<BigEndian>()?;
                let precommit_size = bytes.read_u64::<BigEndian>()?;

                let mut progress_info = Vec::with_capacity(progress_size as usize);
                for _index in 0..progress_size {
                    let info_type = bytes.read_u8()?;
                    let rows = bytes.read_u64::<BigEndian>()? as usize;
                    let bytes = bytes.read_u64::<BigEndian>()? as usize;

                    progress_info.push(match info_type {
                        1 => Ok(ProgressInfo::ScanProgress(ProgressValues { rows, bytes })),
                        2 => Ok(ProgressInfo::WriteProgress(ProgressValues { rows, bytes })),
                        3 => Ok(ProgressInfo::ResultProgress(ProgressValues { rows, bytes })),
                        _ => Err(ErrorCode::UnImplement(format!(
                            "Unimplemented progress info type, {}",
                            info_type
                        ))),
                    }?);
                }

                let mut precommit = Vec::with_capacity(precommit_size as usize);
                for _index in 0..precommit_size {
                    precommit.push(PrecommitBlock::read(&mut bytes)?);
                }

                Ok(DataPacket::ProgressAndPrecommit {
                    precommit,
                    progress: progress_info,
                })
            }
            _ => Err(ErrorCode::BadBytes("Unknown flight data packet type.")),
        }
    }
}

impl TryFrom<FlightData> for FragmentData {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        Ok(FragmentData::create(FlightData {
            app_metadata: vec![],
            flight_descriptor: None,
            data_body: flight_data.data_body,
            data_header: flight_data.data_header,
        }))
    }
}
