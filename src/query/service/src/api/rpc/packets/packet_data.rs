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

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use common_arrow::arrow_format::flight::data::FlightData;
use common_exception::ErrorCode;
use common_exception::Result;
use tracing::error;

use crate::api::rpc::packets::ProgressInfo;
use crate::api::PrecommitBlock;

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

impl From<DataPacket> for FlightData {
    fn from(packet: DataPacket) -> Self {
        match packet {
            DataPacket::ErrorCode(error) => {
                error!("Got error code data packet: {:?}", error);
                FlightData::from(error)
            }
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

                // Progress.
                // TODO(winter): remove unwrap.
                for progress_info in progress {
                    progress_info.write(&mut data_body).unwrap();
                }

                // Pre-commit.
                // TODO(winter): remove unwrap.
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

                // Progress.
                let mut progress_info = Vec::with_capacity(progress_size as usize);
                for _index in 0..progress_size {
                    progress_info.push(ProgressInfo::read(&mut bytes)?);
                }

                // Pre-commit.
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
