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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::vec;

use arrow_flight::FlightData;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use bytes::Bytes;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::statistics::data_cache_statistics::DataCacheMetricValues;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::PlanProfile;
use databend_common_storage::CopyStatus;
use databend_common_storage::MutationStatus;
use log::error;

use crate::servers::flight::v1::packets::ProgressInfo;

pub struct FragmentData {
    meta: Bytes,
    pub data: FlightData,
}

impl FragmentData {
    pub fn get_meta(&self) -> &[u8] {
        &self.meta[0..self.meta.len() - 1]
    }

    pub fn create(meta: Bytes, data: FlightData) -> FragmentData {
        FragmentData { meta, data }
    }
}

impl Debug for FragmentData {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("FragmentData").finish()
    }
}

#[derive(Debug)]
pub enum DataPacket {
    ErrorCode(ErrorCode),
    Dictionary(FlightData),
    FragmentData(FragmentData),
    QueryProfiles(HashMap<u32, PlanProfile>),
    SerializeProgress(Vec<ProgressInfo>),
    CopyStatus(CopyStatus),
    MutationStatus(MutationStatus),
    DataCacheMetrics(DataCacheMetricValues),
    PartStatistics(HashMap<u32, PartStatistics>),
    QueryPerf(String),
}

fn calc_size(flight_data: &FlightData) -> usize {
    flight_data.data_body.len() + flight_data.data_header.len() + flight_data.app_metadata.len()
}

impl DataPacket {
    pub fn bytes_size(&self) -> usize {
        match self {
            DataPacket::ErrorCode(_) => 0,
            DataPacket::CopyStatus(_) => 0,
            DataPacket::MutationStatus(_) => 0,
            DataPacket::SerializeProgress(_) => 0,
            DataPacket::Dictionary(v) => calc_size(v),
            DataPacket::FragmentData(v) => calc_size(&v.data) + v.meta.len(),
            DataPacket::QueryProfiles(_) => 0,
            DataPacket::DataCacheMetrics(_) => 0,
            DataPacket::QueryPerf(_) => 0,
            DataPacket::PartStatistics(_) => 0,
        }
    }
}

impl TryFrom<DataPacket> for FlightData {
    type Error = ErrorCode;

    fn try_from(packet: DataPacket) -> Result<Self> {
        Ok(match packet {
            DataPacket::ErrorCode(error) => {
                error!("Got error code data packet: {:?}", error);
                FlightData::from(error)
            }
            DataPacket::FragmentData(fragment_data) => FlightData::from(fragment_data),
            DataPacket::QueryProfiles(profiles) => FlightData {
                app_metadata: vec![0x03].into(),
                data_body: serde_json::to_vec(&profiles)?.into(),
                data_header: Default::default(),
                flight_descriptor: None,
            },
            DataPacket::SerializeProgress(progress) => {
                let mut data_body = vec![];
                data_body.write_u64::<BigEndian>(progress.len() as u64)?;

                // Progress.
                for progress_info in progress {
                    progress_info.write(&mut data_body)?;
                }

                FlightData {
                    data_body: data_body.into(),
                    data_header: Default::default(),
                    flight_descriptor: None,
                    app_metadata: vec![0x04].into(),
                }
            }
            DataPacket::Dictionary(mut flight_data) => {
                let mut app_metadata = flight_data.app_metadata.to_vec();
                app_metadata.push(0x05);
                flight_data.app_metadata = app_metadata.into();
                flight_data
            }
            DataPacket::CopyStatus(status) => FlightData {
                app_metadata: vec![0x06].into(),
                data_body: serde_json::to_vec(&status)?.into(),
                data_header: Default::default(),
                flight_descriptor: None,
            },
            DataPacket::MutationStatus(status) => FlightData {
                app_metadata: vec![0x07].into(),
                data_body: serde_json::to_vec(&status)?.into(),
                data_header: Default::default(),
                flight_descriptor: None,
            },
            DataPacket::DataCacheMetrics(metrics) => FlightData {
                app_metadata: vec![0x08].into(),
                data_body: serde_json::to_vec(&metrics)?.into(),
                data_header: Default::default(),
                flight_descriptor: None,
            },
            DataPacket::QueryPerf(query_perf) => FlightData {
                app_metadata: vec![0x09].into(),
                data_body: query_perf.into_bytes().into(),
                data_header: Default::default(),
                flight_descriptor: None,
            },
            DataPacket::PartStatistics(stat) => FlightData {
                app_metadata: vec![0x10].into(),
                data_body: serde_json::to_vec(&stat)?.into(),
                data_header: Default::default(),
                flight_descriptor: None,
            },
        })
    }
}

impl From<FragmentData> for FlightData {
    fn from(data: FragmentData) -> Self {
        let mut metadata = data.meta.to_vec();
        metadata.push(0x01);
        FlightData {
            app_metadata: metadata.into(),
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

        match flight_data.app_metadata.last().unwrap() {
            0x01 => Ok(DataPacket::FragmentData(FragmentData::try_from(
                flight_data,
            )?)),
            0x02 => Ok(DataPacket::ErrorCode(ErrorCode::try_from(flight_data)?)),
            0x03 => {
                let status =
                    serde_json::from_slice::<HashMap<u32, PlanProfile>>(&flight_data.data_body)?;
                Ok(DataPacket::QueryProfiles(status))
            }
            0x04 => {
                let mut bytes = flight_data.data_body.deref();
                let progress_size = bytes.read_u64::<BigEndian>()?;

                // Progress.
                let mut progress_info = Vec::with_capacity(progress_size as usize);
                for _index in 0..progress_size {
                    progress_info.push(ProgressInfo::read(&mut bytes)?);
                }

                Ok(DataPacket::SerializeProgress(progress_info))
            }
            0x05 => Ok(DataPacket::Dictionary(flight_data)),
            0x06 => {
                let status = serde_json::from_slice::<CopyStatus>(&flight_data.data_body)?;
                Ok(DataPacket::CopyStatus(status))
            }
            0x07 => {
                let status = serde_json::from_slice::<MutationStatus>(&flight_data.data_body)?;
                Ok(DataPacket::MutationStatus(status))
            }
            0x08 => {
                let status =
                    serde_json::from_slice::<DataCacheMetricValues>(&flight_data.data_body)?;
                Ok(DataPacket::DataCacheMetrics(status))
            }
            0x09 => {
                let query_perf = String::from_utf8(flight_data.data_body.to_vec())
                    .map_err(|_| ErrorCode::BadBytes("Invalid UTF-8 in query performance data."))?;
                Ok(DataPacket::QueryPerf(query_perf))
            }
            0x10 => {
                let stat =
                    serde_json::from_slice::<HashMap<u32, PartStatistics>>(&flight_data.data_body)?;
                Ok(DataPacket::PartStatistics(stat))
            }
            _ => Err(ErrorCode::BadBytes("Unknown flight data packet type.")),
        }
    }
}

impl TryFrom<FlightData> for FragmentData {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        Ok(FragmentData::create(flight_data.app_metadata, FlightData {
            app_metadata: Default::default(),
            flight_descriptor: None,
            data_body: flight_data.data_body,
            data_header: flight_data.data_header,
        }))
    }
}
