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

use std::mem::size_of;
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::FlightData;

use crate::exception::ErrorCodeBacktrace;
use crate::ErrorCode;
use crate::Result;

impl From<ErrorCode> for FlightData {
    fn from(error: ErrorCode) -> Self {
        let error_message = error.message();
        let error_backtrace = error.backtrace_str();

        let message = error_message.as_bytes();
        let backtrace = error_backtrace.as_bytes();

        let mut data_body = Vec::with_capacity(16 + message.len() + backtrace.len());

        data_body.extend((message.len() as u64).to_be_bytes());
        data_body.extend_from_slice(message);
        data_body.extend((backtrace.len() as u64).to_be_bytes());
        data_body.extend_from_slice(backtrace);

        FlightData {
            data_body,
            app_metadata: vec![0x02],
            data_header: error.code().to_be_bytes().to_vec(),
            flight_descriptor: None,
        }
    }
}

fn read_be_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes[0..size_of::<u64>()].try_into().unwrap())
}

impl TryFrom<FlightData> for ErrorCode {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        match flight_data.data_header.try_into() {
            Err(_) => Err(ErrorCode::BadBytes("Cannot parse inf usize.")),
            Ok(slice) => {
                let code = u16::from_be_bytes(slice);
                let data_body = flight_data.data_body;

                let mut data_offset = 0;
                let message_len = read_be_u64(&data_body) as usize;
                data_offset += size_of::<u64>();
                let message = &data_body[data_offset..data_offset + message_len];
                data_offset += message_len;
                data_offset += size_of::<u64>();
                let backtrace = &data_body[data_offset..];

                match backtrace.is_empty() {
                    true => Ok(ErrorCode::create(
                        code,
                        String::from_utf8(message.to_vec())?,
                        None,
                        None,
                    )),
                    false => Ok(ErrorCode::create(
                        code,
                        String::from_utf8(message.to_vec())?,
                        None,
                        Some(ErrorCodeBacktrace::Serialized(Arc::new(String::from_utf8(
                            backtrace.to_vec(),
                        )?))),
                    )),
                }
            }
        }
    }
}
