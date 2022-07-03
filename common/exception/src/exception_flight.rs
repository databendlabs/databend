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

use common_arrow::arrow_format::flight::data::FlightData;
use crate::ErrorCode;
use crate::Result;

impl From<ErrorCode> for FlightData {
    fn from(error: ErrorCode) -> Self {
        // TODO: has stack trace
        FlightData {
            app_metadata: vec![0x02],
            data_body: error.message().into_bytes(),
            data_header: error.code().to_be_bytes().to_vec(),
            flight_descriptor: None,
        }
    }
}

impl TryFrom<FlightData> for ErrorCode {
    type Error = ErrorCode;

    fn try_from(flight_data: FlightData) -> Result<Self> {
        match flight_data.data_header.try_into() {
            Err(_) => Err(ErrorCode::BadBytes("Cannot parse inf usize.")),
            Ok(slice) => {
                let code = u16::from_be_bytes(slice);
                let message = String::from_utf8(flight_data.data_body)?;
                Ok(ErrorCode::create(code, message, None, None))
            }
        }
    }
}