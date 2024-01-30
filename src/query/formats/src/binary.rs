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

use base64::engine::general_purpose;
use base64::Engine as _;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::BinaryFormat;

fn encode_binary_hex(bytes: &[u8]) -> Vec<u8> {
    hex::encode_upper(bytes).into_bytes()
}

fn decode_binary_hex(bytes: &[u8]) -> Result<Vec<u8>> {
    hex::decode(bytes).map_err(|e| ErrorCode::BadBytes(format!("Illegal hex string: {}", e)))
}

fn encode_binary_base64(bytes: &[u8]) -> Vec<u8> {
    general_purpose::STANDARD.encode(bytes).into_bytes()
}

fn decode_binary_base64(bytes: &[u8]) -> Result<Vec<u8>> {
    general_purpose::STANDARD
        .decode(bytes)
        .map_err(|e| ErrorCode::BadBytes(format!("Illegal base64 string: {e}")))
}

pub fn encode_binary(bytes: &[u8], fmt: BinaryFormat) -> Vec<u8> {
    match fmt {
        BinaryFormat::Base64 => encode_binary_base64(bytes),
        BinaryFormat::Hex => encode_binary_hex(bytes),
    }
}

pub fn decode_binary(bytes: &[u8], fmt: BinaryFormat) -> Result<Vec<u8>> {
    match fmt {
        BinaryFormat::Base64 => decode_binary_base64(bytes),
        BinaryFormat::Hex => decode_binary_hex(bytes),
    }
}
