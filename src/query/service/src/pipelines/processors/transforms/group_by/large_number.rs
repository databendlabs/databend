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

use common_exception::ErrorCode;
use common_exception::Result;
use primitive_types::U256;
use primitive_types::U512;

pub trait LargeNumber: Default + Sized + 'static {
    const BYTE_SIZE: usize;
    fn serialize_to(&self, _bytes: &mut [u8]);
    fn from_bytes(v: &[u8]) -> Result<Self>;
}

impl LargeNumber for u128 {
    const BYTE_SIZE: usize = 16;
    fn serialize_to(&self, bytes: &mut [u8]) {
        let bs = self.to_le_bytes();
        bytes.copy_from_slice(&bs);
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        let bs: [u8; 16] = v.try_into().map_err(|_| {
            ErrorCode::StrParseError(format!(
                "Unable to parse into u128, unexpected byte size: {}",
                v.len()
            ))
        })?;
        Ok(u128::from_le_bytes(bs))
    }
}

impl LargeNumber for U256 {
    const BYTE_SIZE: usize = 32;
    fn serialize_to(&self, bytes: &mut [u8]) {
        self.to_little_endian(bytes);
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        Ok(U256::from_little_endian(v))
    }
}

impl LargeNumber for U512 {
    const BYTE_SIZE: usize = 64;
    fn serialize_to(&self, bytes: &mut [u8]) {
        self.to_little_endian(bytes);
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        Ok(U512::from_little_endian(v))
    }
}
