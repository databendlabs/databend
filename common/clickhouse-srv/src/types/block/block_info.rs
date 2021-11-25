// Copyright 2021 Datafuse Labs.
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

use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Result;

#[allow(dead_code)]
#[derive(Copy, Clone)]
pub struct BlockInfo {
    num1: u64,
    is_overflows: bool,
    num2: u64,
    bucket_num: i32,
    num3: u64,
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self {
            num1: 0,
            is_overflows: false,
            num2: 0,
            bucket_num: -1,
            num3: 0,
        }
    }
}

impl BlockInfo {
    pub(crate) fn read<R: ReadEx>(reader: &mut R) -> Result<Self> {
        let block_info = Self {
            num1: reader.read_uvarint()?,
            is_overflows: reader.read_scalar()?,
            num2: reader.read_uvarint()?,
            bucket_num: reader.read_scalar()?,
            num3: reader.read_uvarint()?,
        };
        Ok(block_info)
    }

    pub fn write(&self, encoder: &mut Encoder) {
        encoder.uvarint(1);
        encoder.write(self.is_overflows);
        encoder.uvarint(2);
        encoder.write(self.bucket_num);
        encoder.uvarint(0);
    }
}
