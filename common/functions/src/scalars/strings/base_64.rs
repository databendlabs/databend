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

use common_datavalues::prelude::DFStringArray;

use super::string2string::String2StringFunction;
use super::string2string::StringOperator;

#[derive(Clone, Default)]
pub struct Encode {
    buf: Vec<u8>,
}

impl StringOperator for Encode {
    #[inline]
    fn apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> usize {
        self.buf.resize(s.len() * 4 / 3 + 4, 0);
        base64::encode_config_slice(s, base64::STANDARD, buffer)
    }

    fn estimate_bytes(&self, array: &DFStringArray) -> usize {
        array.inner().values().len() * 4 / 3 + array.len() * 4
    }
}

#[derive(Clone, Default)]
pub struct Decode {
    buf: Vec<u8>,
}

impl StringOperator for Decode {
    #[inline]
    fn apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> usize {
        self.buf.resize((s.len() + 3) / 4 * 3, 0);
        base64::decode_config_slice(s, base64::STANDARD, buffer).unwrap_or(0)
    }

    fn estimate_bytes(&self, array: &DFStringArray) -> usize {
        array.inner().values().len() * 4 / 3 + array.len() * 4
    }
}

pub type Base64EncodeFunction = String2StringFunction<Encode>;
pub type Base64DecodeFunction = String2StringFunction<Decode>;
