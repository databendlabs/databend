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

use super::string2string::String2StringFunction;
use super::string2string::StringOperator;

#[derive(Clone, Default)]
pub struct Encode {
    buf: Vec<u8>,
}

impl StringOperator for Encode {
    #[inline]
    fn apply<'a>(&'a mut self, s: &'a [u8]) -> Option<&'a [u8]> {
        self.buf.resize(s.len() * 4 / 3 + 4, 0);
        let bytes_written = base64::encode_config_slice(s, base64::STANDARD, &mut self.buf);
        Some(&self.buf[..bytes_written])
    }
}

#[derive(Clone, Default)]
pub struct Decode {
    buf: Vec<u8>,
}

impl StringOperator for Decode {
    #[inline]
    fn apply<'a>(&'a mut self, s: &'a [u8]) -> Option<&'a [u8]> {
        self.buf.resize((s.len() + 3) / 4 * 3, 0);
        match base64::decode_config_slice(s, base64::STANDARD, &mut self.buf) {
            Ok(bw) => Some(&self.buf[..bw]),
            Err(_) => None,
        }
    }
}

pub type Base64EncodeFunction = String2StringFunction<Encode>;
pub type Base64DecodeFunction = String2StringFunction<Decode>;
