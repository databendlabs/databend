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
pub struct Quote {
    buffer: Vec<u8>,
}

impl StringOperator for Quote {
    #[inline]
    fn apply<'a>(&'a mut self, value: &'a [u8]) -> Option<&'a [u8]> {
        self.buffer.clear();

        for ch in value {
            match *ch {
                0 => self.buffer.extend_from_slice(&[b'\\', b'0']),
                b'\'' => self.buffer.extend_from_slice(&[b'\\', b'\'']),
                b'\"' => self.buffer.extend_from_slice(&[b'\\', b'\"']),
                8 => self.buffer.extend_from_slice(&[b'\\', b'b']),
                b'\n' => self.buffer.extend_from_slice(&[b'\\', b'n']),
                b'\r' => self.buffer.extend_from_slice(&[b'\\', b'r']),
                b'\t' => self.buffer.extend_from_slice(&[b'\\', b't']),
                b'\\' => self.buffer.extend_from_slice(&[b'\\', b'\\']),
                _ => self.buffer.push(*ch),
            }
        }

        Some(&self.buffer[..self.buffer.len()])
    }
}

pub type QuoteFunction = String2StringFunction<Quote>;
