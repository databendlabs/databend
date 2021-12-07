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
pub struct Quote {}

impl StringOperator for Quote {
    #[inline]
    fn apply<'a>(&'a mut self, value: &'a [u8], buffer: &mut [u8]) -> usize {
        let mut len = 0;
        for ch in value {
            match *ch {
                0 | b'\'' | b'\"' | 8 | b'\n' | b'\r' | b'\t' | b'\\' => len += 2,
                _ => len += 1,
            };
            match *ch {
                0 => buffer.copy_from_slice(&[b'\\', b'0']),
                b'\'' => buffer.copy_from_slice(&[b'\\', b'\'']),
                b'\"' => buffer.copy_from_slice(&[b'\\', b'\"']),
                8 => buffer.copy_from_slice(&[b'\\', b'b']),
                b'\n' => buffer.copy_from_slice(&[b'\\', b'n']),
                b'\r' => buffer.copy_from_slice(&[b'\\', b'r']),
                b'\t' => buffer.copy_from_slice(&[b'\\', b't']),
                b'\\' => buffer.copy_from_slice(&[b'\\', b'\\']),
                _ => buffer.copy_from_slice(&[*ch]),
            };
        }
        len
    }

    fn estimate_bytes(&self, array: &DFStringArray) -> usize {
        array.into_no_null_iter().fold(0, |mut total_bytes, x| {
            total_bytes += x.len() * 2;
            total_bytes
        })
    }
}

pub type QuoteFunction = String2StringFunction<Quote>;
