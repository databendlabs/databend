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

use std::io::Write;

use common_datavalues::prelude::DFStringArray;

use super::string2string::String2StringFunction;
use super::string2string::StringOperator;
#[derive(Clone, Default)]
pub struct Quote {}

impl StringOperator for Quote {
    #[inline]
    fn apply_with_no_null<'a>(&'a mut self, value: &'a [u8], mut buffer: &mut [u8]) -> usize {
        let mut len = 0;
        for ch in value {
            match *ch {
                0 => len += buffer.write(&[b'\\', b'0']).unwrap_or(0),
                b'\'' => len += buffer.write(&[b'\\', b'\'']).unwrap_or(0),
                b'\"' => len += buffer.write(&[b'\\', b'\"']).unwrap_or(0),
                8 => len += buffer.write(&[b'\\', b'b']).unwrap_or(0),
                b'\n' => len += buffer.write(&[b'\\', b'n']).unwrap_or(0),
                b'\r' => len += buffer.write(&[b'\\', b'r']).unwrap_or(0),
                b'\t' => len += buffer.write(&[b'\\', b't']).unwrap_or(0),
                b'\\' => len += buffer.write(&[b'\\', b'\\']).unwrap_or(0),
                _ => len += buffer.write(&[*ch]).unwrap_or(0),
            };
        }
        len
    }

    fn estimate_bytes(&self, array: &DFStringArray) -> usize {
        array.inner().values().len() * 2
    }
}

pub type QuoteFunction = String2StringFunction<Quote>;
