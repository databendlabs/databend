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

use common_datavalues::StringColumn;
use common_exception::Result;

use super::string2string::String2StringFunction;
use super::string2string::StringOperator;

#[derive(Clone, Default)]
pub struct Quote {}

impl StringOperator for Quote {
    #[inline]
    fn try_apply<'a>(&'a mut self, value: &'a [u8], buffer: &mut [u8]) -> Result<usize> {
        let mut offset = 0;
        for ch in value {
            match *ch {
                0 => {
                    let x = &mut buffer[offset..offset + 2];
                    x.copy_from_slice(&[b'\\', b'0']);
                    offset += 2;
                }
                b'\'' => {
                    let x = &mut buffer[offset..offset + 2];
                    x.copy_from_slice(&[b'\\', b'\'']);
                    offset += 2;
                }
                b'\"' => {
                    let x = &mut buffer[offset..offset + 2];
                    x.copy_from_slice(&[b'\\', b'\"']);
                    offset += 2;
                }
                8 => {
                    let x = &mut buffer[offset..offset + 2];
                    x.copy_from_slice(&[b'\\', b'b']);
                    offset += 2;
                }
                b'\n' => {
                    let x = &mut buffer[offset..offset + 2];
                    x.copy_from_slice(&[b'\\', b'n']);
                    offset += 2;
                }
                b'\r' => {
                    let x = &mut buffer[offset..offset + 2];
                    x.copy_from_slice(&[b'\\', b'r']);
                    offset += 2;
                }
                b'\t' => {
                    let x = &mut buffer[offset..offset + 2];
                    x.copy_from_slice(&[b'\\', b't']);
                    offset += 2;
                }
                b'\\' => {
                    let x = &mut buffer[offset..offset + 2];
                    x.copy_from_slice(&[b'\\', b'\\']);
                    offset += 2;
                }
                _ => {
                    let x = &mut buffer[offset..offset + 1];
                    x.copy_from_slice(&[*ch]);
                    offset += 1;
                }
            };
        }
        Ok(offset)
    }

    fn estimate_bytes(&self, array: &StringColumn) -> usize {
        array.values().len() * 2
    }
}

pub type QuoteFunction = String2StringFunction<Quote>;
