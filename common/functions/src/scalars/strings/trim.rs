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

use common_exception::Result;

use super::string2string::String2StringFunction;
use super::string2string::StringOperator;

#[derive(Clone, Default)]
pub struct LTrim;

impl StringOperator for LTrim {
    #[inline]
    fn try_apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> Result<usize> {
        for (idx, ch) in s.iter().enumerate() {
            if *ch != b' ' && *ch != b'\t' {
                let len = s.len() - idx;
                let buffer = &mut buffer[0..s.len() - idx];
                buffer.copy_from_slice(&s[idx..]);
                return Ok(len);
            }
        }
        Ok(0)
    }
}

#[derive(Clone, Default)]
pub struct RTrim;

impl StringOperator for RTrim {
    fn try_apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> Result<usize> {
        for (idx, ch) in s.iter().rev().enumerate() {
            if *ch != b' ' && *ch != b'\t' {
                let len = s.len() - idx;
                let buffer = &mut buffer[0..len];
                buffer.copy_from_slice(&s[..s.len() - idx]);
                return Ok(len);
            }
        }
        Ok(0)
    }
}

#[derive(Clone, Default)]
pub struct Trim;

impl StringOperator for Trim {
    fn try_apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> Result<usize> {
        let start_index = s.iter().position(|ch| *ch != b' ' && *ch != b'\t');
        let end_index = s.iter().rev().position(|ch| *ch != b' ' && *ch != b'\t');
        match (start_index, end_index) {
            (Some(start_index), Some(end_index)) => {
                let len = s.len() - end_index - start_index;
                let buffer = &mut buffer[0..len];
                buffer.copy_from_slice(&s[start_index..s.len() - end_index]);
                Ok(len)
            }
            (_, _) => Ok(0),
        }
    }
}

pub type LTrimFunction = String2StringFunction<LTrim>;
pub type RTrimFunction = String2StringFunction<RTrim>;
pub type TrimFunction = String2StringFunction<Trim>;
