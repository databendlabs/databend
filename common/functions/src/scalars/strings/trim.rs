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

use super::string2string::String2StringFunction;
use super::string2string::StringOperator;

#[derive(Clone, Default)]
pub struct LTrim;

impl StringOperator for LTrim {
    #[inline]
    fn apply<'a>(&'a mut self, s: &'a [u8], mut buffer: &mut [u8]) -> (usize, bool) {
        for (idx, ch) in s.iter().enumerate() {
            if *ch != b' ' && *ch != b'\t' {
                // return Some(&s[idx..]);
                return (buffer.write(&s[idx..]).unwrap_or(0), false);
            }
        }
        (0, false)
    }
}

#[derive(Clone, Default)]
pub struct RTrim;

impl StringOperator for RTrim {
    fn apply<'a>(&'a mut self, s: &'a [u8], mut buffer: &mut [u8]) -> (usize, bool) {
        for (idx, ch) in s.iter().rev().enumerate() {
            if *ch != b' ' && *ch != b'\t' {
                return (buffer.write(&s[..s.len() - idx]).unwrap_or(0), false);
            }
        }
        (0, false)
    }
}

#[derive(Clone, Default)]
pub struct Trim;

impl StringOperator for Trim {
    fn apply<'a>(&'a mut self, s: &'a [u8], mut buffer: &mut [u8]) -> (usize, bool) {
        let start_index = s.iter().position(|ch| *ch != b' ' && *ch != b'\t');
        let end_index = s.iter().rev().position(|ch| *ch != b' ' && *ch != b'\t');
        match (start_index, end_index) {
            (Some(start_index), Some(end_index)) => (
                buffer
                    .write(&s[start_index..s.len() - end_index])
                    .unwrap_or(0),
                false,
            ),
            (Some(start_index), None) => (buffer.write(&s[start_index..]).unwrap_or(0), false),
            (None, Some(end_index)) => {
                (buffer.write(&s[..s.len() - end_index]).unwrap_or(0), false)
            }
            (None, None) => (0, false),
        }
    }
}

pub type LTrimFunction = String2StringFunction<LTrim>;
pub type RTrimFunction = String2StringFunction<RTrim>;
pub type TrimFunction = String2StringFunction<Trim>;
