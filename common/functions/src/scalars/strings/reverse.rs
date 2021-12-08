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
pub struct Reverse {}

impl StringOperator for Reverse {
    #[inline]
    fn apply_with_no_null<'a>(&'a mut self, s: &'a [u8], mut buffer: &mut [u8]) -> usize {
        let mut tmp = Vec::new();
        tmp.extend_from_slice(s);
        tmp.reverse();
        buffer.write(tmp.as_slice()).unwrap_or(0)
    }
}

pub type ReverseFunction = String2StringFunction<Reverse>;
