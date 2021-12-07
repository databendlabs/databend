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
pub struct Reverse {}

impl StringOperator for Reverse {
    #[inline]
    fn apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> usize {
        let mut tmp = Vec::new();
        tmp.copy_from_slice(s);
        tmp.reverse();
        buffer.copy_from_slice(tmp.as_slice());
        s.len()
    }

    fn estimate_bytes(&self, array: &DFStringArray) -> usize {
        array.into_no_null_iter().fold(0, |mut total_bytes, x| {
            total_bytes += x.len();
            total_bytes
        })
    }
}

pub type ReverseFunction = String2StringFunction<Reverse>;
