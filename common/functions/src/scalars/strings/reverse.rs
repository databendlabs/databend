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
pub struct Reverse {}

impl StringOperator for Reverse {
    #[inline]
    fn try_apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> Result<usize> {
        let buffer = &mut buffer[0..s.len()];
        buffer.copy_from_slice(s);
        buffer.reverse();
        Ok(s.len())
    }
}

pub type ReverseFunction = String2StringFunction<Reverse>;
