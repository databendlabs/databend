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

use common_datavalues::Column;
use common_datavalues::StringColumn;
use common_exception::Result;

use crate::scalars::strings::String2StringFunction;
use crate::scalars::strings::StringOperator;

#[derive(Clone, Default)]
pub struct Blake3Hash {}

impl StringOperator for Blake3Hash {
    #[inline]
    fn try_apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> Result<usize> {
        let buffer = &mut buffer[0..64];
        // TODO blake3 lib doesn't allow encode into buffer...
        hex::encode_to_slice(blake3::hash(s).as_bytes(), buffer).unwrap();
        Ok(64)
    }

    fn estimate_bytes(&self, array: &StringColumn) -> usize {
        array.len() * 64
    }
}

pub type Blake3HashFunction = String2StringFunction<Blake3Hash>;
