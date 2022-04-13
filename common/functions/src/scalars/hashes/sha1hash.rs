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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use sha1::Digest;

use crate::scalars::strings::String2StringFunction;
use crate::scalars::strings::StringOperator;

#[derive(Clone, Default)]
pub struct Sha1 {}

impl StringOperator for Sha1 {
    #[inline]
    fn try_apply<'a>(&'a mut self, s: &'a [u8], buffer: &mut [u8]) -> Result<usize> {
        let buffer = &mut buffer[0..40];
        // TODO sha1 lib doesn't allow encode into buffer...
        let mut m = ::sha1::Sha1::new();
        m.update(s);
        hex::encode_to_slice(m.finalize().as_slice(), buffer)
            .map_err(|e| ErrorCode::StrParseError(e.to_string()))?;
        Ok(40)
    }

    fn estimate_bytes(&self, array: &StringColumn) -> usize {
        array.len() * 40
    }
}

pub type Sha1HashFunction = String2StringFunction<Sha1>;
