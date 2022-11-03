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

use super::string2number_try::TryNumberOperator;
use super::string2number_try::TryString2NumberFunction;
use super::NumberOperator;
use super::String2NumberFunction;

#[derive(Clone, Default)]
pub struct StringLength {}

#[derive(Clone, Default)]
pub struct StringUtf8Length {}

impl NumberOperator<u64> for StringLength {
    const IS_DETERMINISTIC: bool = true;
    const MAYBE_MONOTONIC: bool = false;

    fn apply<'a>(&'a mut self, value: &'a [u8]) -> u64 {
        value.len() as u64
    }
}

impl TryNumberOperator<u64> for StringUtf8Length {
    const IS_DETERMINISTIC: bool = true;
    const MAYBE_MONOTONIC: bool = false;

    fn try_apply<'a>(&'a mut self, value: &'a [u8]) -> Result<u64> {
        let s = std::str::from_utf8(value)?;
        Ok(s.chars().count() as u64)
    }
}

pub type StringLengthFunction = String2NumberFunction<StringLength, u64>;
pub type StringUtf8LengthFunction = TryString2NumberFunction<StringUtf8Length, u64>;
