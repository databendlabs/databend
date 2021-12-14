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

use common_datavalues::DataType;
use common_exception::Result;

use super::NumberResultFunction;
use super::String2NumberFunction;

#[derive(Clone)]
pub struct Length {}

impl NumberResultFunction<u64> for Length {
    const IS_DETERMINISTIC: bool = true;
    const MAYBE_MONOTONIC: bool = false;

    fn return_type() -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn to_number(value: &[u8]) -> u64 {
        value.len() as u64
    }
}

pub type LengthFunction = String2NumberFunction<Length, u64>;
