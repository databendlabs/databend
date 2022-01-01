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

use std::hash::Hasher;

use common_datavalues::DataType;
use twox_hash::XxHash32;
use twox_hash::XxHash64;

use crate::scalars::strings::NumberResultFunction;
use crate::scalars::strings::String2NumberFunction;

#[derive(Clone)]
pub struct DfXxHash32 {}

impl NumberResultFunction<u32> for DfXxHash32 {
    const IS_DETERMINISTIC: bool = true;
    const MAYBE_MONOTONIC: bool = false;

    fn return_type(nullable: bool) -> DataType {
        DataType::UInt32(nullable)
    }

    fn to_number(value: &[u8]) -> u32 {
        let mut hasher = XxHash32::default();
        hasher.write(value);
        hasher.finish() as u32
    }
}

#[derive(Clone)]
pub struct DfXxHash64 {}

impl NumberResultFunction<u64> for DfXxHash64 {
    const IS_DETERMINISTIC: bool = true;
    const MAYBE_MONOTONIC: bool = false;

    fn return_type(nullable: bool) -> DataType {
        DataType::UInt64(nullable)
    }

    fn to_number(value: &[u8]) -> u64 {
        let mut hasher = XxHash64::default();
        hasher.write(value);
        hasher.finish()
    }
}

pub type XxHash32Function = String2NumberFunction<DfXxHash32, u32>;
pub type XxHash64Function = String2NumberFunction<DfXxHash64, u64>;
