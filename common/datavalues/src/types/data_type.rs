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

use core::fmt;

use common_macros::MallocSizeOf;

// TODO REMOVE THIS
#[derive(
    serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, MallocSizeOf,
)]
pub enum DataType {
    Null,
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (16 bits), it's physical type is UInt16
    Date16,
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits), it's physical type is Int32
    Date32,

    /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
    /// in seconds, it's physical type is UInt32
    /// Option<String> indicates the timezone, if it's None, it's UTC
    DateTime32(Option<String>),

    /// A 64-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
    /// in nanoseconds, it's physical type is UInt64
    /// The time resolution is determined by the precision parameter, range from 0 to 9
    /// Typically are used - 3 (milliseconds), 6 (microseconds), 9 (nanoseconds)
    /// Option<String> indicates the timezone, if it's None, it's UTC
    DateTime64(u32, Option<String>),

    Interval(IntervalUnit),

    // List(Box<DataField>),
    // Struct(Vec<DataField>),
    String,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    MallocSizeOf,
)]
pub enum IntervalUnit {
    YearMonth,
    DayTime,
}

impl fmt::Display for IntervalUnit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match self {
            IntervalUnit::YearMonth => "YearMonth",
            IntervalUnit::DayTime => "DayTime",
        };
        write!(f, "{}", str)
    }
}
