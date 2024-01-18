// Copyright 2021 Datafuse Labs
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

use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub enum ProfileStatisticsName {
    /// The time spent to process in nanoseconds
    CpuTime,
    /// The time spent to wait in nanoseconds, usually used to
    /// measure the time spent on waiting for I/O
    WaitTime,
    ExchangeRows,
    ExchangeBytes,
    OutputRows,
    OutputBytes,
    ScanBytes,
    ScanCacheBytes,
    ScanPartitions,
    PartitionTotal,
}

impl Display for ProfileStatisticsName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProfileStatisticsName::CpuTime => write!(f, "CpuTime"),
            ProfileStatisticsName::WaitTime => write!(f, "WaitTime"),
            ProfileStatisticsName::ExchangeRows => write!(f, "ExchangeRows"),
            ProfileStatisticsName::ExchangeBytes => write!(f, "ExchangeBytes"),
            ProfileStatisticsName::OutputRows => write!(f, "OutputRows"),
            ProfileStatisticsName::OutputBytes => write!(f, "OutputBytes"),
            ProfileStatisticsName::ScanBytes => write!(f, "ScanBytes"),
            ProfileStatisticsName::ScanCacheBytes => write!(f, "ScanCacheBytes"),
            ProfileStatisticsName::ScanPartitions => write!(f, "ScanPartitions"),
            ProfileStatisticsName::PartitionTotal => write!(f, "PartitionTotal"),
        }
    }
}

impl From<usize> for ProfileStatisticsName {
    fn from(value: usize) -> Self {
        match value {
            _ if value == ProfileStatisticsName::CpuTime as usize => ProfileStatisticsName::CpuTime,
            _ if value == ProfileStatisticsName::WaitTime as usize => {
                ProfileStatisticsName::WaitTime
            }
            _ if value == ProfileStatisticsName::ExchangeRows as usize => {
                ProfileStatisticsName::ExchangeRows
            }
            _ if value == ProfileStatisticsName::ExchangeBytes as usize => {
                ProfileStatisticsName::ExchangeBytes
            }
            _ if value == ProfileStatisticsName::OutputRows as usize => {
                ProfileStatisticsName::OutputRows
            }
            _ if value == ProfileStatisticsName::OutputBytes as usize => {
                ProfileStatisticsName::OutputBytes
            }
            _ if value == ProfileStatisticsName::ScanBytes as usize => {
                ProfileStatisticsName::ScanBytes
            }
            _ if value == ProfileStatisticsName::ScanCacheBytes as usize => {
                ProfileStatisticsName::ScanCacheBytes
            }
            _ if value == ProfileStatisticsName::ScanPartitions as usize => {
                ProfileStatisticsName::ScanPartitions
            }
            _ if value == ProfileStatisticsName::PartitionTotal as usize => {
                ProfileStatisticsName::PartitionTotal
            }
            _ => panic!("logical error"),
        }
    }
}

#[derive(Clone)]
pub struct ProfileDesc {
    name: ProfileStatisticsName,
    desc: &'static str,
    display_name: &'static str,
}

pub static PROFILES_DESC: [ProfileDesc; std::mem::variant_count::<ProfileStatisticsName>()] = [
    ProfileDesc {
        name: ProfileStatisticsName::CpuTime,
        display_name: "cpu time",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::WaitTime,
        display_name: "wait time",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::ExchangeRows,
        display_name: "exchange rows",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::ExchangeBytes,
        display_name: "exchange bytes",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::OutputRows,
        display_name: "output rows",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::OutputBytes,
        display_name: "output bytes",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::ScanBytes,
        display_name: "bytes scanned",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::ScanCacheBytes,
        display_name: "bytes scanned from cache",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::ScanPartitions,
        display_name: "partitions scanned",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::PartitionTotal,
        display_name: "partitions total",
        desc: "",
    },
];
