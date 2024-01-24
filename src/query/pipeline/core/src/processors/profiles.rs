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

use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use once_cell::sync::OnceCell;

#[derive(Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize, Debug)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileDesc {
    desc: &'static str,
    display_name: &'static str,
    index: usize,
}

pub static PROFILES_DESC_NEW: OnceCell<Arc<HashMap<ProfileStatisticsName, ProfileDesc>>> =
    OnceCell::new();

pub fn get_statistics_desc() -> Arc<HashMap<ProfileStatisticsName, ProfileDesc>> {
    PROFILES_DESC_NEW.get_or_init(|| {
        Arc::new(HashMap::from([
            (ProfileStatisticsName::CpuTime, ProfileDesc {
                display_name: "cpu time",
                desc: "The time spent to process in nanoseconds",
                index: ProfileStatisticsName::CpuTime as usize,
            }),
            (ProfileStatisticsName::WaitTime, ProfileDesc {
                display_name: "wait time",
                desc: "The time spent to wait in nanoseconds, usually used to measure the time spent on waiting for I/O",
                index: ProfileStatisticsName::WaitTime as usize,
            }),
            (ProfileStatisticsName::ExchangeRows, ProfileDesc {
                display_name: "exchange rows",
                desc: "The number of data rows exchange between nodes in cluster mode",
                index: ProfileStatisticsName::ExchangeRows as usize,
            }),
            (ProfileStatisticsName::ExchangeBytes, ProfileDesc {
                display_name: "exchange bytes",
                desc: "The number of data bytes exchange between nodes in cluster mode",
                index: ProfileStatisticsName::ExchangeBytes as usize,
            }),
            (ProfileStatisticsName::OutputRows, ProfileDesc {
                display_name: "output rows",
                desc: "The number of rows from the physical plan output to the next physical plan",
                index: ProfileStatisticsName::OutputRows as usize,
            }),
            (ProfileStatisticsName::OutputBytes, ProfileDesc {
                display_name: "output bytes",
                desc: "The number of bytes from the physical plan output to the next physical plan",
                index: ProfileStatisticsName::OutputBytes as usize,
            }),
            (ProfileStatisticsName::ScanBytes, ProfileDesc {
                display_name: "bytes scanned",
                desc: "The bytes scanned of query",
                index: ProfileStatisticsName::ScanBytes as usize,
            }),
            (ProfileStatisticsName::ScanCacheBytes, ProfileDesc {
                display_name: "bytes scanned from cache",
                desc: "The bytes scanned from cache of query",
                index: ProfileStatisticsName::ScanCacheBytes as usize,
            }),
            (ProfileStatisticsName::ScanPartitions, ProfileDesc {
                display_name: "partitions scanned",
                desc: "The partitions scanned of query",
                index: ProfileStatisticsName::ScanPartitions as usize,
            }),
            (ProfileStatisticsName::PartitionTotal, ProfileDesc {
                display_name: "partitions total",
                desc: "The partitions total of table",
                index: ProfileStatisticsName::PartitionTotal as usize,
            }),
        ]))
    }).clone()
}
