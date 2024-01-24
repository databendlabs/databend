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
    SpillWriteCount,
    SpillWriteBytes,
    SpillWriteTime,
    SpillReadCount,
    SpillReadBytes,
    SpillReadTime,
}

impl Display for ProfileStatisticsName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<usize> for ProfileStatisticsName {
    fn from(value: usize) -> Self {
        let statistics_index = get_statistics_name_index();

        if value > statistics_index.len() {
            panic!("logical error");
        }

        match &statistics_index[value] {
            None => panic!("logical error"),
            Some(statistics_name) => statistics_name.clone(),
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileDesc {
    desc: &'static str,
    display_name: &'static str,
    index: usize,
}

pub static PROFILES_DESC: OnceCell<Arc<HashMap<ProfileStatisticsName, ProfileDesc>>> =
    OnceCell::new();

pub static PROFILES_INDEX: OnceCell<
    Arc<[Option<ProfileStatisticsName>; std::mem::variant_count::<ProfileStatisticsName>()]>,
> = OnceCell::new();

fn get_statistics_name_index()
-> Arc<[Option<ProfileStatisticsName>; std::mem::variant_count::<ProfileStatisticsName>()]> {
    PROFILES_INDEX
        .get_or_init(|| {
            let statistics_desc = get_statistics_desc();
            let mut statistics_index = std::array::from_fn(|_v| None);

            for (k, v) in statistics_desc.iter() {
                statistics_index[v.index] = Some(k.clone());
            }

            Arc::new(statistics_index)
        })
        .clone()
}

pub fn get_statistics_desc() -> Arc<HashMap<ProfileStatisticsName, ProfileDesc>> {
    PROFILES_DESC.get_or_init(|| {
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
            (ProfileStatisticsName::SpillWriteCount, ProfileDesc {
                display_name: "numbers spilled",
                desc: "The number of spilled",
                index: ProfileStatisticsName::SpillWriteCount as usize,
            }),
            (ProfileStatisticsName::SpillWriteBytes, ProfileDesc {
                display_name: "bytes spilled",
                desc: "The bytes spilled of query",
                index: ProfileStatisticsName::SpillWriteBytes as usize,
            }),
        ]))
    }).clone()
}
