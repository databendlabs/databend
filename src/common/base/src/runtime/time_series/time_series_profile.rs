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
use std::mem::variant_count;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use concurrent_queue::ConcurrentQueue;
use log::info;
use serde::Serialize;

use crate::runtime::profile::ProfileStatisticsName;

// 1 second in milliseconds
const DEFAULT_INTERVAL: usize = 1000;

// Flush to log every 10 records
const DEFAULT_BATCH_SIZE: usize = 10;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize, Debug)]
pub enum TimeSeriesProfileStatisticsName {
    OutputRows,
    OutputBytes,
}

impl From<ProfileStatisticsName> for Option<TimeSeriesProfileStatisticsName> {
    fn from(value: ProfileStatisticsName) -> Self {
        match value {
            ProfileStatisticsName::OutputRows => Some(TimeSeriesProfileStatisticsName::OutputRows),
            ProfileStatisticsName::OutputBytes => {
                Some(TimeSeriesProfileStatisticsName::OutputBytes)
            }
            _ => None,
        }
    }
}

impl Display for TimeSeriesProfileStatisticsName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeSeriesProfileStatisticsName::OutputRows => write!(f, "output_rows"),
            TimeSeriesProfileStatisticsName::OutputBytes => write!(f, "output_bytes"),
        }
    }
}

impl From<usize> for TimeSeriesProfileStatisticsName {
    fn from(value: usize) -> Self {
        match value {
            0 => TimeSeriesProfileStatisticsName::OutputRows,
            1 => TimeSeriesProfileStatisticsName::OutputBytes,
            _ => unreachable!("Invalid TimeSeriesProfileStatisticsName"),
        }
    }
}

pub struct TimeSeriesProfileItem {
    // Every record is a tuple of (timestamp, value)
    queue: ConcurrentQueue<(usize, usize)>,
    last_record_timestamp: AtomicUsize,
    name: TimeSeriesProfileStatisticsName,
}

pub struct TimeSeriesProfile {
    pub items: Vec<TimeSeriesProfileItem>,
    pub plan_id: u32,
    pub query_id: String,
}

impl TimeSeriesProfile {
    fn create_items() -> Vec<TimeSeriesProfileItem> {
        let len = variant_count::<TimeSeriesProfileStatisticsName>();
        let mut items = Vec::with_capacity(len);
        for i in 0..len {
            items.push(TimeSeriesProfileItem {
                queue: ConcurrentQueue::unbounded(),
                last_record_timestamp: AtomicUsize::new(0),
                name: TimeSeriesProfileStatisticsName::from(i),
            });
        }
        items
    }

    pub fn create(plan_id: u32, query_id: String) -> Arc<Self> {
        Arc::new(TimeSeriesProfile {
            items: Self::create_items(),
            plan_id,
            query_id,
        })
    }

    pub fn record_point(&self, name: ProfileStatisticsName, value: usize) {
        let time_series_name: Option<TimeSeriesProfileStatisticsName> = name.into();
        if time_series_name.is_none() {
            return;
        }
        let item = &self.items[time_series_name.unwrap() as usize];
        let now =
            chrono::Local::now().timestamp_millis() as usize / DEFAULT_INTERVAL * DEFAULT_INTERVAL;
        let mut current_last_record = 0;
        loop {
            match item.last_record_timestamp.compare_exchange_weak(
                current_last_record,
                now,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    item.queue
                        .push((now, value))
                        .expect("Failed to push to queue");
                    if item.queue.len() >= DEFAULT_BATCH_SIZE {
                        self.flush(item);
                    }
                    break;
                }
                Err(last_record) => {
                    if now - DEFAULT_INTERVAL < last_record {
                        break;
                    }
                    current_last_record = last_record;
                }
            }
        }
    }

    pub fn flush(&self, item: &TimeSeriesProfileItem) {
        let batch = Vec::from_iter(item.queue.try_iter());
        if batch.is_empty() {
            return;
        }
        #[derive(Serialize)]
        struct TimeSeriesStatistics {
            pub query_id: String,
            pub plan_id: u32,
            pub stats_name: String,
            pub stats: Vec<(usize, usize)>,
        }
        info!(
            target: "databend::log::time_series",
            "{}",
            serde_json::to_string(&TimeSeriesStatistics{
                query_id: self.query_id.clone(),
                plan_id: self.plan_id,
                stats_name: item.name.to_string(),
                stats: batch,
            }).unwrap()
        );
    }

    pub fn finish(&self) {
        for item in self.items.iter() {
            self.flush(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    #[test]
    fn test_time_series_profile() {
        let time_series_profile = TimeSeriesProfile::create(1, "test_query_id".to_string());
        time_series_profile.record_point(ProfileStatisticsName::OutputRows, 100);
        time_series_profile.record_point(ProfileStatisticsName::OutputRows, 200);
        time_series_profile.record_point(ProfileStatisticsName::OutputBytes, 400);
        let batch = Vec::from_iter(
            time_series_profile.items[TimeSeriesProfileStatisticsName::OutputRows as usize]
                .queue
                .try_iter(),
        );
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].1, 100);
        let batch = Vec::from_iter(
            time_series_profile.items[TimeSeriesProfileStatisticsName::OutputBytes as usize]
                .queue
                .try_iter(),
        );
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].1, 400);
        std::thread::sleep(Duration::from_millis(1100));
        time_series_profile.record_point(ProfileStatisticsName::OutputRows, 300);
        let batch = Vec::from_iter(
            time_series_profile.items[TimeSeriesProfileStatisticsName::OutputRows as usize]
                .queue
                .try_iter(),
        );
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].1, 300);
    }

    #[test]
    fn test_time_series_enum() {
        let len = variant_count::<TimeSeriesProfileStatisticsName>();
        for i in 0..len {
            let name = TimeSeriesProfileStatisticsName::from(i);
            assert_eq!(name as usize, i);
        }
    }
}
