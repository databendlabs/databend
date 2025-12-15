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

use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use concurrent_queue::ConcurrentQueue;
use once_cell::sync::OnceCell;
use serde::Serialize;

// 1 second in milliseconds
const DEFAULT_INTERVAL: usize = 1000;

// DataPoint is a tuple of (timestamp, value)
type DataPoint = (usize, usize);

pub enum TimeSeriesProfileName {
    OutputRows,
    OutputBytes,
}

#[derive(Serialize)]
pub struct TimeSeriesProfileDesc {
    name: &'static str,
    index: u32,
}
pub static TIME_SERIES_PROFILES_DESC: OnceCell<Arc<Vec<TimeSeriesProfileDesc>>> = OnceCell::new();

pub fn get_time_series_profile_desc() -> Arc<Vec<TimeSeriesProfileDesc>> {
    TIME_SERIES_PROFILES_DESC
        .get_or_init(|| {
            Arc::new(vec![
                TimeSeriesProfileDesc {
                    name: "OutputRows",
                    index: TimeSeriesProfileName::OutputRows as u32,
                },
                TimeSeriesProfileDesc {
                    name: "OutputBytes",
                    index: TimeSeriesProfileName::OutputBytes as u32,
                },
            ])
        })
        .clone()
}

pub struct ProfilePoints {
    pub points: ConcurrentQueue<DataPoint>,
    pub value: AtomicUsize,
    pub last_check_timestamp: AtomicUsize,
}

pub struct TimeSeriesProfiles {
    pub profiles: Vec<ProfilePoints>,
}

impl ProfilePoints {
    pub fn new() -> Self {
        ProfilePoints {
            points: ConcurrentQueue::unbounded(),
            last_check_timestamp: AtomicUsize::new(0),
            value: AtomicUsize::new(0),
        }
    }
    pub fn record_time_slot(&self, now: usize, value: usize) -> bool {
        let mut is_record = false;
        let mut current_last_check = 0;
        loop {
            match self.last_check_timestamp.compare_exchange_weak(
                current_last_check,
                now,
                SeqCst,
                SeqCst,
            ) {
                Ok(_) => {
                    if current_last_check == 0 {
                        // the first time, we will record it in next time slot
                        break;
                    }
                    if now == current_last_check {
                        // still in the same slot
                        break;
                    }
                    let last_value = self.value.swap(0, SeqCst);
                    let _ = self.points.push((current_last_check, last_value));
                    is_record = true;
                    break;
                }
                Err(last_record) => {
                    if now < last_record {
                        // for concurrent situation, `now` could be earlier than `last_record`
                        // that means we are missing the time slot, it is already push into
                        // the points queue. We just need to push the value into the queue again.
                        // will merge them in the flush
                        let _ = self.points.push((now, value));
                        // early return, should avoid adding value into this time slot
                        return true;
                    }
                    current_last_check = last_record;
                }
            }
        }
        self.value.fetch_add(value, SeqCst);
        is_record
    }
}

impl Default for ProfilePoints {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeSeriesProfiles {
    pub fn new() -> Self {
        let type_num = mem::variant_count::<TimeSeriesProfileName>();
        TimeSeriesProfiles {
            profiles: Self::create_profiles(type_num),
        }
    }

    fn create_profiles(type_num: usize) -> Vec<ProfilePoints> {
        let mut profiles = Vec::with_capacity(type_num);
        for _ in 0..type_num {
            profiles.push(ProfilePoints::new());
        }
        profiles
    }

    pub fn record(&self, name: TimeSeriesProfileName, value: usize) -> bool {
        let profile = &self.profiles[name as usize];
        // safe unwrap, time cannot back to UNIX_EPOCH
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as usize
            / DEFAULT_INTERVAL;
        profile.record_time_slot(now, value)
    }

    pub fn flush(&self, finish: bool, quota: &mut i32) -> Vec<Vec<Vec<usize>>> {
        let mut batch = Vec::with_capacity(self.profiles.len());
        for profile in self.profiles.iter() {
            if *quota == 0 && !finish {
                break;
            }
            if finish {
                // if flush called by finish, we need to flush the last record
                let last_timestamp = profile.last_check_timestamp.load(SeqCst);
                let last_value = profile.value.swap(0, SeqCst);
                if last_value != 0 && last_timestamp != 0 {
                    let _ = profile.points.push((last_timestamp, last_value));
                }
            }
            let mut points = Vec::with_capacity(profile.points.len());
            while let Ok(point) = profile.points.pop() {
                points.push(point);
                *quota -= 1;
                if *quota == 0 && !finish {
                    break;
                }
            }
            batch.push(compress_time_point(&points));
        }
        batch
    }
}

impl Default for TimeSeriesProfiles {
    fn default() -> Self {
        Self::new()
    }
}

/// Compresses a sequence of (`Vec<DataPoint>`, i.e., a list of (timestamp, value))
/// into a more compact format: `Vec<Vec<usize>>`.
///
/// Compressed format description:
/// - Each `Vec<usize>` represents a segment of consecutive timestamps.
/// - The first element of each `Vec` is the starting timestamp (start_time) of the segment.
/// - The following elements are the values corresponding to each consecutive timestamp in that segment.
///
/// Example:
///    given the original data:
///   `[(1744971865,100), (1744971866,200), (1744971867,50), (1744971868,150), (1744971870,20), (1744971871,40)]`
///   the compressed result will be:
///   `[[1744971865, 100, 200, 50, 150], [1744971870, 20, 40]]`
///
///   Because the data collection is highly competitive, the results may include entries
///   like [[1744971865, 100, 200, 50], [1744971866, 123], [1744971868, 150]], which means
///   the timestamp is not always strictly increasing
///
///
/// Note:
///   Why convert to `[timestamp, value0, value1, value2]` instead of `[timestamp, (value0, value1, value2)]`:
///   Rust serde_json will convert a tuple to a list. [timestamp, (value0, value1, value2)] will be converted to
///   `[timestamp, value0, value1, value2]` after serialization.
///   See: https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=3c153dfcfdde3032c80c05f4010f3d0f
pub fn compress_time_point(points: &[DataPoint]) -> Vec<Vec<usize>> {
    let mut result = Vec::new();
    let mut i = 0;
    while i < points.len() {
        let (start_time, value) = points[i];
        let mut group = Vec::new();
        group.push(start_time);
        group.push(value);
        let mut j = i + 1;
        while j < points.len()
            && (points[j].0 == points[j - 1].0 + 1 || points[j].0 == points[j - 1].0)
        {
            let mut v = points[j].1;
            if points[j].0 == points[j - 1].0 {
                v += group.pop().unwrap();
            }
            group.push(v);
            j += 1;
        }
        result.push(group);
        i = j;
    }
    result
}
