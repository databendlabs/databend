use std::sync::atomic::Ordering::SeqCst;
use std::thread;
use std::time::Duration;

use databend_common_base::runtime::compress_time_point;
use databend_common_base::runtime::TimeSeriesProfileName;
use databend_common_base::runtime::TimeSeriesProfiles;
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
use databend_common_exception::Result;

#[test]
fn test_time_series_profile_record() -> Result<()> {
    let profile = TimeSeriesProfiles::new();

    // 1. For the first record, we need only increase the inner counter
    profile.record(TimeSeriesProfileName::OutputBytes, 1000);
    let value = profile.profiles[TimeSeriesProfileName::OutputBytes as usize]
        .value
        .load(SeqCst);
    let len = profile.profiles[TimeSeriesProfileName::OutputBytes as usize]
        .points
        .len();
    assert_eq!(value, 1000);
    assert_eq!(len, 0);

    thread::sleep(Duration::from_secs(1));

    // 2. for next time slot, we need to add a new point
    // after that, we need to reset the counter
    profile.record(TimeSeriesProfileName::OutputBytes, 1234);
    let value = profile.profiles[TimeSeriesProfileName::OutputBytes as usize]
        .value
        .load(SeqCst);
    let len = profile.profiles[TimeSeriesProfileName::OutputBytes as usize]
        .points
        .len();
    assert_eq!(value, 1234);
    assert_eq!(len, 1);
    // the first point should be 1000
    let first = profile.profiles[TimeSeriesProfileName::OutputBytes as usize]
        .points
        .pop()
        .unwrap();
    assert_ne!(first.0, 0);
    assert_eq!(first.1, 1000);

    Ok(())
}
#[test]
fn test_compress_time_point_basic() {
    let input = vec![
        (1744971865, 100),
        (1744971866, 200),
        (1744971867, 50),
        (1744971868, 150),
        (1744971870, 20),
        (1744971871, 40),
    ];
    let expected = vec![vec![1744971865, 100, 200, 50, 150], vec![
        1744971870, 20, 40,
    ]];
    assert_eq!(compress_time_point(&input), expected);
}

#[test]
fn test_compress_time_point_no_consecutive() {
    let input = vec![(1744971865, 10), (1744971867, 20), (1744971869, 30)];
    let expected = vec![vec![1744971865, 10], vec![1744971867, 20], vec![
        1744971869, 30,
    ]];
    assert_eq!(compress_time_point(&input), expected);
}

#[test]
fn test_finish_flush() {
    let profile = TimeSeriesProfiles::new();

    profile.record(TimeSeriesProfileName::OutputBytes, 2000);
    profile.record(TimeSeriesProfileName::OutputRows, 1000);

    thread::sleep(Duration::from_secs(1));

    // Finish flush will read this from `profile.value` and append it to the points
    profile.record(TimeSeriesProfileName::OutputBytes, 2);
    profile.record(TimeSeriesProfileName::OutputRows, 1);

    let batch = profile.flush(true, &mut 4);

    assert_eq!(batch.len(), 2);
    // [[timestamp, 1000, 1]]
    assert_eq!(batch[0].1[0].len(), 3);
    // [[timestamp, 2000, 2]]
    assert_eq!(batch[1].1[0].len(), 3);
}
