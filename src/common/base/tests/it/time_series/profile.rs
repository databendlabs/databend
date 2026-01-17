use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::thread;
use std::time::Duration;

use databend_common_base::runtime::ProfilePoints;
use databend_common_base::runtime::QueryTimeSeriesProfile;
use databend_common_base::runtime::TimeSeriesProfileName;
use databend_common_base::runtime::TimeSeriesProfiles;
use databend_common_base::runtime::compress_time_point;
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

#[test]
fn test_time_series_profile_record() -> anyhow::Result<()> {
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
fn test_compress_time_point_special_duplicate_merge() {
    let input = vec![
        (1744971865, 100),
        (1744971866, 200),
        // same timestamp, we will merge
        (1744971867, 50),
        (1744971867, 123),
        (1744971868, 150),
        (1744971870, 20),
        (1744971871, 40),
    ];
    let expected = vec![vec![1744971865, 100, 200, 50 + 123, 150], vec![
        1744971870, 20, 40,
    ]];
    assert_eq!(compress_time_point(&input), expected);
}

#[test]
fn test_compress_time_point_special_invalid() {
    let input = vec![
        (1744971865, 100),
        (1744971866, 200),
        (1744971867, 50),
        // extreme case, we will not merge it
        (1744971865, 123),
        (1744971868, 150),
        (1744971870, 20),
        (1744971871, 40),
    ];
    let expected = vec![
        vec![1744971865, 100, 200, 50],
        vec![1744971865, 123],
        vec![1744971868, 150],
        vec![1744971870, 20, 40],
    ];
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
    // sleep(1) is not exactly 1 second sleep time, but **at least** 1 second.
    if batch[0].len() == 1 {
        // 99.9%: [[timestamp, 1000, 1]]
        assert_eq!(batch[0][0][1], 1000);
        assert_eq!(batch[0][0][2], 1);
    } else {
        // 0.1%: [[timestamp, 1000], [timestamp+2, 1]]
        assert_eq!(batch[0][0][1], 1000);
        assert_eq!(batch[0][1][1], 1);
    }
    if batch[1].len() == 1 {
        // 99.9%: [[timestamp, 2000, 2]]
        assert_eq!(batch[1][0][1], 2000);
        assert_eq!(batch[1][0][2], 2);
    } else {
        // 0.1%: [[timestamp, 2000], [timestamp+2, 2]]
        assert_eq!(batch[1][0][1], 2000);
        assert_eq!(batch[1][1][1], 2);
    }
}

#[test]
fn test_record_inner_basic() -> anyhow::Result<()> {
    let points = ProfilePoints::new();
    let now = chrono::Utc::now().timestamp() as usize;

    // Simulate recording in the same time slot
    for i in 0..10 {
        points.record_time_slot(now, i);
    }
    assert_eq!(points.points.len(), 0);
    assert_eq!(points.value.load(SeqCst), (0..10).sum::<usize>());

    // Next time slot
    for i in 0..100 {
        points.record_time_slot(now + 1, i);
    }
    assert_eq!(points.points.len(), 1);
    let x = points.points.pop().unwrap();
    assert_eq!(x.0, now);
    assert_eq!(x.1, (0..10).sum::<usize>());
    assert_eq!(points.value.load(SeqCst), (0..100).sum::<usize>());
    Ok(())
}

#[test]
fn test_record_inner_special() -> anyhow::Result<()> {
    // Simulate concurrently recording but one thread is late
    let points = ProfilePoints::new();
    let now = 1000000001_usize;
    for i in 0..10 {
        points.record_time_slot(now, i);
    }
    points.record_time_slot(now + 1, 123);
    points.record_time_slot(now, 456);
    points.record_time_slot(now + 2, 789);

    let v = points.points.try_iter().collect::<Vec<_>>();

    assert_eq!(
        format!("{:?}", v),
        "[(1000000001, 45), (1000000001, 456), (1000000002, 123)]"
    );

    assert_eq!(points.value.load(SeqCst), 789);
    Ok(())
}

#[test]
fn test_record_inner_special_invalid() -> anyhow::Result<()> {
    // Simulate concurrently recording but one thread is later than 1 second
    let points = ProfilePoints::new();
    let now = 1000000001_usize;
    for i in 0..10 {
        points.record_time_slot(now, i);
    }
    points.record_time_slot(now + 1, 123);
    points.record_time_slot(now - 2, 456);
    points.record_time_slot(now + 2, 789);

    let v = points.points.try_iter().collect::<Vec<_>>();

    assert_eq!(
        format!("{:?}", v),
        "[(1000000001, 45), (999999999, 456), (1000000002, 123)]"
    );

    assert_eq!(points.value.load(SeqCst), 789);
    Ok(())
}

#[test]
fn test_should_flush() -> anyhow::Result<()> {
    let global_count = AtomicUsize::new(0);
    for _i in 0..1023 {
        let query_profile = QueryTimeSeriesProfile::should_flush(&global_count);
        assert!(!query_profile);
    }
    let query_profile = QueryTimeSeriesProfile::should_flush(&global_count);
    assert!(query_profile);
    assert_eq!(global_count.load(SeqCst), 0);
    for _i in 0..1023 {
        let query_profile = QueryTimeSeriesProfile::should_flush(&global_count);
        assert!(!query_profile);
    }
    let query_profile = QueryTimeSeriesProfile::should_flush(&global_count);
    assert!(query_profile);
    assert_eq!(global_count.load(SeqCst), 0);

    Ok(())
}
