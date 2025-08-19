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

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use databend_common_base::runtime::ExecutorStats;
use databend_common_base::runtime::ExecutorStatsSlot;
use databend_common_base::runtime::Thread;

#[test]
fn test_executor_stats_slot_basic() {
    let slot = ExecutorStatsSlot::new();
    let timestamp = 1000;

    // Test initial state
    let (ts, val) = slot.get();
    assert_eq!(ts, 0);
    assert_eq!(val, 0);

    // Test adding values
    slot.add(timestamp, 100);
    let (ts, val) = slot.get();
    assert_eq!(ts, timestamp);
    assert_eq!(val, 100);

    // Test accumulation with same timestamp
    slot.add(timestamp, 200);
    let (ts, val) = slot.get();
    assert_eq!(ts, timestamp);
    assert_eq!(val, 300);

    // Test different timestamp resets value
    slot.add(timestamp + 1, 50);
    let (ts, val) = slot.get();
    assert_eq!(ts, timestamp + 1);
    assert_eq!(val, 50);
}

#[test]
fn test_executor_stats_slot_overflow() {
    let slot = ExecutorStatsSlot::new();
    let timestamp = 2000;

    // Test saturation at u32::MAX
    slot.add(timestamp, u32::MAX - 10);
    slot.add(timestamp, 20); // Should saturate at u32::MAX
    let (ts, val) = slot.get();
    assert_eq!(ts, timestamp);
    assert_eq!(val, u32::MAX);
}

#[test]
fn test_executor_stats_slot_concurrent() {
    let slot = Arc::new(ExecutorStatsSlot::new());
    let timestamp = 4000;
    let num_threads = 10;
    let adds_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let slot = slot.clone();
            Thread::spawn(move || {
                for _ in 0..adds_per_thread {
                    slot.add(timestamp, 1);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // All threads should have accumulated their values
    let (ts, val) = slot.get();
    assert_eq!(ts, timestamp);
    assert_eq!(val, num_threads * adds_per_thread);
}

// --- record_process function tests ---

#[test]
fn test_record_process_single_second() {
    let stats = ExecutorStats::new();
    let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
    let elapsed_micros = 500_000; // 500ms = 500,000 microseconds
    let rows = 1000;

    stats.record_process(base_time, elapsed_micros, rows);

    let snapshot = stats.dump_snapshot();

    // Find the slot with data
    let mut found_time = false;
    let mut found_rows = false;

    for (ts, micros) in snapshot.process_time {
        if ts == 1000 && micros == 500_000 {
            found_time = true;
            break;
        }
    }

    for (ts, row_count) in snapshot.process_rows {
        if ts == 1000 && row_count == 1000 {
            found_rows = true;
            break;
        }
    }

    assert!(
        found_time,
        "Should find process time recorded in single second"
    );
    assert!(
        found_rows,
        "Should find process rows recorded in single second"
    );
}

#[test]
fn test_record_process_proportional_allocation() {
    let stats = ExecutorStats::new();
    // Start at 999.5 seconds, run for 1.0 seconds (exactly half in each second)
    let base_time = SystemTime::UNIX_EPOCH + Duration::from_millis(999_500);
    let elapsed_micros = 1_000_000; // 1 second = 1,000,000 microseconds
    let rows = 1000;

    stats.record_process(base_time, elapsed_micros, rows);

    let snapshot = stats.dump_snapshot();

    let mut micros_999 = 0;
    let mut micros_1000 = 0;
    let mut rows_999 = 0;
    let mut rows_1000 = 0;

    for (ts, micros) in snapshot.process_time {
        if ts == 999 {
            micros_999 = micros;
        } else if ts == 1000 {
            micros_1000 = micros;
        }
    }

    for (ts, row_count) in snapshot.process_rows {
        if ts == 999 {
            rows_999 = row_count;
        } else if ts == 1000 {
            rows_1000 = row_count;
        }
    }

    // Due to floating point precision issues, we check ranges rather than exact values
    assert!(
        micros_999 + micros_1000 <= 1_000_000,
        "Total micros should not exceed input"
    );
    assert!(
        rows_999 + rows_1000 <= 1000,
        "Total rows should not exceed input"
    );

    // Each second should have some allocation
    assert!(micros_999 > 0, "Second 999 should have some time allocated");
    assert!(
        micros_1000 > 0,
        "Second 1000 should have some time allocated"
    );
    assert!(rows_999 > 0, "Second 999 should have some rows allocated");
    assert!(rows_1000 > 0, "Second 1000 should have some rows allocated");
}
