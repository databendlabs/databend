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

use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::MemoryMeter;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;

const MIB: usize = 1024 * 1024;

#[test]
fn test_memory_meter_counts_what_calls_keep() {
    let mut meter = MemoryMeter::default();
    let kept = meter.track(|| vec![1u8; 8 * MIB]);
    assert!(meter.bytes() >= 8 * MIB);

    // What a call frees before it returns does not count.
    let before = meter.bytes();
    meter.track(|| drop(std::hint::black_box(vec![1u8; 8 * MIB])));
    assert!(meter.bytes() < before + MIB);

    meter.mark();
    assert_eq!(meter.bytes(), 0);
    let more = meter.track(|| vec![1u8; 4 * MIB]);
    assert!(meter.bytes() >= 4 * MIB && meter.bytes() < 5 * MIB);
    drop((kept, more));
}

/// Memory handed over before a mark and freed afterwards, here on another thread, does not
/// offset what is allocated after the mark.
#[test]
fn test_memory_meter_ignores_frees_of_what_was_handed_over() {
    let mut meter = MemoryMeter::default();
    let handed_over = meter.track(|| vec![1u8; 8 * MIB]);
    meter.mark();
    Thread::spawn(move || drop(handed_over)).join().unwrap();
    assert_eq!(meter.bytes(), 0);

    let kept = meter.track(|| vec![1u8; 4 * MIB]);
    assert!(meter.bytes() >= 4 * MIB && meter.bytes() < 5 * MIB);
    drop(kept);
    assert!(meter.bytes() < MIB);
}

#[test]
fn test_memory_meter_feeds_the_parent_stat() {
    let parent = MemStat::create("memory_meter_parent".to_string());
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.mem_stat = Some(parent.clone());
    let _guard = ThreadTracker::tracking(payload);

    let mut meter = MemoryMeter::default();
    let kept = meter.track(|| vec![1u8; 8 * MIB]);
    assert!(meter.bytes() >= 8 * MIB);
    assert!(parent.get_memory_usage() >= 8 * MIB);
    drop(kept);
}
