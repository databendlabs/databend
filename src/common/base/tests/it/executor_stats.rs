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

use databend_common_base::runtime::ExecutorStatsSlot;
mod executor_stats_loom_tests {
    use loom::sync::Arc;
    use loom::thread;
    use rand::Rng;

    use super::*;
    #[test]
    pub fn test_slot_with_loom() {
        let mut rng = rand::thread_rng();
        let numbers: [u32; 3] = [rng.gen::<u32>(), rng.gen::<u32>(), rng.gen::<u32>()];
        let expected_sum = numbers.iter().fold(0u32, |acc, &x| acc.saturating_add(x));
        let expected_timestamp = 1751871568;

        loom::model(move || {
            let slot = Arc::new(ExecutorStatsSlot::new());

            let ths: Vec<_> = numbers
                .map(|number| {
                    let slot_clone = slot.clone();
                    thread::spawn(move || {
                        slot_clone.add(expected_timestamp, number);
                    })
                })
                .into_iter()
                .collect();

            for th in ths {
                th.join().unwrap();
            }

            let (timestamp, sum) = slot.get();
            assert_eq!(timestamp, expected_timestamp);
            assert_eq!(sum, expected_sum);
        });
    }
}

mod executor_stats_regular_tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

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
                thread::spawn(move || {
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
}
