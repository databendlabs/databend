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

use std::time::SystemTime;

use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;

#[test]
fn test_mem_tracker_with_primitive_type() {
    fn test_primitive_type<T: Copy>(index: T) {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let _test = Box::new(index);

        drop(_guard);
        drop(_test);
        assert_eq!(
            mem_stat.get_memory_usage(),
            std::mem::size_of_val(&index) as i64
        );
    }

    test_primitive_type(0_i8);
    test_primitive_type(0_i16);
    test_primitive_type(0_i32);
    test_primitive_type(0_i64);
    test_primitive_type(0_u8);
    test_primitive_type(0_u16);
    test_primitive_type(0_u32);
    test_primitive_type(0_u64);
}

#[test]
fn test_mem_tracker_with_string_type() {
    for _index in 0..10 {
        let length = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as usize
            % 1000;

        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);

        let str = "".repeat(length);
        drop(_guard);
        assert_eq!(mem_stat.get_memory_usage(), str.as_bytes().len() as i64);
    }
}

#[test]
fn test_mem_tracker_with_vec_type() {
    for _index in 0..10 {
        let length = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as usize
            % 1000;

        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);

        let vec = (0..length).collect::<Vec<usize>>();
        drop(_guard);
        assert_eq!(
            mem_stat.get_memory_usage(),
            (vec.capacity() * std::mem::size_of::<usize>()) as i64
        );
    }
}
