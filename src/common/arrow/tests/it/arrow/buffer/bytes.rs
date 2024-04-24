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

use databend_common_arrow::arrow::buffer::Bytes;
use databend_common_base::mem_allocator::GlobalAllocator;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::OwnedMemoryUsageSize;
use databend_common_base::runtime::ThreadTracker;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: GlobalAllocator = GlobalAllocator;

#[test]
fn test_bytes_owned_memory_usage() {
    fn test_primitive_type<T: Copy>(index: T) {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut bytes = Bytes::from(vec![index; 1000]);

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            bytes.owned_memory_usage() as i64
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
