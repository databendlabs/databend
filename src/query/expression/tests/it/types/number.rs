// Copyright 2022 Datafuse Labs.
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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_base::mem_allocator::GlobalAllocator;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::OwnedMemoryUsageSize;
use databend_common_base::runtime::ThreadTracker;
use databend_common_expression::types::NumberColumn;
use ordered_float::OrderedFloat;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: GlobalAllocator = GlobalAllocator;

macro_rules! test_owned_memory_usage_with_type {
    ($t:tt, $ele:expr) => {
        for len in 0..100 {
            let mem_stat = MemStat::create("TEST".to_string());
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.mem_stat = Some(mem_stat.clone());

            let _guard = ThreadTracker::tracking(payload);
            let mut column = NumberColumn::$t(Buffer::from(vec![$ele; len]));

            drop(_guard);
            assert_ne!(mem_stat.get_memory_usage(), 0);
            assert_eq!(
                mem_stat.get_memory_usage(),
                column.owned_memory_usage() as i64
            );
            drop(column);
        }
    };
}

#[test]
fn test_number_column_owned_memory_usage() {
    test_owned_memory_usage_with_type!(Int8, 0_i8);
    test_owned_memory_usage_with_type!(Int16, 0_i16);
    test_owned_memory_usage_with_type!(Int32, 0_i32);
    test_owned_memory_usage_with_type!(Int64, 0_i64);

    test_owned_memory_usage_with_type!(UInt8, 0_u8);
    test_owned_memory_usage_with_type!(UInt16, 0_u16);
    test_owned_memory_usage_with_type!(UInt32, 0_u32);
    test_owned_memory_usage_with_type!(UInt64, 0_u64);

    test_owned_memory_usage_with_type!(Float32, OrderedFloat::from(0_f32));
    test_owned_memory_usage_with_type!(Float64, OrderedFloat::from(0_f64));
}
