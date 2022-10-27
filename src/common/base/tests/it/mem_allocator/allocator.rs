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

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::ptr::null_mut;

use common_base::mem_allocator::print_memory_stats;
use common_base::mem_allocator::total_memory_usage;
use common_base::mem_allocator::JEAllocator;
use common_base::mem_allocator::ProxyAllocator;
use tracing_subscriber::util::SubscriberInitExt;

#[test]
fn test_print_counters_ary() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .finish()
        .init();
    print_memory_stats();
}

static ALLOC: ProxyAllocator<JEAllocator> = ProxyAllocator::new(JEAllocator);

#[test]
#[serial_test::serial]
// Works only if run alone.
fn test_allocator() {
    let layout = Layout::from_size_align(32, 1).unwrap();
    let ptr = unsafe { ALLOC.alloc(layout) };
    assert_ne!(ptr, null_mut());

    assert_eq!(total_memory_usage(), 32);

    unsafe { ALLOC.dealloc(ptr, layout) };
}

#[test]
#[serial_test::serial]
fn test_alignment() {
    for alloc in (0..16).map(|i| i << 1) {
        for alignment in (0..16).map(|i| 1 << i) {
            let layout = Layout::from_size_align(alloc, alignment).unwrap();
            let ptr = unsafe { ALLOC.alloc(layout) };
            assert_ne!(ptr, null_mut());

            assert_eq!(ptr as usize % alignment, 0);

            unsafe { ALLOC.dealloc(ptr, layout) };
        }
    }
}
