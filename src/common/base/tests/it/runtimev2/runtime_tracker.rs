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

use common_base::mem_allocator::JEAllocator;
use common_base::runtime::ProxyAllocator;
use common_base::runtime::RuntimeTracker;

static ALLOC: ProxyAllocator<JEAllocator> = ProxyAllocator::new(JEAllocator);

#[test]
#[serial_test::serial]
// Works only if run alone.
fn test_runtime_tracker() {
    let tracker = RuntimeTracker::create();

    let layout = Layout::from_size_align(32, 1).unwrap();
    let ptr = unsafe { ALLOC.alloc(layout) };
    assert_ne!(ptr, null_mut());

    let memory_usage = tracker.get_memory_usage();

    assert_eq!(memory_usage, 32);

    unsafe { ALLOC.dealloc(ptr, layout) };
}
