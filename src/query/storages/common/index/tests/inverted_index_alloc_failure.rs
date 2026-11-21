// Copyright 2026 Datafuse Labs
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

//! Own binary for the allocator and hook. Inject the same unwind as a query memory limit.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::sync::Arc;

use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::set_alloc_error_hook;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_index::SearchPinDirectory;
use tantivy::Directory;
use tantivy::directory::OwnedBytes;
use tantivy::directory::RamDirectory;

thread_local! {
    static FAIL_IN: Cell<usize> = const { Cell::new(0) };
}

fn fail_now() -> bool {
    FAIL_IN.with(|n| n.get() > 0 && !LimitMemGuard::is_unlimited() && n.replace(n.get() - 1) == 1)
}

struct FailingAlloc;

unsafe impl GlobalAlloc for FailingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if fail_now() {
            return ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if fail_now() {
            return std::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: FailingAlloc = FailingAlloc;

fn fail_on(nth: usize, op: impl FnOnce()) -> Result<()> {
    ThreadTracker::replace_error_message(Some("memory usage exceeds limit".to_string()));
    FAIL_IN.set(nth);
    let result = catch_unwind(op);
    FAIL_IN.set(0);
    ThreadTracker::replace_error_message(None);
    if let Err(error) = &result {
        assert_eq!(error.code(), ErrorCode::MEMORY_EXCEEDS_LIMIT);
    }
    result
}

#[test]
fn test_shared_pin_survives_a_warmup_allocation_failure() {
    set_alloc_error_hook();

    let raw = RamDirectory::default();
    let path = Path::new("fixture.term");
    raw.atomic_write(path, &[b'x'; 128]).unwrap();
    let pin = SearchPinDirectory::shared(Arc::new(raw), |_| true);
    let file = pin.open_read(path).unwrap();
    pin.insert(path, 2..4, OwnedBytes::new(b"xx".to_vec()))
        .unwrap();
    let key = path.to_path_buf();
    let covering = OwnedBytes::new(b"xxxx".to_vec());
    // Fails while collecting the keys that the new range would replace, under the write lock.
    assert!(fail_on(1, || pin.insert(key, 1..5, covering).unwrap()).is_err());
    assert_eq!(file.read_bytes_slice(2..4).unwrap().as_ref(), b"xx");
    for start in (6..128).step_by(2) {
        let key = PathBuf::from(path);
        let bytes = OwnedBytes::new(vec![b'x']);
        // Only tree nodes can grow here; those allocations must complete even at the limit.
        fail_on(1, || pin.insert(key, start..start + 1, bytes).unwrap()).unwrap();
    }
    assert_eq!(file.read_bytes_slice(2..4).unwrap().as_ref(), b"xx");
    assert_eq!(file.read_bytes_slice(126..127).unwrap().as_ref(), b"x");
}
