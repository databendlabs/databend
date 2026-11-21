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
use std::collections::BTreeMap;
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
use databend_storages_common_index::FooterDirectory;
use databend_storages_common_index::InvertedIndexBundleFooter;
use databend_storages_common_index::InvertedIndexSearcher;
use databend_storages_common_index::SearchPinDirectory;
use tantivy::Directory;
use tantivy::Index;
use tantivy::IndexSettings;
use tantivy::Term;
use tantivy::directory::OwnedBytes;
use tantivy::directory::RamDirectory;
use tantivy::schema::Field;
use tantivy::schema::Schema;
use tantivy::schema::TEXT;

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

fn searcher() -> (InvertedIndexSearcher, Field) {
    let raw = RamDirectory::default();
    let mut schema = Schema::builder();
    let body = schema.add_text_field("body", TEXT);
    let index = Index::create(raw.clone(), schema.build(), IndexSettings::default()).unwrap();
    let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
    writer.add_document(tantivy::doc!(body => "first")).unwrap();
    writer.commit().unwrap();
    drop(writer);
    let files = index
        .directory()
        .list_managed_files()
        .into_iter()
        .filter(|path| {
            !matches!(path.to_str(), Some(".managed.json" | "meta.json"))
                && raw.exists(path).unwrap()
        })
        .map(|path| {
            let bytes = raw.atomic_read(&path).unwrap();
            (path, bytes)
        })
        .collect::<Vec<_>>();
    let bundle = InvertedIndexBundleFooter::build(
        files,
        BTreeMap::new(),
        BTreeMap::new(),
        raw.atomic_read(Path::new(".managed.json")).unwrap(),
        raw.atomic_read(Path::new("meta.json")).unwrap(),
    )
    .unwrap();
    let footer = InvertedIndexBundleFooter::open(&bundle).unwrap();
    let directory = FooterDirectory::new(raw, footer);
    (InvertedIndexSearcher::open(directory).unwrap(), body)
}

#[test]
fn test_searcher_survives_a_warmup_allocation_failure() {
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

    let (searcher, field) = searcher();
    let first = Term::from_field_text(field, "first");
    searcher.remember_term_info(&first, None);
    // The first allocation clones the term. Fail the second, when growing the map under its
    // write lock. Try enough new terms to force a growth without depending on its capacity.
    let failed = (1..64).find_map(|i| {
        let term = Term::from_field_text(field, &i.to_string());
        fail_on(2, || searcher.remember_term_info(&term, None))
            .is_err()
            .then_some((i, term))
    });
    let (count, term) = failed.expect("term map did not try to grow");
    assert_eq!(searcher.term_info(&first), Some(None));
    assert_eq!(searcher.term_info(&term), None);
    searcher.remember_term_info(&term, None);
    assert_eq!(searcher.term_info(&term), Some(None));
    for i in 1..count {
        let term = Term::from_field_text(field, &i.to_string());
        assert_eq!(searcher.term_info(&term), Some(None));
    }
}
