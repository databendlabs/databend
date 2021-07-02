// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

cfg_if::cfg_if! {
    if #[cfg(feature = "jemalloc-alloc")] {
        #[global_allocator]
        static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
    }
    else if #[cfg(feature = "tcmalloc-alloc")] {
        #[global_allocator]
        static ALLOC: tcmalloc::TCMalloc = tcmalloc::TCMalloc;
    }
    else if #[cfg(feature = "snmalloc-alloc")] {
        #[global_allocator]
        static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;
    }
}

// To use the global alloc
// put let _ = common_allocators::init() in your main method.
pub fn init() -> String {
    cfg_if::cfg_if! {
        if #[cfg(feature = "jemalloc-alloc")] {
            "jemalloc".to_string()
        }
        else if #[cfg(feature = "tcmalloc-alloc")] {
            "tcmalloc".to_string()
        }
        else if #[cfg(feature = "snmalloc-alloc")] {
            "snmalloc".to_string()
        }
        else {
            "default".to_string()
        }
    }
}
