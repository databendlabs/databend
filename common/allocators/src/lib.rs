// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(feature = "jemalloc-alloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "tcmalloc-alloc")]
#[global_allocator]
static ALLOC: tcmalloc::TCMalloc = tcmalloc::TCMalloc;

#[cfg(feature = "snmalloc-alloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// To use the global alloc
// put let _ = common_allocators::init() in your main method.
pub fn init() -> String {
    #[cfg(feature = "jemalloc-alloc")]
    return "jemalloc".to_string();

    #[cfg(feature = "tcmalloc-alloc")]
    return "tcmalloc".to_string();

    #[cfg(feature = "snmalloc-alloc")]
    return "snmalloc".to_string();
}
