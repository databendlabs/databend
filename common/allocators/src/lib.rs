// Copyright 2020 Datafuse Labs.
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
