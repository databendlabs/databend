// Copyright 2021 Datafuse Labs.
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

#[derive(Clone)]
pub struct CacheSettings {
    pub memory_items_cache_capacity: u64,
    pub memory_bytes_cache_capacity: u64,

    // write cache if true.
    pub cache_on_write: bool,
}

impl Default for CacheSettings {
    fn default() -> Self {
        CacheSettings {
            memory_items_cache_capacity: 10000 * 100,
            memory_bytes_cache_capacity: 1024 * 1024 * 1024,
            cache_on_write: false,
        }
    }
}
