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

/// A value that can be cached. Its kind decides where it is cached; one type may be the value
/// of several kinds.
pub trait CachedItem: Send + Sync + 'static {
    fn mem_bytes(&self) -> usize;
}

/// For kinds whose values are handed out as `Arc<T>` beyond the cache. Holders of the inner
/// `Arc` are not seen by the store: such values are not pinned and drop out of `resident` once
/// evicted, as with the pre-refactor caches.
impl<T: CachedItem> CachedItem for std::sync::Arc<T> {
    fn mem_bytes(&self) -> usize {
        T::mem_bytes(self)
    }
}
