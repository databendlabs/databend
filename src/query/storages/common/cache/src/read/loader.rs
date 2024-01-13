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

use databend_common_exception::Result;

pub struct LoadParams {
    pub location: String,
    pub len_hint: Option<u64>,
    pub ver: u64,
    pub put_cache: bool,
}

pub type CacheKey = String;

/// Loads an object from storage
#[async_trait::async_trait]
pub trait Loader<T> {
    /// Loads object of type T, located by [params][LoadParams].
    async fn load(&self, params: &LoadParams) -> Result<T>;

    /// the [CacheKey] returns will be used as the key of cached item.
    fn cache_key(&self, params: &LoadParams) -> CacheKey {
        params.location.clone()
    }
}
