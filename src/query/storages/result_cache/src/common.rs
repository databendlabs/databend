// Copyright 2023 Datafuse Labs.
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

use sha2::Digest;
use sha2::Sha256;
use uuid::Uuid;

const RESULT_CACHE_PREFIX: &str = "_result_cache";

#[inline(always)]
pub fn gen_result_cache_meta_key(tenant: &str, key: &str) -> String {
    let key = Sha256::digest(key);
    format!("{RESULT_CACHE_PREFIX}/{tenant}/{key:?}")
}

#[inline(always)]
pub(crate) fn gen_result_cache_location() -> String {
    format!("{}/{}", RESULT_CACHE_PREFIX, Uuid::new_v4().as_simple())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ResultCacheValue {
    /// The query SQL.
    pub sql: String,
    /// The query time.
    pub query_time: u64,
    /// Time-to-live of this query.
    pub ttl: u64,
    /// The size of the result cache (bytes).
    pub result_size: usize,
    /// The number of rows in the result cache.
    pub num_rows: usize,
    /// The location of the result cache file.
    pub location: String,
}
