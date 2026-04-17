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

pub trait TableContextResultCache: Send + Sync {
    /// Add a cache invalidation key for query result cache.
    ///
    /// Historically named `partitions_sha` because most callers use a partition SHA256,
    /// but some callers may use other stable version identifiers (e.g. Fuse snapshot_location).
    fn add_partitions_sha(&self, key: String);

    fn get_partitions_shas(&self) -> Vec<String>;

    fn add_cache_key_extra(&self, extra: String);

    fn get_cache_key_extras(&self) -> Vec<String>;

    fn get_cacheable(&self) -> bool;

    fn set_cacheable(&self, cacheable: bool);

    fn get_result_cache_key(&self, query_id: &str) -> Option<String>;

    fn set_query_id_result_cache(&self, query_id: String, result_cache_key: String);
}
