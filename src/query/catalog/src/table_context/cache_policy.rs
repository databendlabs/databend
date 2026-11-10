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

use std::sync::Arc;

use crate::cache_policy::CachePolicy;
use crate::cache_policy::TableDataCacheWriteBudget;

/// The cache policy of the statement: readers built from the context apply it, together with
/// their own (`CachePolicy::at_most`).
pub trait TableContextCachePolicy: Send + Sync {
    fn cache_policy(&self) -> CachePolicy {
        CachePolicy::DEFAULT
    }

    /// Sets the statement's policy, for the reads of the statement on every node from then on.
    fn set_cache_policy(&self, _policy: CachePolicy) {
        unimplemented!()
    }

    /// What the statement may still put into the in-memory cache of decoded columns, shared by
    /// its scans on this node. The default is a budget of its own per call: no bound.
    fn table_data_cache_write_budget(&self) -> Arc<TableDataCacheWriteBudget> {
        Arc::default()
    }
}
