//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_metrics::label_counter;
use common_metrics::label_counter_with_val;

const CACHE_READ_BYTES_FROM_REMOTE: &str = "cache_read_bytes_from_remote";
const CACHE_READ_BYTES_FROM_LOCAL: &str = "cache_read_bytes_from_local";
const CACHE_ACCESS_COUNT: &str = "cache_access_count";
const CACHE_ACCESS_HIT_COUNT: &str = "cache_access_hit_count";

pub struct TenantLabel<'a> {
    pub tenant_id: &'a str,
    pub cluster_id: &'a str,
}

pub struct CacheDeferMetrics<'a> {
    pub tenant_label: TenantLabel<'a>,
    pub cache_hit: bool,
    pub read_bytes: u64,
}

impl Drop for CacheDeferMetrics<'_> {
    fn drop(&mut self) {
        let label = &self.tenant_label;
        let tenant_id = label.tenant_id;
        let cluster_id = label.cluster_id;

        label_counter(CACHE_ACCESS_COUNT, tenant_id, cluster_id);
        if self.cache_hit {
            label_counter(CACHE_ACCESS_HIT_COUNT, tenant_id, cluster_id);
            label_counter_with_val(
                CACHE_READ_BYTES_FROM_LOCAL,
                self.read_bytes,
                tenant_id,
                cluster_id,
            );
        } else {
            label_counter_with_val(
                CACHE_READ_BYTES_FROM_REMOTE,
                self.read_bytes,
                tenant_id,
                cluster_id,
            );
        }
    }
}
