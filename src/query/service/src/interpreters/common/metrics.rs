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

use common_metrics::register_histogram_family_in_milliseconds;
use common_metrics::Family;
use common_metrics::Histogram;
use lazy_static::lazy_static;
use metrics::increment_gauge;

lazy_static! {
    static ref COMPACT_HOOK_EXECUTION_MS: Family<Vec<(&'static str, String)>, Histogram> =
        register_histogram_family_in_milliseconds("compact_hook_execution_ms");
    static ref COMPACT_HOOK_COMPACTION_MS: Family<Vec<(&'static str, String)>, Histogram> =
        register_histogram_family_in_milliseconds("compact_hook_compaction_ms");
}

// the time used in executing the main operation  (replace-into, copy-into, etc)
// metrics names with pattern `compact_hook_{operation_name}_time_execution_ms`
pub fn metrics_inc_compact_hook_main_operation_time_ms(operation_name: &str, c: u64) {
    increment_gauge!(
        format!("compact_hook_{}_time_execution_ms", operation_name),
        c as f64
    );
    let labels = &vec![("operation", operation_name.to_string())];
    COMPACT_HOOK_EXECUTION_MS
        .get_or_create(labels)
        .observe(c as f64);
}

// the time used in executing the compaction
// metrics names with pattern `compact_hook_{operation_name}_time_compaction_ms`
pub fn metrics_inc_compact_hook_compact_time_ms(operation_name: &str, c: u64) {
    increment_gauge!(
        format!("compact_hook_{}_time_compaction_ms", operation_name),
        c as f64
    );
    let labels = &vec![("operation", operation_name.to_string())];
    COMPACT_HOOK_COMPACTION_MS
        .get_or_create(labels)
        .observe(c as f64);
}
