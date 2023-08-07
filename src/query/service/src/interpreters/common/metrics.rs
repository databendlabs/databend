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

use metrics::increment_gauge;

// the time used in executing the main operation  (replace-into, copy-into, etc)
pub fn metrics_inc_compact_hook_main_operation_time_ms(operation_name: &str, c: u64) {
    increment_gauge!(
        format!("compact_hook_{}_time_execution_ms", operation_name),
        c as f64
    );
}

// the time used in executing the compaction
pub fn metrics_inc_compact_hook_compact_time_ms(operation_name: &str, c: u64) {
    increment_gauge!(
        format!("compact_hook_{}_time_compaction_ms", operation_name),
        c as f64
    );
}
