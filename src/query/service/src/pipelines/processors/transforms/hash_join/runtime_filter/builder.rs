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

use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;
pub(super) fn should_enable_runtime_filter(
    desc: &RuntimeFilterDesc,
    build_num_rows: usize,
    selectivity_threshold: u64,
) -> bool {
    if build_num_rows == 0 {
        return false;
    }

    let Some(build_table_rows) = desc.build_table_rows else {
        log::info!(
            "RUNTIME-FILTER: Disable runtime filter {} - no build table statistics available",
            desc.id
        );
        return false;
    };

    let selectivity_pct = (build_num_rows as f64 / build_table_rows as f64) * 100.0;

    if selectivity_pct < selectivity_threshold as f64 {
        log::info!(
            "RUNTIME-FILTER: Enable runtime filter {} - low selectivity: {:.2}% < {}% (build_rows={}, build_table_rows={})",
            desc.id,
            selectivity_pct,
            selectivity_threshold,
            build_num_rows,
            build_table_rows
        );
        true
    } else {
        log::info!(
            "RUNTIME-FILTER: Disable runtime filter {} - high selectivity: {:.2}% >= {}% (build_rows={}, build_table_rows={})",
            desc.id,
            selectivity_pct,
            selectivity_threshold,
            build_num_rows,
            build_table_rows
        );
        false
    }
}
