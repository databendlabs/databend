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
    let enabled = bloom_selectivity_allows(desc, build_num_rows, selectivity_threshold);
    log::info!(
        "RUNTIME-FILTER: bloom runtime filter {} enabled={} (build_rows={}, build_table_rows={:?}, probe_table_rows={:?}, threshold={}%)",
        desc.id,
        enabled,
        build_num_rows,
        desc.build_table_rows,
        desc.probe_table_rows,
        selectivity_threshold
    );
    enabled
}

/// Pure form of [`should_enable_runtime_filter`] without logging.
///
/// It is monotone in `build_num_rows`: once it returns `false` for some row count, it returns
/// `false` for every larger count. Local builders rely on this to drop bloom hashes as soon as
/// the rows seen so far already rule the bloom filter out.
pub(super) fn bloom_selectivity_allows(
    desc: &RuntimeFilterDesc,
    build_num_rows: usize,
    selectivity_threshold: u64,
) -> bool {
    if build_num_rows == 0 {
        return false;
    }

    // Unknown/zero denominators cannot justify the optimization. Preserve
    // the existing build-statistics path; probe statistics only add an OR.
    let below_threshold = |table_rows: Option<u64>| {
        table_rows.is_some_and(|rows| {
            rows > 0
                && (build_num_rows as u128) * 100 < (rows as u128) * (selectivity_threshold as u128)
        })
    };
    below_threshold(desc.build_table_rows) || below_threshold(desc.probe_table_rows)
}
