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

use databend_common_expression::RemoteExpr;

// pub type ProbeKeyWithEquivalents = Option<Vec<(RemoteExpr<String>, usize, usize)>>;

/// Collection of runtime filters for a join operation
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct PhysicalRuntimeFilters {
    pub filters: Vec<PhysicalRuntimeFilter>,
}

/// A runtime filter that is built once and applied to multiple probe targets
///
/// # Design
/// A single runtime filter is constructed once from the build side and then
/// pushed down to multiple table scans on the probe side. This is particularly
/// useful when join columns form equivalence classes (e.g., t1.c1 = t2.c1 = t3.c1),
/// allowing one filter to be applied to multiple tables.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PhysicalRuntimeFilter {
    /// Unique identifier for this runtime filter
    pub id: usize,

    /// The build key expression used to construct the filter
    pub build_key: RemoteExpr,

    /// List of (probe_key, scan_id) pairs that this filter should be applied to
    /// A single filter is built once and then pushed down to multiple scans
    /// All probe targets in this list are in the same equivalence class
    pub probe_targets: Vec<(RemoteExpr<String>, usize)>,

    pub build_table_rows: Option<u64>,
    pub probe_table_rows: Option<u64>,

    /// Enable bloom filter for this runtime filter
    pub enable_bloom_runtime_filter: bool,

    /// Enable inlist filter for this runtime filter
    pub enable_inlist_runtime_filter: bool,

    /// Enable min-max filter for this runtime filter
    pub enable_min_max_runtime_filter: bool,
}
