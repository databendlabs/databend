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

use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::Expr;

use crate::runtime_filter_info::IndexRuntimeFilters;
use crate::runtime_filter_info::PartitionRuntimeFilters;
use crate::runtime_filter_info::RowRuntimeFilters;
use crate::runtime_filter_info::RuntimeBloomFilter;
use crate::runtime_filter_info::RuntimeFilterBuilder;
use crate::runtime_filter_info::RuntimeFilterEntry;
use crate::runtime_filter_info::RuntimeFilterInfo;
use crate::runtime_filter_info::RuntimeFilterReport;
use crate::runtime_filter_info::RuntimeFilterSource;

pub trait TableContextRuntimeFilter: Send + Sync {
    // --- New builder/source API ---

    /// Build side: get (or create) a builder for the given scan_id.
    /// Each call returns a new clone (builder_count incremented).
    fn get_runtime_filter_builder(&self, _scan_id: usize) -> RuntimeFilterBuilder {
        unimplemented!()
    }

    /// Probe side: get a source for the given scan_id.
    /// Returns None if no builder was registered for this scan_id.
    fn get_runtime_filter_source(&self, _scan_id: usize) -> Option<RuntimeFilterSource> {
        unimplemented!()
    }

    // --- Reporting API (kept for logging) ---

    fn set_runtime_filter(&self, _filters: HashMap<usize, RuntimeFilterInfo>) {
        unimplemented!()
    }

    fn get_runtime_filters(&self, id: usize) -> Vec<RuntimeFilterEntry>;

    fn runtime_filter_reports(&self) -> HashMap<usize, Vec<RuntimeFilterReport>>;

    fn clear_runtime_filter(&self);

    fn assert_no_runtime_filter_state(&self) -> Result<()> {
        unimplemented!()
    }

    // --- Legacy API (to be removed incrementally) ---

    fn get_bloom_runtime_filter_with_id(&self, id: usize) -> Vec<(String, RuntimeBloomFilter)>;

    fn get_inlist_runtime_filter_with_id(&self, id: usize) -> Vec<Expr<String>>;

    fn get_min_max_runtime_filter_with_id(&self, id: usize) -> Vec<Expr<String>>;

    fn has_bloom_runtime_filters(&self, id: usize) -> bool;

    fn add_partition_runtime_filters(&self, _: usize, _: PartitionRuntimeFilters);

    fn add_index_runtime_filters(&self, _: usize, _: IndexRuntimeFilters);

    fn add_row_runtime_filters(&self, _: usize, _: RowRuntimeFilters);

    fn get_partition_runtime_filters(&self, _: usize) -> PartitionRuntimeFilters;

    fn get_row_runtime_filters(&self, _: usize) -> RowRuntimeFilters;

    fn get_index_runtime_filters(&self, _: usize) -> IndexRuntimeFilters;
}
