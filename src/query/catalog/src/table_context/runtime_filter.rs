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
use std::sync::Arc;

use databend_common_expression::Expr;

use crate::runtime_filter_info::RuntimeBloomFilter;
use crate::runtime_filter_info::RuntimeFilterEntry;
use crate::runtime_filter_info::RuntimeFilterInfo;
use crate::runtime_filter_info::RuntimeFilterReady;
use crate::runtime_filter_info::RuntimeFilterReport;
use crate::runtime_filter_info::RuntimeScanFilter;
use crate::runtime_filter_info::RuntimeScanFilters;

pub trait TableContextRuntimeFilter: Send + Sync {
    fn set_runtime_filter(&self, _filters: HashMap<usize, RuntimeFilterInfo>) {
        unimplemented!()
    }

    fn set_runtime_filter_ready(&self, table_index: usize, ready: Arc<RuntimeFilterReady>);

    fn get_runtime_filter_ready(&self, table_index: usize) -> Vec<Arc<RuntimeFilterReady>>;

    fn clear_runtime_filter(&self);

    /// Register a scan filter (runtime TopN boundary, limit early-stop, ...)
    /// for `scan_id`. Registered filters share the lifecycle of join runtime
    /// filters: they stay in the context until `clear_runtime_filter`. Scan
    /// ids are only unique within one physical plan, so any site that reuses
    /// a QueryContext to build another plan must call `clear_runtime_filter`
    /// first (as the recursive CTE source and the nested query executors do),
    /// otherwise a colliding scan id would observe a stale filter.
    fn register_runtime_scan_filter(&self, _scan_id: usize, _filter: Arc<dyn RuntimeScanFilter>) {}

    fn get_runtime_scan_filters(&self, _scan_id: usize) -> RuntimeScanFilters {
        RuntimeScanFilters::default()
    }

    fn get_runtime_filters(&self, id: usize) -> Vec<RuntimeFilterEntry>;

    fn get_bloom_runtime_filter_with_id(&self, id: usize) -> Vec<(String, RuntimeBloomFilter)>;

    fn get_inlist_runtime_filter_with_id(&self, id: usize) -> Vec<Expr<String>>;

    fn get_min_max_runtime_filter_with_id(&self, id: usize) -> Vec<Expr<String>>;

    fn runtime_filter_reports(&self) -> HashMap<usize, Vec<RuntimeFilterReport>>;

    fn has_bloom_runtime_filters(&self, id: usize) -> bool;
}
