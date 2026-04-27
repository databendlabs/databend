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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_catalog::runtime_filter_info::IndexRuntimeFilters;
use databend_common_catalog::runtime_filter_info::PartitionRuntimeFilters;
use databend_common_catalog::runtime_filter_info::RowRuntimeFilters;
use databend_common_catalog::runtime_filter_info::RuntimeBloomFilter;
use databend_common_catalog::runtime_filter_info::RuntimeFilterBuilder;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReport;
use databend_common_catalog::runtime_filter_info::RuntimeFilterShared;
use databend_common_catalog::runtime_filter_info::RuntimeFilterSource;
use databend_common_catalog::runtime_filter_info::runtime_filter_builder;
use databend_common_catalog::runtime_filter_info::runtime_filter_source;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Expr;
use parking_lot::RwLock;

#[derive(Default)]
pub struct RuntimeFilterState {
    runtime_filters: RwLock<HashMap<usize, RuntimeFilterInfo>>,
    runtime_filter_logged: AtomicBool,
    partition_runtime_filters: RwLock<HashMap<usize, PartitionRuntimeFilters>>,
    index_runtime_filters: RwLock<HashMap<usize, IndexRuntimeFilters>>,
    row_runtime_filters: RwLock<HashMap<usize, RowRuntimeFilters>>,
    // New: per-scan_id shared state for builder/source channel
    channels: RwLock<HashMap<usize, Arc<RuntimeFilterShared>>>,
}

impl RuntimeFilterState {
    pub fn should_log(&self) -> bool {
        !self.runtime_filter_logged.swap(true, Ordering::SeqCst)
    }

    pub fn clear(&self) {
        self.runtime_filters.write().clear();
        self.runtime_filter_logged.store(false, Ordering::SeqCst);
        self.partition_runtime_filters.write().clear();
        self.index_runtime_filters.write().clear();
        self.row_runtime_filters.write().clear();
        self.channels.write().clear();
    }

    pub fn assert_empty(&self, query_id: &str) -> Result<()> {
        if !self.runtime_filters.read().is_empty() {
            return Err(ErrorCode::Internal(format!(
                "Runtime filters should be empty for query {query_id}"
            )));
        }
        if self.runtime_filter_logged.load(Ordering::Relaxed) {
            return Err(ErrorCode::Internal(format!(
                "Runtime filter logged flag should be reset for query {query_id}"
            )));
        }
        Ok(())
    }

    pub fn set_runtime_filter(&self, filters: HashMap<usize, RuntimeFilterInfo>) {
        let mut runtime_filters = self.runtime_filters.write();
        for (scan_id, filter) in filters {
            let entry = runtime_filters.entry(scan_id).or_default();
            for new_filter in filter.filters {
                entry.filters.push(new_filter);
            }
        }
    }

    pub fn get_runtime_filters(&self, id: usize) -> Vec<RuntimeFilterEntry> {
        self.runtime_filters
            .read()
            .get(&id)
            .map(|v| v.filters.clone())
            .unwrap_or_default()
    }

    pub fn get_bloom_runtime_filter_with_id(&self, id: usize) -> Vec<(String, RuntimeBloomFilter)> {
        self.get_runtime_filters(id)
            .into_iter()
            .filter_map(|entry| entry.bloom.map(|bloom| (bloom.column_name, bloom.filter)))
            .collect()
    }

    pub fn get_inlist_runtime_filter_with_id(&self, id: usize) -> Vec<Expr<String>> {
        self.get_runtime_filters(id)
            .into_iter()
            .filter_map(|entry| entry.inlist)
            .collect()
    }

    pub fn get_min_max_runtime_filter_with_id(&self, id: usize) -> Vec<Expr<String>> {
        self.get_runtime_filters(id)
            .into_iter()
            .filter_map(|entry| entry.min_max)
            .collect()
    }

    pub fn runtime_filter_reports(&self) -> HashMap<usize, Vec<RuntimeFilterReport>> {
        self.runtime_filters
            .read()
            .iter()
            .map(|(scan_id, info)| {
                let reports = info
                    .filters
                    .iter()
                    .map(|entry| RuntimeFilterReport {
                        filter_id: entry.id,
                        has_bloom: entry.bloom.is_some(),
                        has_inlist: entry.inlist.is_some(),
                        has_min_max: entry.min_max.is_some(),
                        stats: entry.stats.snapshot(),
                    })
                    .collect();
                (*scan_id, reports)
            })
            .collect()
    }

    pub fn has_bloom_runtime_filters(&self, id: usize) -> bool {
        self.runtime_filters
            .read()
            .get(&id)
            .map(|runtime_filter| {
                runtime_filter
                    .filters
                    .iter()
                    .any(|entry| entry.bloom.is_some())
            })
            .unwrap_or(false)
    }

    pub fn add_partition_runtime_filters(&self, scan_id: usize, filters: PartitionRuntimeFilters) {
        let mut map = self.partition_runtime_filters.write();
        map.insert(scan_id, filters);
    }

    pub fn add_index_runtime_filters(&self, scan_id: usize, filters: IndexRuntimeFilters) {
        let mut map = self.index_runtime_filters.write();
        map.insert(scan_id, filters);
    }

    pub fn add_row_runtime_filters(&self, scan_id: usize, filters: RowRuntimeFilters) {
        let mut map = self.row_runtime_filters.write();
        map.insert(scan_id, filters);
    }

    pub fn get_partition_runtime_filters(&self, scan_id: usize) -> PartitionRuntimeFilters {
        self.partition_runtime_filters
            .read()
            .get(&scan_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_index_runtime_filters(&self, scan_id: usize) -> IndexRuntimeFilters {
        self.index_runtime_filters
            .read()
            .get(&scan_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_row_runtime_filters(&self, scan_id: usize) -> RowRuntimeFilters {
        self.row_runtime_filters
            .read()
            .get(&scan_id)
            .cloned()
            .unwrap_or_default()
    }

    // --- New builder/source channel API ---

    pub fn get_runtime_filter_builder(&self, scan_id: usize) -> RuntimeFilterBuilder {
        let mut channels = self.channels.write();
        let shared = channels
            .entry(scan_id)
            .or_insert_with(|| Arc::new(RuntimeFilterShared::new()))
            .clone();
        runtime_filter_builder(&shared)
    }

    pub fn get_runtime_filter_source(&self, scan_id: usize) -> Option<RuntimeFilterSource> {
        let channels = self.channels.read();
        channels.get(&scan_id).map(runtime_filter_source)
    }
}
