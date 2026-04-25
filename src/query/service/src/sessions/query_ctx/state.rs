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

use super::*;

impl TableContextMergeInto for QueryContext {
    fn set_merge_into_join(&self, join: MergeIntoJoin) {
        let mut merge_into_join = self.shared.merge_into_join.write();
        *merge_into_join = join;
    }

    fn get_merge_into_join(&self) -> MergeIntoJoin {
        let merge_into_join = self.shared.merge_into_join.read();
        MergeIntoJoin {
            merge_into_join_type: merge_into_join.merge_into_join_type.clone(),
            is_distributed: merge_into_join.is_distributed,
            target_tbl_idx: merge_into_join.target_tbl_idx,
        }
    }
}

impl TableContextPerf for QueryContext {
    fn get_running_query_execution_stats(&self) -> Vec<(String, ExecutorStatsSnapshot)> {
        let mut all = SessionManager::instance().get_query_execution_stats();
        all.extend(DataExchangeManager::instance().get_query_execution_stats());
        all
    }

    fn get_perf_config(&self) -> PerfConfig {
        self.shared.get_perf_config()
    }

    fn set_perf_config(&self, config: PerfConfig) {
        self.shared.set_perf_config(config);
    }

    fn get_perf_flag(&self) -> bool {
        self.shared.get_perf_flag()
    }

    fn set_perf_flag(&self, flag: bool) {
        self.shared.set_perf_flag(flag);
    }

    fn get_nodes_perf(&self) -> Arc<Mutex<HashMap<String, String>>> {
        self.shared.get_nodes_perf()
    }

    fn set_nodes_perf(&self, node: String, perf: String) {
        self.shared.set_nodes_perf(node, perf);
    }

    fn get_perf_events(&self) -> Vec<Vec<PerfEvent>> {
        self.shared.get_perf_events()
    }

    fn set_perf_events(&self, event_groups: Vec<Vec<PerfEvent>>) {
        self.shared.set_perf_events(event_groups);
    }
}

impl TableContextQueryIdentity for QueryContext {
    fn get_id(&self) -> String {
        self.shared.init_query_id.as_ref().read().clone()
    }

    fn attach_query_str(&self, kind: QueryKind, query: String) {
        self.shared.attach_query_str(kind, query);
    }

    fn attach_query_hash(&self, text_hash: String, parameterized_hash: String) {
        self.shared.attach_query_hash(text_hash, parameterized_hash);
    }

    fn get_query_str(&self) -> String {
        self.shared.get_query_str()
    }

    fn get_query_parameterized_hash(&self) -> String {
        self.shared.get_query_parameterized_hash()
    }

    fn get_query_text_hash(&self) -> String {
        self.shared.get_query_text_hash()
    }

    fn get_last_query_id(&self, index: i32) -> Option<String> {
        self.shared.session.session_ctx.get_last_query_id(index)
    }

    fn get_query_id_history(&self) -> HashSet<String> {
        self.shared.session.session_ctx.get_query_id_history()
    }
}

impl TableContextPartitionStats for QueryContext {
    fn get_partition(&self) -> Option<PartInfoPtr> {
        if let Some(part) = self.partition_queue.write().pop_front() {
            Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, 1);
            return Some(part);
        }

        None
    }

    fn get_partitions(&self, num: usize) -> Vec<PartInfoPtr> {
        let mut res = Vec::with_capacity(num);
        let mut queue_guard = self.partition_queue.write();

        for _index in 0..num {
            match queue_guard.pop_front() {
                None => break,
                Some(part) => res.push(part),
            };
        }

        Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, res.len());

        res
    }

    fn partition_num(&self) -> usize {
        self.partition_queue.read().len()
    }

    fn set_partitions(&self, partitions: Partitions) -> Result<()> {
        let mut partition_queue = self.partition_queue.write();

        partition_queue.clear();
        for part in partitions.partitions {
            partition_queue.push_back(part);
        }
        Ok(())
    }

    fn get_can_scan_from_agg_index(&self) -> bool {
        self.shared.can_scan_from_agg_index.load(Ordering::Acquire)
    }

    fn set_can_scan_from_agg_index(&self, enable: bool) {
        self.shared
            .can_scan_from_agg_index
            .store(enable, Ordering::Release);
    }

    fn get_enable_sort_spill(&self) -> bool {
        self.shared.enable_sort_spill.load(Ordering::Acquire)
    }

    fn set_enable_sort_spill(&self, enable: bool) {
        self.shared
            .enable_sort_spill
            .store(enable, Ordering::Release);
    }

    fn set_compaction_num_block_hint(&self, table_name: &str, hint: u64) {
        let old = self
            .shared
            .num_fragmented_block_hint
            .lock()
            .insert(table_name.to_string(), hint);
        info!(
            "Set compaction hint for table '{}': old={:?}, new={}",
            table_name, old, hint
        );
    }

    fn get_compaction_num_block_hint(&self, table_name: &str) -> u64 {
        self.shared
            .num_fragmented_block_hint
            .lock()
            .get(table_name)
            .copied()
            .unwrap_or_default()
    }

    fn get_enable_auto_analyze(&self) -> bool {
        self.shared.enable_auto_analyze.load(Ordering::Acquire)
    }

    fn set_enable_auto_analyze(&self, enable: bool) {
        self.shared
            .enable_auto_analyze
            .store(enable, Ordering::Release);
    }

    fn get_pruned_partitions_stats(&self) -> HashMap<u32, PartStatistics> {
        self.shared.get_pruned_partitions_stats()
    }

    fn set_pruned_partitions_stats(&self, plan_id: u32, stats: PartStatistics) {
        self.shared.set_pruned_partitions_stats(plan_id, stats);
    }

    fn merge_pruned_partitions_stats(&self, other: &HashMap<u32, PartStatistics>) {
        self.shared.merge_pruned_partitions_stats(other);
    }
}

impl TableContextRuntimeFilter for QueryContext {
    // --- New builder/source API ---

    fn get_runtime_filter_builder(&self, scan_id: usize) -> RuntimeFilterBuilder {
        self.shared
            .runtime_filter_state
            .get_runtime_filter_builder(scan_id)
    }

    fn get_runtime_filter_source(&self, scan_id: usize) -> Option<RuntimeFilterSource> {
        self.shared
            .runtime_filter_state
            .get_runtime_filter_source(scan_id)
    }

    // --- Reporting ---

    fn clear_runtime_filter(&self) {
        self.shared.runtime_filter_state.clear();
    }

    fn assert_no_runtime_filter_state(&self) -> Result<()> {
        self.shared
            .runtime_filter_state
            .assert_empty(&self.get_id())
    }

    fn set_runtime_filter(&self, filters: HashMap<usize, RuntimeFilterInfo>) {
        self.shared.runtime_filter_state.set_runtime_filter(filters);
    }

    fn get_runtime_filters(&self, id: IndexType) -> Vec<RuntimeFilterEntry> {
        self.shared.runtime_filter_state.get_runtime_filters(id)
    }

    fn get_bloom_runtime_filter_with_id(&self, id: IndexType) -> Vec<(String, RuntimeBloomFilter)> {
        self.shared
            .runtime_filter_state
            .get_bloom_runtime_filter_with_id(id)
    }

    fn get_inlist_runtime_filter_with_id(&self, id: IndexType) -> Vec<Expr<String>> {
        self.shared
            .runtime_filter_state
            .get_inlist_runtime_filter_with_id(id)
    }

    fn get_min_max_runtime_filter_with_id(&self, id: IndexType) -> Vec<Expr<String>> {
        self.shared
            .runtime_filter_state
            .get_min_max_runtime_filter_with_id(id)
    }

    fn runtime_filter_reports(&self) -> HashMap<IndexType, Vec<RuntimeFilterReport>> {
        self.shared.runtime_filter_state.runtime_filter_reports()
    }

    fn has_bloom_runtime_filters(&self, id: usize) -> bool {
        self.shared
            .runtime_filter_state
            .has_bloom_runtime_filters(id)
    }

    fn add_partition_runtime_filters(&self, scan_id: usize, filters: PartitionRuntimeFilters) {
        self.shared
            .runtime_filter_state
            .add_partition_runtime_filters(scan_id, filters);
    }

    fn add_index_runtime_filters(&self, scan_id: usize, filters: IndexRuntimeFilters) {
        self.shared
            .runtime_filter_state
            .add_index_runtime_filters(scan_id, filters);
    }

    fn add_row_runtime_filters(&self, scan_id: usize, filters: RowRuntimeFilters) {
        self.shared
            .runtime_filter_state
            .add_row_runtime_filters(scan_id, filters);
    }

    fn get_partition_runtime_filters(&self, scan_id: usize) -> PartitionRuntimeFilters {
        self.shared
            .runtime_filter_state
            .get_partition_runtime_filters(scan_id)
    }

    fn get_index_runtime_filters(&self, scan_id: usize) -> IndexRuntimeFilters {
        self.shared
            .runtime_filter_state
            .get_index_runtime_filters(scan_id)
    }

    fn get_row_runtime_filters(&self, scan_id: usize) -> RowRuntimeFilters {
        self.shared
            .runtime_filter_state
            .get_row_runtime_filters(scan_id)
    }
}

impl TableContextResultCache for QueryContext {
    fn get_result_cache_key(&self, query_id: &str) -> Option<String> {
        self.shared
            .session
            .session_ctx
            .get_query_result_cache_key(query_id)
    }

    fn set_query_id_result_cache(&self, query_id: String, result_cache_key: String) {
        self.shared
            .session
            .session_ctx
            .update_query_ids_results(query_id, Some(result_cache_key))
    }
}

impl TableContextSpillProgress for QueryContext {
    fn get_join_spill_progress(&self) -> Arc<Progress> {
        self.shared.join_spill_progress.clone()
    }

    fn get_group_by_spill_progress(&self) -> Arc<Progress> {
        self.shared.group_by_spill_progress.clone()
    }

    fn get_aggregate_spill_progress(&self) -> Arc<Progress> {
        self.shared.agg_spill_progress.clone()
    }

    fn get_window_partition_spill_progress(&self) -> Arc<Progress> {
        self.shared.window_partition_spill_progress.clone()
    }

    fn get_join_spill_progress_value(&self) -> ProgressValues {
        self.shared.join_spill_progress.as_ref().get_values()
    }

    fn get_group_by_spill_progress_value(&self) -> ProgressValues {
        self.shared.group_by_spill_progress.as_ref().get_values()
    }

    fn get_aggregate_spill_progress_value(&self) -> ProgressValues {
        self.shared.agg_spill_progress.as_ref().get_values()
    }

    fn get_window_partition_spill_progress_value(&self) -> ProgressValues {
        self.shared
            .window_partition_spill_progress
            .as_ref()
            .get_values()
    }
}

impl TableContextStream for QueryContext {
    fn add_streams_ref(&self, catalog: &str, database: &str, stream: &str, consume: bool) {
        let mut streams = self.shared.streams_refs.write();
        let stream_key = (
            catalog.to_string(),
            database.to_string(),
            stream.to_string(),
        );
        streams
            .entry(stream_key)
            .and_modify(|v| {
                if consume {
                    *v = true;
                }
            })
            .or_insert(consume);
    }

    fn get_consume_streams(&self, query: bool) -> Result<Vec<Arc<dyn Table>>> {
        let streams_refs = self.shared.streams_refs.read();
        let tables = self.shared.tables_refs.lock();
        let mut streams_meta = Vec::with_capacity(streams_refs.len());
        for (stream_key, consume) in streams_refs.iter() {
            if query && !consume {
                continue;
            }
            let stream = tables
                .get(stream_key)
                .ok_or_else(|| ErrorCode::Internal("Stream reference not found in tables cache"))?;
            streams_meta.push(stream.clone());
        }
        Ok(streams_meta)
    }
}

impl TableContextVariables for QueryContext {
    fn set_variable(&self, key: String, value: Scalar) {
        self.shared.session.session_ctx.set_variable(key, value)
    }

    fn unset_variable(&self, key: &str) {
        self.shared.session.session_ctx.unset_variable(key)
    }

    fn get_variable(&self, key: &str) -> Option<Scalar> {
        self.shared.session.session_ctx.get_variable(key)
    }

    fn get_all_variables(&self) -> HashMap<String, Scalar> {
        self.shared.session.session_ctx.get_all_variables()
    }
}
