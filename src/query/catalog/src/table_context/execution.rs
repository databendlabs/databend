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
use std::time::Duration;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::PerfConfig;
use databend_common_base::runtime::PerfEvent;
use parking_lot::Mutex;

use crate::statistics::data_cache_statistics::DataCacheMetrics;
use crate::table_context::ProcessInfo;

pub trait TableContextProgress: Send + Sync {
    fn incr_total_scan_value(&self, value: ProgressValues);

    fn get_total_scan_value(&self) -> ProgressValues;

    fn get_scan_progress(&self) -> Arc<Progress>;

    fn get_scan_progress_value(&self) -> ProgressValues;

    fn get_write_progress(&self) -> Arc<Progress>;

    fn get_write_progress_value(&self) -> ProgressValues;

    fn get_result_progress(&self) -> Arc<Progress>;

    fn get_result_progress_value(&self) -> ProgressValues;
}

pub trait TableContextSpillProgress: Send + Sync {
    fn get_join_spill_progress(&self) -> Arc<Progress>;

    fn get_group_by_spill_progress(&self) -> Arc<Progress>;

    fn get_aggregate_spill_progress(&self) -> Arc<Progress>;

    fn get_window_partition_spill_progress(&self) -> Arc<Progress>;

    fn get_join_spill_progress_value(&self) -> ProgressValues;

    fn get_group_by_spill_progress_value(&self) -> ProgressValues;

    fn get_aggregate_spill_progress_value(&self) -> ProgressValues;

    fn get_window_partition_spill_progress_value(&self) -> ProgressValues;
}

pub trait TableContextTelemetry: Send + Sync {
    fn get_processes_info(&self) -> Vec<ProcessInfo>;

    fn get_queued_queries(&self) -> Vec<ProcessInfo>;

    fn get_status_info(&self) -> String;

    fn set_status_info(&self, info: &str);

    fn get_data_cache_metrics(&self) -> &DataCacheMetrics;

    fn get_query_queued_duration(&self) -> Duration;

    fn set_query_queued_duration(&self, queued_duration: Duration);
}

pub trait TableContextPerf: Send + Sync {
    fn get_perf_config(&self) -> PerfConfig {
        unimplemented!()
    }

    fn set_perf_config(&self, _config: PerfConfig) {
        unimplemented!()
    }

    fn get_perf_flag(&self) -> bool {
        unimplemented!()
    }

    fn set_perf_flag(&self, _flag: bool) {
        unimplemented!()
    }

    fn get_nodes_perf(&self) -> Arc<Mutex<HashMap<String, String>>> {
        unimplemented!()
    }

    fn set_nodes_perf(&self, _node: String, _perf: String) {
        unimplemented!()
    }

    fn get_perf_events(&self) -> Vec<Vec<PerfEvent>> {
        unimplemented!()
    }

    fn set_perf_events(&self, _event_groups: Vec<Vec<PerfEvent>>) {
        unimplemented!()
    }

    fn get_running_query_execution_stats(&self) -> Vec<(String, ExecutorStatsSnapshot)> {
        unimplemented!()
    }
}
