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

use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::PerfConfig;
use databend_common_base::runtime::PerfEvent;
use parking_lot::Mutex;

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
