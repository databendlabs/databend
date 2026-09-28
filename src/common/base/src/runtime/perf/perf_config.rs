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

fn default_frequency() -> i32 {
    99
}

/// Configuration of `EXPLAIN PERF CPU`, shared with every node running the query.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PerfConfig {
    /// The query runs under `EXPLAIN PERF CPU`, the executor marks its threads for sampling.
    pub perf_enabled: bool,
    /// Whether this node starts its own CPU profiler. The coordinator starts one for the whole
    /// statement, its fragments must not start another.
    pub profiler_enabled: bool,
    #[serde(default = "default_frequency")]
    pub frequency: i32,
}

impl PerfConfig {
    pub fn is_perf_active(&self) -> bool {
        self.perf_enabled
    }
}
