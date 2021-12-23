// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_infallible::RwLock;

#[derive(Clone, Debug, Default)]
pub struct DalMetrics {
    pub read_bytes: usize,
    pub write_bytes: usize,
    pub cost_ms: usize,
}

#[derive(Clone, Debug, Default)]
pub struct DalContext {
    metrics: Arc<RwLock<DalMetrics>>,
}

impl DalContext {
    pub fn create() -> Self {
        DalContext {
            metrics: Arc::new(Default::default()),
        }
    }

    /// Increment read bytes.
    pub fn inc_read_bytes(&self, bytes: usize) {
        let mut metrics = self.metrics.write();
        metrics.read_bytes += bytes;
    }

    /// Increment write bytes.
    pub fn inc_write_bytes(&self, bytes: usize) {
        let mut metrics = self.metrics.write();
        metrics.write_bytes += bytes;
    }

    // Increment cost in ms.
    pub fn inc_cost_ms(&self, cost: usize) {
        let mut metrics = self.metrics.write();
        metrics.cost_ms += cost;
    }

    pub fn get_metrics(&self) -> DalMetrics {
        self.metrics.read().clone()
    }
}
