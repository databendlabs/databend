// Copyright 2022 Datafuse Labs.
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

use opendal::Operator;

use crate::metrics::StorageMetricsLayer;
use crate::StorageMetrics;

/// Storage Context.
#[derive(Clone, Debug, Default)]
pub struct StorageContext {
    /// Record the metrics of table data (via data operator).
    table_data_metrics: Arc<StorageMetrics>,
}

impl StorageContext {
    /// Get table data metrics from storage context.
    pub fn table_data_metrics(&self) -> Arc<StorageMetrics> {
        self.table_data_metrics.clone()
    }

    /// Wrap input operator with table data metrics layer.
    ///
    /// All IO operator happened in this operator will record in table_data_metrics.
    pub fn with_table_data_metrics_layer(&self, op: Operator) -> Operator {
        op.layer(StorageMetricsLayer::new(self.table_data_metrics.clone()))
    }
}
