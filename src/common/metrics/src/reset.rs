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

use common_exception::Result;
use metrics::gauge;
use metrics_exporter_prometheus::PrometheusHandle;

use crate::dump_metric_samples;
use crate::MetricValue;

/// Reset gauge metrics to 0.
pub fn reset_metrics(handle: PrometheusHandle) -> Result<()> {
    let samples = dump_metric_samples(handle)?;
    for sample in samples {
        if let MetricValue::Gauge(_) = sample.value {
            gauge!(sample.name, 0_f64);
        }
    }
    Ok(())
}
