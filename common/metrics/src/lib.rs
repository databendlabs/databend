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

mod dump;
mod recorder;

pub use dump::dump_metric_samples;
pub use dump::HistogramCount;
pub use dump::MetricSample;
pub use dump::MetricValue;
pub use dump::SummaryCount;
pub use metrics_exporter_prometheus::PrometheusHandle;
pub use recorder::init_default_metrics_recorder;
pub use recorder::label_counter;
pub use recorder::label_counter_with_val;
pub use recorder::try_handle;
