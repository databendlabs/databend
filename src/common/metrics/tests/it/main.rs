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

use databend_common_exception::ErrorCode;
use databend_common_metrics::dump_metric_samples;
use databend_common_metrics::load_global_prometheus_registry;
use databend_common_metrics::register_counter;
use databend_common_metrics::register_histogram_in_milliseconds;
use databend_common_metrics::MetricValue;

#[tokio::test(flavor = "multi_thread")]
async fn test_dump_metric_samples() -> databend_common_exception::Result<()> {
    let counter1 = register_counter("test_test1_count");
    let counter2 = register_counter("test_test2_count");
    let histogram1 = register_histogram_in_milliseconds("test_test_query_usedtime");
    counter1.inc();
    counter2.inc_by(2);
    histogram1.observe(2.0);

    let registry = load_global_prometheus_registry();
    let samples = dump_metric_samples(registry.inner())
        .unwrap()
        .into_iter()
        .map(|s| (s.name.clone(), s))
        .collect::<HashMap<_, _>>();
    assert_eq!(
        MetricValue::Untyped(1.0),
        samples.get("test_test1_count_total").unwrap().value
    );

    let histogram = match &samples.get("test_test_query_usedtime").unwrap().value {
        MetricValue::Histogram(histogram) => histogram,
        _ => return Err(ErrorCode::Internal("test failed")),
    };
    assert_eq!(16, histogram.len());

    Ok(())
}
