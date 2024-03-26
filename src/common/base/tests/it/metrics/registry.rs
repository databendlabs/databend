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

use databend_common_base::base::GlobalUniqName;
use databend_common_base::runtime::metrics::{GLOBAL_METRICS_REGISTRY, MetricSample, MetricValue, register_counter, ScopedRegistry};
use databend_common_base::runtime::ThreadTracker;
use databend_common_exception::Result;

fn assert_contain_metric(samples: Vec<MetricSample>, expected: MetricSample) {
    for sample in samples {
        if sample.name == expected.name {
            assert_eq!(sample.value, expected.value);
            assert_eq!(sample.labels, expected.labels);
            return;
        }
    }

    assert!(false);
}

#[test]
fn test_tracking_scoped_counter_metrics() -> Result<()> {
    let uniq_metric_name = GlobalUniqName::unique();
    let counter = register_counter(&uniq_metric_name);
    counter.inc();

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        counter.inc();

        assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Counter(2_f64),
        });

        assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Counter(1_f64),
        });
    }

    // untracking assert
    counter.inc();

    assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Counter(3_f64),
    });

    assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Counter(1_f64),
    });

    Ok(())
}