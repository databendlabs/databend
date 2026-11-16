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
use std::sync::Once;
use std::sync::mpsc;
use std::time::Duration;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::metrics::BUCKET_MILLISECONDS;
use databend_common_base::runtime::metrics::BUCKET_SECONDS;
use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use databend_common_base::runtime::metrics::HistogramCount;
use databend_common_base::runtime::metrics::MAX_HISTOGRAM_BOUND;
use databend_common_base::runtime::metrics::MetricSample;
use databend_common_base::runtime::metrics::MetricValue;
use databend_common_base::runtime::metrics::SampleCollector;
use databend_common_base::runtime::metrics::ScopedRegistry;
use databend_common_base::runtime::metrics::register_counter;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_gauge;
use databend_common_base::runtime::metrics::register_histogram_in_milliseconds;
use databend_common_base::runtime::metrics::register_histogram_in_seconds;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::gauge::ConstGauge;

fn assert_contain_metric(samples: Vec<MetricSample>, expected: MetricSample) {
    for sample in samples {
        if sample.name == expected.name && sample.labels == expected.labels {
            assert_eq!(sample.value, expected.value);
            return;
        }
    }

    panic!()
}

fn assert_contain_metrics(samples: Vec<MetricSample>, expected: Vec<MetricSample>) {
    for expected in expected {
        assert_contain_metric(samples.clone(), expected);
    }
}

#[test]
fn test_tracking_scoped_counter_metrics() -> anyhow::Result<()> {
    let uniq_metric_name = GlobalUniq::unique();
    let counter = register_counter(&uniq_metric_name);
    let uniq_metric_name = format!("{}_total", uniq_metric_name);
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

#[test]
fn test_tracking_scoped_gauge_metrics() -> anyhow::Result<()> {
    let uniq_metric_name = GlobalUniq::unique();
    let gauge = register_gauge(&uniq_metric_name);
    gauge.inc();

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        gauge.inc();

        assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Gauge(2_f64),
        });

        assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Gauge(1_f64),
        });
    }

    // untracking assert
    gauge.inc();

    assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Gauge(3_f64),
    });

    assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Gauge(1_f64),
    });

    Ok(())
}

#[test]
fn test_tracking_scoped_histogram_in_seconds_metrics() -> anyhow::Result<()> {
    let uniq_metric_name = GlobalUniq::unique();
    let histogram = register_histogram_in_seconds(&uniq_metric_name);
    histogram.observe(28801.0);

    fn seconds_histogram_value(v: f64) -> Vec<HistogramCount> {
        BUCKET_SECONDS
            .iter()
            .map(|lt| HistogramCount {
                less_than: *lt,
                count: 0.0,
            })
            .chain(std::iter::once(HistogramCount {
                less_than: MAX_HISTOGRAM_BOUND,
                count: v,
            }))
            .collect::<Vec<_>>()
    }

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        histogram.observe(28801.0);

        assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Histogram(seconds_histogram_value(2_f64)),
        });

        assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Histogram(seconds_histogram_value(1_f64)),
        });
    }

    // untracking assert
    histogram.observe(28801.0);

    assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Histogram(seconds_histogram_value(3_f64)),
    });

    assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Histogram(seconds_histogram_value(1_f64)),
    });

    Ok(())
}

#[test]
fn test_tracking_scoped_histogram_in_milliseconds_metrics() -> anyhow::Result<()> {
    let uniq_metric_name = GlobalUniq::unique();
    let histogram = register_histogram_in_milliseconds(&uniq_metric_name);
    histogram.observe(28800001.0);

    fn milliseconds_histogram_value(v: f64) -> Vec<HistogramCount> {
        BUCKET_MILLISECONDS
            .iter()
            .map(|lt| HistogramCount {
                less_than: *lt,
                count: 0.0,
            })
            .chain(std::iter::once(HistogramCount {
                less_than: MAX_HISTOGRAM_BOUND,
                count: v,
            }))
            .collect::<Vec<_>>()
    }

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        histogram.observe(28800001.0);

        assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Histogram(milliseconds_histogram_value(2_f64)),
        });

        assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Histogram(milliseconds_histogram_value(1_f64)),
        });
    }

    // untracking assert
    histogram.observe(28800001.0);

    assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Histogram(milliseconds_histogram_value(3_f64)),
    });

    assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Histogram(milliseconds_histogram_value(1_f64)),
    });

    Ok(())
}

#[test]
fn test_tracking_histogram_inf() -> anyhow::Result<()> {
    let uniq_metric_name = GlobalUniq::unique();
    let histogram = register_histogram_in_seconds(&uniq_metric_name);
    // observe a value that exceed the max bucket of the histogram
    histogram.observe(3600.0 * 355.0);

    let output = GLOBAL_METRICS_REGISTRY.render_metrics()?;
    let expected = format!("{}_bucket{{le=\"+Inf\"}} 1", uniq_metric_name);
    assert!(output.contains(&expected));
    Ok(())
}

#[test]
fn test_tracking_scoped_family_counter_metrics() -> anyhow::Result<()> {
    let uniq_metric_name = GlobalUniq::unique();
    let counter = register_counter_family::<Vec<(&'static str, u64)>>(&uniq_metric_name);
    let uniq_metric_name = format!("{}_total", uniq_metric_name);
    counter.get_or_create(&vec![("TEST_LABEL1", 1)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL1", 2)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL2", 1)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL3", 1)]).inc();

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        counter.get_or_create(&vec![("TEST_LABEL1", 1)]).inc();
        counter.get_or_create(&vec![("TEST_LABEL1", 2)]).inc();
        counter.get_or_create(&vec![("TEST_LABEL1", 3)]).inc();
        counter.get_or_create(&vec![("TEST_LABEL2", 1)]).inc();
        counter.get_or_create(&vec![("TEST_LABEL4", 1)]).inc();

        assert_contain_metrics(GLOBAL_METRICS_REGISTRY.dump_sample()?, vec![
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "1".to_string())]),
                value: MetricValue::Counter(2_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
                value: MetricValue::Counter(2_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "3".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL2".to_string(), "1".to_string())]),
                value: MetricValue::Counter(2_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL3".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL4".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
        ]);

        assert_contain_metrics(scoped_registry.dump_sample()?, vec![
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "3".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL2".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL4".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
        ]);
    }

    // untracking assert
    counter.get_or_create(&vec![("TEST_LABEL1", 1)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL1", 2)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL2", 1)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL3", 1)]).inc();

    assert_contain_metrics(GLOBAL_METRICS_REGISTRY.dump_sample()?, vec![
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "1".to_string())]),
            value: MetricValue::Counter(3_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
            value: MetricValue::Counter(3_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "3".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL2".to_string(), "1".to_string())]),
            value: MetricValue::Counter(3_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL3".to_string(), "1".to_string())]),
            value: MetricValue::Counter(2_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL4".to_string(), "1".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
    ]);

    assert_contain_metrics(scoped_registry.dump_sample()?, vec![
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "1".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "3".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL2".to_string(), "1".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL4".to_string(), "1".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
    ]);

    Ok(())
}

#[derive(Debug)]
struct ReadOnReadCollector {
    name: String,
}

impl Collector for ReadOnReadCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let family = encoder.encode_descriptor(&self.name, "", None, MetricType::Gauge)?;
        ConstGauge::new(7i64).encode(family)
    }
}

impl SampleCollector for ReadOnReadCollector {
    fn sample(&self, samples: &mut Vec<MetricSample>) {
        samples.push(MetricSample {
            name: self.name.clone(),
            labels: HashMap::new(),
            value: MetricValue::Gauge(7.0),
        });
    }
}

/// A sample collector reaches both `dump_sample`, which serves `system.metrics`, and the
/// scrape; a plain collector only the scrape.
#[test]
fn test_sample_collector_is_dumped_and_rendered() -> anyhow::Result<()> {
    let name = format!("sampled_{}", GlobalUniq::unique());
    GLOBAL_METRICS_REGISTRY
        .register_sample_collector(Arc::new(ReadOnReadCollector { name: name.clone() }));
    let scrape_only = format!("scraped_{}", GlobalUniq::unique());
    GLOBAL_METRICS_REGISTRY.register_collector(Box::new(ReadOnReadCollector {
        name: scrape_only.clone(),
    }));

    let samples = GLOBAL_METRICS_REGISTRY.dump_sample()?;
    assert_contain_metric(samples.clone(), MetricSample {
        name: name.clone(),
        labels: HashMap::new(),
        value: MetricValue::Gauge(7.0),
    });
    assert!(!samples.iter().any(|sample| sample.name == scrape_only));

    let text = GLOBAL_METRICS_REGISTRY.render_metrics()?;
    assert!(text.contains(&format!("databend_{name} 7")), "{text}");
    assert!(
        text.contains(&format!("databend_{scrape_only} 7")),
        "{text}"
    );

    Ok(())
}

/// Registers a metric on its first read, which takes the registry's lock.
#[derive(Debug)]
struct RegistersOnRead {
    name: String,
    registered: Once,
}

impl Collector for RegistersOnRead {
    fn encode(&self, _encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        Ok(())
    }
}

impl SampleCollector for RegistersOnRead {
    fn sample(&self, samples: &mut Vec<MetricSample>) {
        self.registered.call_once(|| {
            register_counter(&format!("{}_registered", self.name)).inc();
        });
        samples.push(MetricSample {
            name: self.name.clone(),
            labels: HashMap::new(),
            value: MetricValue::Gauge(1.0),
        });
    }
}

/// `dump_sample` reads the sample collectors after releasing the registry's lock, so a slow
/// collector holds up neither registration nor the scoped registries of running queries.
#[test]
fn test_sample_collector_reads_outside_registry_lock() -> anyhow::Result<()> {
    let name = format!("registers_{}", GlobalUniq::unique());
    GLOBAL_METRICS_REGISTRY.register_sample_collector(Arc::new(RegistersOnRead {
        name: name.clone(),
        registered: Once::new(),
    }));

    let (sender, receiver) = mpsc::channel();
    Thread::spawn(move || {
        let _ = sender.send(GLOBAL_METRICS_REGISTRY.dump_sample().map(|s| s.len()));
    });
    let dumped = receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("dump_sample held the registry lock while reading a collector");
    assert!(dumped? > 0);
    assert!(
        GLOBAL_METRICS_REGISTRY
            .dump_sample()?
            .iter()
            .any(|sample| sample.name == name)
    );
    Ok(())
}
