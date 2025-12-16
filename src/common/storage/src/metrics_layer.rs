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

use std::fmt;
use std::fmt::Debug;
use std::sync::LazyLock;
use std::time::Duration;

use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::FamilyGauge;
use databend_common_base::runtime::metrics::FamilyHistogram;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_gauge_family;
use databend_common_base::runtime::metrics::register_histogram_family;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal_layer_observe_metrics_common as observe;
use prometheus_client::encoding::EncodeLabel;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::LabelSetEncoder;

pub static METRICS_LAYER: LazyLock<MetricsLayer> = LazyLock::new(|| MetricsLayer {
    metrics: MetricsRecorder::new(),
});

#[derive(Debug, Clone)]
pub struct MetricsLayer {
    metrics: MetricsRecorder,
}

impl<A: Access> Layer<A> for MetricsLayer {
    type LayeredAccess = observe::MetricsAccessor<A, MetricsRecorder>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        observe::MetricsLayer::new(self.metrics.clone()).layer(inner)
    }
}

#[derive(Debug, Clone)]
pub struct MetricsRecorder {
    operation_bytes: FamilyHistogram<OperationLabels>,
    operation_bytes_rate: FamilyHistogram<OperationLabels>,
    operation_entries: FamilyHistogram<OperationLabels>,
    operation_entries_rate: FamilyHistogram<OperationLabels>,
    operation_duration_seconds: FamilyHistogram<OperationLabels>,
    operation_errors_total: FamilyCounter<OperationLabels>,
    operation_executing: FamilyGauge<OperationLabels>,
    operation_ttfb_seconds: FamilyHistogram<OperationLabels>,

    http_executing: FamilyGauge<OperationLabels>,
    http_request_bytes: FamilyHistogram<OperationLabels>,
    http_request_bytes_rate: FamilyHistogram<OperationLabels>,
    http_request_duration_seconds: FamilyHistogram<OperationLabels>,
    http_response_bytes: FamilyHistogram<OperationLabels>,
    http_response_bytes_rate: FamilyHistogram<OperationLabels>,
    http_response_duration_seconds: FamilyHistogram<OperationLabels>,
    http_connection_errors_total: FamilyCounter<OperationLabels>,
    http_status_errors_total: FamilyCounter<OperationLabels>,
}

impl MetricsRecorder {
    pub fn new() -> Self {
        let operation_bytes = register_histogram_family(
            observe::MetricValue::OperationBytes(0).name(),
            observe::DEFAULT_BYTES_BUCKETS.to_vec().into_iter(),
        );
        let operation_bytes_rate = register_histogram_family(
            observe::MetricValue::OperationBytesRate(0.0).name(),
            observe::DEFAULT_BYTES_RATE_BUCKETS.to_vec().into_iter(),
        );
        let operation_entries = register_histogram_family(
            observe::MetricValue::OperationEntries(0).name(),
            observe::DEFAULT_ENTRIES_BUCKETS.to_vec().into_iter(),
        );
        let operation_entries_rate = register_histogram_family(
            observe::MetricValue::OperationEntriesRate(0.0).name(),
            observe::DEFAULT_ENTRIES_RATE_BUCKETS.to_vec().into_iter(),
        );
        let operation_duration_seconds = register_histogram_family(
            observe::MetricValue::OperationDurationSeconds(Duration::default()).name(),
            observe::DEFAULT_DURATION_SECONDS_BUCKETS
                .to_vec()
                .into_iter(),
        );
        let operation_errors_total =
            register_counter_family(observe::MetricValue::OperationErrorsTotal.name());
        let operation_executing =
            register_gauge_family(observe::MetricValue::OperationExecuting(0).name());
        let operation_ttfb_seconds = register_histogram_family(
            observe::MetricValue::OperationTtfbSeconds(Duration::default()).name(),
            observe::DEFAULT_TTFB_BUCKETS.to_vec().into_iter(),
        );

        let http_executing = register_gauge_family(observe::MetricValue::HttpExecuting(0).name());
        let http_request_bytes = register_histogram_family(
            observe::MetricValue::HttpRequestBytes(0).name(),
            observe::DEFAULT_BYTES_BUCKETS.to_vec().into_iter(),
        );
        let http_request_bytes_rate = register_histogram_family(
            observe::MetricValue::HttpRequestBytesRate(0.0).name(),
            observe::DEFAULT_BYTES_RATE_BUCKETS.to_vec().into_iter(),
        );
        let http_request_duration_seconds = register_histogram_family(
            observe::MetricValue::HttpRequestDurationSeconds(Duration::default()).name(),
            observe::DEFAULT_DURATION_SECONDS_BUCKETS
                .to_vec()
                .into_iter(),
        );
        let http_response_bytes = register_histogram_family(
            observe::MetricValue::HttpResponseBytes(0).name(),
            observe::DEFAULT_BYTES_BUCKETS.to_vec().into_iter(),
        );
        let http_response_bytes_rate = register_histogram_family(
            observe::MetricValue::HttpResponseBytesRate(0.0).name(),
            observe::DEFAULT_BYTES_RATE_BUCKETS.to_vec().into_iter(),
        );
        let http_response_duration_seconds = register_histogram_family(
            observe::MetricValue::HttpResponseDurationSeconds(Duration::default()).name(),
            observe::DEFAULT_DURATION_SECONDS_BUCKETS
                .to_vec()
                .into_iter(),
        );
        let http_connection_errors_total =
            register_counter_family(observe::MetricValue::HttpConnectionErrorsTotal.name());
        let http_status_errors_total =
            register_counter_family(observe::MetricValue::HttpStatusErrorsTotal.name());

        MetricsRecorder {
            operation_bytes,
            operation_bytes_rate,
            operation_entries,
            operation_entries_rate,
            operation_duration_seconds,
            operation_errors_total,
            operation_executing,
            operation_ttfb_seconds,

            http_executing,
            http_request_bytes,
            http_request_bytes_rate,
            http_request_duration_seconds,
            http_response_bytes,
            http_response_bytes_rate,
            http_response_duration_seconds,
            http_connection_errors_total,
            http_status_errors_total,
        }
    }
}

impl observe::MetricsIntercept for MetricsRecorder {
    fn observe(&self, labels: observe::MetricLabels, value: observe::MetricValue) {
        let labels = OperationLabels(labels);
        match value {
            observe::MetricValue::OperationBytes(v) => self
                .operation_bytes
                .get_or_create(&labels)
                .observe(v as f64),
            observe::MetricValue::OperationBytesRate(v) => {
                self.operation_bytes_rate.get_or_create(&labels).observe(v)
            }
            observe::MetricValue::OperationEntries(v) => self
                .operation_entries
                .get_or_create(&labels)
                .observe(v as f64),
            observe::MetricValue::OperationEntriesRate(v) => self
                .operation_entries_rate
                .get_or_create(&labels)
                .observe(v),
            observe::MetricValue::OperationDurationSeconds(v) => self
                .operation_duration_seconds
                .get_or_create(&labels)
                .observe(v.as_secs_f64()),
            observe::MetricValue::OperationErrorsTotal => {
                self.operation_errors_total.get_or_create(&labels).inc();
            }
            observe::MetricValue::OperationExecuting(v) => {
                self.operation_executing
                    .get_or_create(&labels)
                    .inc_by(v as i64);
            }
            observe::MetricValue::OperationTtfbSeconds(v) => self
                .operation_ttfb_seconds
                .get_or_create(&labels)
                .observe(v.as_secs_f64()),

            observe::MetricValue::HttpExecuting(v) => {
                self.http_executing.get_or_create(&labels).inc_by(v as i64);
            }
            observe::MetricValue::HttpRequestBytes(v) => self
                .http_request_bytes
                .get_or_create(&labels)
                .observe(v as f64),
            observe::MetricValue::HttpRequestBytesRate(v) => self
                .http_request_bytes_rate
                .get_or_create(&labels)
                .observe(v),
            observe::MetricValue::HttpRequestDurationSeconds(v) => self
                .http_request_duration_seconds
                .get_or_create(&labels)
                .observe(v.as_secs_f64()),
            observe::MetricValue::HttpResponseBytes(v) => self
                .http_response_bytes
                .get_or_create(&labels)
                .observe(v as f64),
            observe::MetricValue::HttpResponseBytesRate(v) => self
                .http_response_bytes_rate
                .get_or_create(&labels)
                .observe(v),
            observe::MetricValue::HttpResponseDurationSeconds(v) => self
                .http_response_duration_seconds
                .get_or_create(&labels)
                .observe(v.as_secs_f64()),
            observe::MetricValue::HttpConnectionErrorsTotal => {
                self.http_connection_errors_total
                    .get_or_create(&labels)
                    .inc();
            }
            observe::MetricValue::HttpStatusErrorsTotal => {
                self.http_status_errors_total.get_or_create(&labels).inc();
            }
            _ => {}
        };
    }
}

/// observe::MetricLabels contains root but we don't want it.
#[derive(Clone, Debug)]
struct OperationLabels(observe::MetricLabels);

impl PartialEq for OperationLabels {
    fn eq(&self, other: &Self) -> bool {
        self.0.scheme == other.0.scheme
            && self.0.namespace == other.0.namespace
            && self.0.operation == other.0.operation
            && self.0.error == other.0.error
            && self.0.status_code == other.0.status_code
    }
}

impl Eq for OperationLabels {}

impl std::hash::Hash for OperationLabels {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.scheme.hash(state);
        self.0.namespace.hash(state);
        self.0.operation.hash(state);
        self.0.error.hash(state);
        self.0.status_code.hash(state);
    }
}

impl EncodeLabelSet for OperationLabels {
    fn encode(&self, mut encoder: LabelSetEncoder) -> Result<(), fmt::Error> {
        (observe::LABEL_SCHEME, self.0.scheme).encode(encoder.encode_label())?;
        (observe::LABEL_NAMESPACE, self.0.namespace.as_ref()).encode(encoder.encode_label())?;
        (observe::LABEL_OPERATION, self.0.operation).encode(encoder.encode_label())?;

        if let Some(error) = &self.0.error {
            (observe::LABEL_ERROR, error.into_static()).encode(encoder.encode_label())?;
        }
        if let Some(code) = &self.0.status_code {
            (observe::LABEL_STATUS_CODE, code.as_str()).encode(encoder.encode_label())?;
        }
        Ok(())
    }
}
