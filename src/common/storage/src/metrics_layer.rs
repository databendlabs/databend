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
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_histogram_family;
use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::FamilyHistogram;
use opendal::layers::observe;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal::raw::Operation;
use opendal::ErrorKind;
use opendal::Scheme;
use prometheus_client::encoding::EncodeLabel;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::LabelSetEncoder;
use prometheus_client::metrics::histogram::exponential_buckets;

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
    /// Latency of the specific operation be called.
    operation_duration_seconds: FamilyHistogram<OperationLabels>,
    /// The histogram of bytes
    operation_bytes: FamilyHistogram<OperationLabels>,
    /// Total counter of the specific operation be called.
    operation_errors_total: FamilyCounter<OperationLabels>,
}

impl MetricsRecorder {
    pub fn new() -> Self {
        MetricsRecorder {
            operation_duration_seconds: register_histogram_family(
                &observe::METRIC_OPERATION_DURATION_SECONDS.name(),
                exponential_buckets(0.01, 2.0, 16),
            ),
            operation_bytes: register_histogram_family(
                &observe::METRIC_OPERATION_BYTES.name(),
                exponential_buckets(1.0, 2.0, 16),
            ),
            operation_errors_total: register_counter_family("opendal_operation_errors"),
        }
    }
}

impl observe::MetricsIntercept for MetricsRecorder {
    fn observe_operation_duration_seconds(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        _path: &str,
        op: Operation,
        duration: Duration,
    ) {
        self.operation_duration_seconds
            .get_or_create(&OperationLabels {
                scheme,
                namespace,
                root,
                operation: op,
                path: None,
                error: None,
            })
            .observe(duration.as_secs_f64())
    }

    fn observe_operation_bytes(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        _path: &str,
        op: Operation,
        bytes: usize,
    ) {
        self.operation_bytes
            .get_or_create(&OperationLabels {
                scheme,
                namespace,
                root,
                operation: op,
                path: None,
                error: None,
            })
            .observe(bytes as f64)
    }

    fn observe_operation_errors_total(
        &self,
        scheme: Scheme,
        namespace: Arc<String>,
        root: Arc<String>,
        _path: &str,
        op: Operation,
        error: ErrorKind,
    ) {
        self.operation_errors_total
            .get_or_create(&OperationLabels {
                scheme,
                namespace,
                root,
                operation: op,
                path: None,
                error: Some(error.into_static()),
            })
            .inc();
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct OperationLabels {
    scheme: Scheme,
    namespace: Arc<String>,
    root: Arc<String>,
    operation: Operation,
    path: Option<String>,
    error: Option<&'static str>,
}

impl EncodeLabelSet for OperationLabels {
    fn encode(&self, mut encoder: LabelSetEncoder) -> Result<(), fmt::Error> {
        (observe::LABEL_SCHEME, self.scheme.into_static()).encode(encoder.encode_label())?;
        (observe::LABEL_NAMESPACE, self.namespace.as_str()).encode(encoder.encode_label())?;
        (observe::LABEL_ROOT, self.root.as_str()).encode(encoder.encode_label())?;
        (observe::LABEL_OPERATION, self.operation.into_static()).encode(encoder.encode_label())?;
        if let Some(path) = &self.path {
            (observe::LABEL_PATH, path.as_str()).encode(encoder.encode_label())?;
        }
        if let Some(error) = self.error {
            (observe::LABEL_ERROR, error).encode(encoder.encode_label())?;
        }
        Ok(())
    }
}
