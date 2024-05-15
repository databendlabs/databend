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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_histogram_family;
use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::FamilyHistogram;
use futures::FutureExt;
use futures::TryFutureExt;
use opendal::raw::oio;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal::raw::LayeredAccess;
use opendal::raw::OpBatch;
use opendal::raw::OpCreateDir;
use opendal::raw::OpDelete;
use opendal::raw::OpList;
use opendal::raw::OpPresign;
use opendal::raw::OpRead;
use opendal::raw::OpStat;
use opendal::raw::OpWrite;
use opendal::raw::Operation;
use opendal::raw::RpBatch;
use opendal::raw::RpCreateDir;
use opendal::raw::RpDelete;
use opendal::raw::RpList;
use opendal::raw::RpPresign;
use opendal::raw::RpRead;
use opendal::raw::RpStat;
use opendal::raw::RpWrite;
use opendal::Buffer;
use opendal::ErrorKind;
use opendal::Scheme;
use prometheus_client::metrics::histogram::exponential_buckets;

type OperationLabels = [(&'static str, &'static str); 2];
type ErrorLabels = [(&'static str, &'static str); 3];

pub static METRICS_LAYER: LazyLock<MetricsLayer> = LazyLock::new(|| MetricsLayer {
    metrics: Arc::new(MetricsRecorder::new()),
});

#[derive(Debug, Clone)]
pub struct MetricsRecorder {
    /// Total counter of the specific operation be called.
    requests_total: FamilyCounter<OperationLabels>,
    /// Total counter of the errors.
    errors_total: FamilyCounter<ErrorLabels>,
    /// Latency of the specific operation be called.
    request_duration_seconds: FamilyHistogram<OperationLabels>,
    /// The histogram of bytes
    bytes_histogram: FamilyHistogram<OperationLabels>,
    /// The counter of bytes
    bytes_total: FamilyCounter<OperationLabels>,
}

impl MetricsRecorder {
    pub fn new() -> Self {
        MetricsRecorder {
            requests_total: register_counter_family("opendal_requests"),
            errors_total: register_counter_family("opendal_errors"),
            request_duration_seconds: register_histogram_family(
                "opendal_request_duration_seconds",
                exponential_buckets(0.01, 2.0, 16),
            ),
            bytes_histogram: register_histogram_family(
                "opendal_bytes_histogram",
                exponential_buckets(1.0, 2.0, 16),
            ),
            bytes_total: register_counter_family("opendal_bytes"),
        }
    }

    fn increment_errors_total(&self, scheme: Scheme, op: Operation, err: ErrorKind) {
        let labels = [
            ("scheme", scheme.into_static()),
            ("op", op.into_static()),
            ("err", err.into_static()),
        ];
        self.errors_total.get_or_create(&labels).inc();
    }

    fn increment_request_total(&self, scheme: Scheme, op: Operation) {
        let labels = [("scheme", scheme.into_static()), ("op", op.into_static())];
        self.requests_total.get_or_create(&labels).inc();
    }

    fn observe_bytes_total(&self, scheme: Scheme, op: Operation, bytes: usize) {
        let labels = [("scheme", scheme.into_static()), ("op", op.into_static())];
        self.bytes_histogram
            .get_or_create(&labels)
            .observe(bytes as f64);
        self.bytes_total.get_or_create(&labels).inc_by(bytes as u64);
    }

    fn observe_request_duration(&self, scheme: Scheme, op: Operation, duration: Duration) {
        let labels = [("scheme", scheme.into_static()), ("op", op.into_static())];
        self.request_duration_seconds
            .get_or_create(&labels)
            .observe(duration.as_secs_f64());
    }
}

#[derive(Debug, Clone)]
pub struct MetricsLayer {
    metrics: Arc<MetricsRecorder>,
}

impl<A: Access> Layer<A> for MetricsLayer {
    type LayeredAccess = MetricsLayerAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let meta = inner.info();
        let scheme = meta.scheme();

        MetricsLayerAccessor {
            inner,
            metrics: self.metrics.clone(),
            scheme,
        }
    }
}

#[derive(Clone)]
pub struct MetricsLayerAccessor<A: Access> {
    inner: A,
    scheme: Scheme,
    metrics: Arc<MetricsRecorder>,
}

impl<A: Access> Debug for MetricsLayerAccessor<A> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("MetricsAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access> LayeredAccess for MetricsLayerAccessor<A> {
    type Inner = A;
    type Reader = OperatorMetricsWrapper<A::Reader>;
    type BlockingReader = OperatorMetricsWrapper<A::BlockingReader>;
    type Writer = OperatorMetricsWrapper<A::Writer>;
    type BlockingWriter = OperatorMetricsWrapper<A::BlockingWriter>;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> opendal::Result<RpCreateDir> {
        self.metrics
            .increment_request_total(self.scheme, Operation::CreateDir);

        let start_time = Instant::now();
        let create_res = self.inner.create_dir(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::CreateDir,
            start_time.elapsed(),
        );
        create_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::CreateDir, e.kind());
            e
        })
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Read);

        let read_res = self
            .inner
            .read(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        OperatorMetricsWrapper::new(
                            r,
                            Operation::Read,
                            self.metrics.clone(),
                            self.scheme,
                        ),
                    )
                })
            })
            .await;
        read_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Read, e.kind());
            e
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Write);

        let write_res = self
            .inner
            .write(path, args)
            .map(|v| {
                v.map(|(rp, r)| {
                    (
                        rp,
                        OperatorMetricsWrapper::new(
                            r,
                            Operation::Write,
                            self.metrics.clone(),
                            self.scheme,
                        ),
                    )
                })
            })
            .await;

        write_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Write, e.kind());
            e
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> opendal::Result<RpStat> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Stat);
        let start_time = Instant::now();

        let stat_res = self
            .inner
            .stat(path, args)
            .inspect_err(|e| {
                self.metrics
                    .increment_errors_total(self.scheme, Operation::Stat, e.kind());
            })
            .await;

        self.metrics
            .observe_request_duration(self.scheme, Operation::Stat, start_time.elapsed());
        stat_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Stat, e.kind());
            e
        })
    }

    async fn delete(&self, path: &str, args: OpDelete) -> opendal::Result<RpDelete> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Delete);
        let start_time = Instant::now();

        let delete_res = self.inner.delete(path, args).await;

        self.metrics
            .observe_request_duration(self.scheme, Operation::Delete, start_time.elapsed());
        delete_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Delete, e.kind());
            e
        })
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::List);
        let start_time = Instant::now();

        let list_res = self.inner.list(path, args).await;

        self.metrics
            .observe_request_duration(self.scheme, Operation::List, start_time.elapsed());
        list_res.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::List, e.kind());
            e
        })
    }

    async fn batch(&self, args: OpBatch) -> opendal::Result<RpBatch> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Batch);
        let start_time = Instant::now();

        let result = self.inner.batch(args).await;

        self.metrics
            .observe_request_duration(self.scheme, Operation::Batch, start_time.elapsed());
        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Batch, e.kind());
            e
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> opendal::Result<RpPresign> {
        self.metrics
            .increment_request_total(self.scheme, Operation::Presign);
        let start_time = Instant::now();

        let result = self.inner.presign(path, args).await;

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::Presign,
            start_time.elapsed(),
        );
        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::Presign, e.kind());
            e
        })
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> opendal::Result<RpCreateDir> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingCreateDir);
        let start_time = Instant::now();

        let result = self.inner.blocking_create_dir(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingCreateDir,
            start_time.elapsed(),
        );
        result.map_err(|e| {
            self.metrics.increment_errors_total(
                self.scheme,
                Operation::BlockingCreateDir,
                e.kind(),
            );
            e
        })
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<(RpRead, Self::BlockingReader)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingRead);

        let result = self.inner.blocking_read(path, args).map(|(rp, r)| {
            (
                rp,
                OperatorMetricsWrapper::new(
                    r,
                    Operation::BlockingRead,
                    self.metrics.clone(),
                    self.scheme,
                ),
            )
        });

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingRead, e.kind());
            e
        })
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<(RpWrite, Self::BlockingWriter)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingWrite);

        let result = self.inner.blocking_write(path, args).map(|(rp, r)| {
            (
                rp,
                OperatorMetricsWrapper::new(
                    r,
                    Operation::BlockingWrite,
                    self.metrics.clone(),
                    self.scheme,
                ),
            )
        });

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingWrite, e.kind());
            e
        })
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> opendal::Result<RpStat> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingStat);
        let start_time = Instant::now();

        let result = self.inner.blocking_stat(path, args);
        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingStat,
            start_time.elapsed(),
        );

        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingStat, e.kind());
            e
        })
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> opendal::Result<RpDelete> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingDelete);
        let start_time = Instant::now();

        let result = self.inner.blocking_delete(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingDelete,
            start_time.elapsed(),
        );
        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingDelete, e.kind());
            e
        })
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> opendal::Result<(RpList, Self::BlockingLister)> {
        self.metrics
            .increment_request_total(self.scheme, Operation::BlockingList);
        let start_time = Instant::now();

        let result = self.inner.blocking_list(path, args);

        self.metrics.observe_request_duration(
            self.scheme,
            Operation::BlockingList,
            start_time.elapsed(),
        );
        result.map_err(|e| {
            self.metrics
                .increment_errors_total(self.scheme, Operation::BlockingList, e.kind());
            e
        })
    }
}

pub struct OperatorMetricsWrapper<R> {
    inner: R,

    op: Operation,
    metrics: Arc<MetricsRecorder>,
    scheme: Scheme,
}

impl<R> OperatorMetricsWrapper<R> {
    fn new(inner: R, op: Operation, metrics: Arc<MetricsRecorder>, scheme: Scheme) -> Self {
        Self {
            inner,
            op,
            metrics,
            scheme,
        }
    }
}

impl<R: oio::Read> oio::Read for OperatorMetricsWrapper<R> {
    async fn read_at(&self, offset: u64, limit: usize) -> opendal::Result<Buffer> {
        let start = Instant::now();

        self.inner
            .read_at(offset, limit)
            .await
            .map(|res| {
                self.metrics
                    .observe_bytes_total(self.scheme, self.op, res.len());
                self.metrics
                    .observe_request_duration(self.scheme, self.op, start.elapsed());
                res
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, err.kind());
                err
            })
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for OperatorMetricsWrapper<R> {
    fn read_at(&self, offset: u64, limit: usize) -> opendal::Result<Buffer> {
        let start = Instant::now();

        self.inner
            .read_at(offset, limit)
            .map(|res| {
                self.metrics
                    .observe_bytes_total(self.scheme, self.op, res.len());
                self.metrics
                    .observe_request_duration(self.scheme, self.op, start.elapsed());
                res
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, err.kind());
                err
            })
    }
}

impl<R: oio::Write> oio::Write for OperatorMetricsWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> opendal::Result<usize> {
        let start = Instant::now();

        self.inner
            .write(bs)
            .await
            .map(|res| {
                self.metrics.observe_bytes_total(self.scheme, self.op, res);
                self.metrics
                    .observe_request_duration(self.scheme, self.op, start.elapsed());
                res
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, err.kind());
                err
            })
    }

    async fn close(&mut self) -> opendal::Result<()> {
        self.inner.close().await.map_err(|err| {
            self.metrics
                .increment_errors_total(self.scheme, self.op, err.kind());
            err
        })
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        self.inner.abort().await.map_err(|err| {
            self.metrics
                .increment_errors_total(self.scheme, self.op, err.kind());
            err
        })
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for OperatorMetricsWrapper<R> {
    fn write(&mut self, bs: Buffer) -> opendal::Result<usize> {
        let start = Instant::now();

        self.inner
            .write(bs)
            .map(|res| {
                self.metrics.observe_bytes_total(self.scheme, self.op, res);
                self.metrics
                    .observe_request_duration(self.scheme, self.op, start.elapsed());
                res
            })
            .map_err(|err| {
                self.metrics
                    .increment_errors_total(self.scheme, self.op, err.kind());
                err
            })
    }

    fn close(&mut self) -> opendal::Result<()> {
        self.inner.close().map_err(|err| {
            self.metrics
                .increment_errors_total(self.scheme, self.op, err.kind());
            err
        })
    }
}
