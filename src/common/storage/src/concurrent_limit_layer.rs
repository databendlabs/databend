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

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Ported from Apache OpenDAL 0.54.1 `src/layers/concurrent_limit.rs`.

use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_base::runtime::metrics::FamilyGauge;
use databend_common_base::runtime::metrics::register_gauge_family;
use opendal::Buffer;
use opendal::Metadata;
use opendal::Result;
use opendal::layers::observe;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal::raw::LayeredAccess;
use opendal::raw::OpCreateDir;
use opendal::raw::OpDelete;
use opendal::raw::OpList;
use opendal::raw::OpRead;
use opendal::raw::OpStat;
use opendal::raw::OpWrite;
use opendal::raw::RpCreateDir;
use opendal::raw::RpDelete;
use opendal::raw::RpList;
use opendal::raw::RpRead;
use opendal::raw::RpStat;
use opendal::raw::RpWrite;
use opendal::raw::oio;
use prometheus_client::encoding::EncodeLabel;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::LabelSetEncoder;
use tokio::sync::Semaphore;
use tokio::sync::SemaphorePermit;
use tokio::sync::TryAcquireError;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ConcurrentLimitLabels {
    scheme: &'static str,
    namespace: Arc<str>,
}

impl EncodeLabelSet for ConcurrentLimitLabels {
    fn encode(&self, mut encoder: LabelSetEncoder) -> Result<(), fmt::Error> {
        (observe::LABEL_SCHEME, self.scheme).encode(encoder.encode_label())?;
        (observe::LABEL_NAMESPACE, self.namespace.as_ref()).encode(encoder.encode_label())?;
        Ok(())
    }
}

static CONCURRENT_LIMIT_QUEUED_OPERATIONS: LazyLock<FamilyGauge<ConcurrentLimitLabels>> =
    LazyLock::new(|| register_gauge_family("storage_concurrent_limit_queued_operations"));

struct QueuedOperationGuard {
    labels: Arc<ConcurrentLimitLabels>,
}

impl QueuedOperationGuard {
    fn new(labels: Arc<ConcurrentLimitLabels>) -> Self {
        CONCURRENT_LIMIT_QUEUED_OPERATIONS
            .get_or_create(labels.as_ref())
            .inc();
        Self { labels }
    }
}

impl Drop for QueuedOperationGuard {
    fn drop(&mut self) {
        CONCURRENT_LIMIT_QUEUED_OPERATIONS
            .get_or_create(self.labels.as_ref())
            .dec();
    }
}

async fn acquire_operation_permit<'a>(
    semaphore: &'a Semaphore,
    labels: &Arc<ConcurrentLimitLabels>,
) -> SemaphorePermit<'a> {
    match semaphore.try_acquire() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            let _queued = QueuedOperationGuard::new(labels.clone());
            semaphore.acquire().await.expect("semaphore must be valid")
        }
        Err(TryAcquireError::Closed) => semaphore.acquire().await.expect("semaphore must be valid"),
    }
}

/// Add concurrent request limit.
///
/// # Notes
///
/// Users can control how many concurrent connections could be established
/// between OpenDAL and underlying storage services.
///
/// All operators wrapped by this layer will share a common semaphore. This
/// allows you to reuse the same layer across multiple operators, ensuring
/// that the total number of concurrent requests across the entire
/// application does not exceed the limit.
///
/// Databend limits each operation instead of each IO handle. Creating a
/// reader, writer, lister or deleter takes a permit only for that creation
/// call. The returned handle stores the shared semaphore and reacquires a
/// permit for each actual async IO operation, such as read, write, close, list
/// next or delete flush. This prevents long-lived idle handles from consuming
/// concurrency permits.
///
/// # Examples
///
/// Add a concurrent limit layer to the operator:
///
/// ```no_run
/// # use databend_common_storage::ConcurrentLimitLayer;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
/// # use opendal::services;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(ConcurrentLimitLayer::new(1024))
///     .finish();
/// Ok(())
/// # }
/// ```
///
/// Share a concurrent limit layer between the operators:
///
/// ```no_run
/// # use databend_common_storage::ConcurrentLimitLayer;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
/// # use opendal::services;
///
/// # fn main() -> Result<()> {
/// let limit = ConcurrentLimitLayer::new(1024);
///
/// let _operator_a = Operator::new(services::Memory::default())?
///     .layer(limit.clone())
///     .finish();
/// let _operator_b = Operator::new(services::Memory::default())?
///     .layer(limit.clone())
///     .finish();
///
/// Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ConcurrentLimitLayer {
    operation_semaphore: Arc<Semaphore>,
}

impl ConcurrentLimitLayer {
    /// Create a new ConcurrentLimitLayer will specify permits.
    ///
    /// This permits will applied to all operations.
    pub fn new(permits: usize) -> Self {
        Self {
            operation_semaphore: Arc::new(Semaphore::new(permits)),
        }
    }
}

impl<A: Access> Layer<A> for ConcurrentLimitLayer {
    type LayeredAccess = ConcurrentLimitAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        ConcurrentLimitAccessor {
            inner,
            semaphore: self.operation_semaphore.clone(),
            labels: Arc::new(ConcurrentLimitLabels {
                scheme: info.scheme(),
                namespace: info.name(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentLimitAccessor<A: Access> {
    inner: A,
    semaphore: Arc<Semaphore>,
    labels: Arc<ConcurrentLimitLabels>,
}

impl<A: Access> LayeredAccess for ConcurrentLimitAccessor<A> {
    type Inner = A;
    type Reader = ConcurrentLimitWrapper<A::Reader>;
    type Writer = ConcurrentLimitWrapper<A::Writer>;
    type Lister = ConcurrentLimitWrapper<A::Lister>;
    type Deleter = ConcurrentLimitWrapper<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.read(path, args).await.map(|(rp, r)| {
            (
                rp,
                ConcurrentLimitWrapper::new(r, self.semaphore.clone(), self.labels.clone()),
            )
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.write(path, args).await.map(|(rp, w)| {
            (
                rp,
                ConcurrentLimitWrapper::new(w, self.semaphore.clone(), self.labels.clone()),
            )
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.delete().await.map(|(rp, w)| {
            (
                rp,
                ConcurrentLimitWrapper::new(w, self.semaphore.clone(), self.labels.clone()),
            )
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.list(path, args).await.map(|(rp, s)| {
            (
                rp,
                ConcurrentLimitWrapper::new(s, self.semaphore.clone(), self.labels.clone()),
            )
        })
    }
}

pub struct ConcurrentLimitWrapper<R> {
    inner: R,
    // Keep the shared semaphore, not an acquired permit. Readers and writers
    // can live across scheduler rounds, so holding a permit here would make an
    // idle handle consume the global IO concurrency limit.
    semaphore: Arc<Semaphore>,
    labels: Arc<ConcurrentLimitLabels>,
}

impl<R> ConcurrentLimitWrapper<R> {
    fn new(inner: R, semaphore: Arc<Semaphore>, labels: Arc<ConcurrentLimitLabels>) -> Self {
        Self {
            inner,
            semaphore,
            labels,
        }
    }
}

impl<R: oio::Read> oio::Read for ConcurrentLimitWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.read().await
    }
}

impl<R: oio::Write> oio::Write for ConcurrentLimitWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.abort().await
    }
}

impl<R: oio::List> oio::List for ConcurrentLimitWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.next().await
    }
}

impl<R: oio::Delete> oio::Delete for ConcurrentLimitWrapper<R> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        let _permit = acquire_operation_permit(self.semaphore.as_ref(), &self.labels).await;

        self.inner.flush().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use opendal::Buffer;
    use opendal::Operator;
    use opendal::Result;
    use opendal::raw::oio;
    use opendal::raw::oio::Read;
    use opendal::services;
    use tokio::sync::Semaphore;
    use tokio::sync::oneshot;

    use super::CONCURRENT_LIMIT_QUEUED_OPERATIONS;
    use super::ConcurrentLimitLabels;
    use super::ConcurrentLimitLayer;
    use super::ConcurrentLimitWrapper;

    #[tokio::test]
    async fn test_io_handles_do_not_hold_operation_permits() -> Result<()> {
        let layer = ConcurrentLimitLayer::new(1);
        let op = Operator::new(services::Memory::default())?
            .layer(layer.clone())
            .finish();

        let mut writer = op.writer("path").await?;
        assert_eq!(layer.operation_semaphore.available_permits(), 1);

        writer.write(Buffer::from(vec![1, 2, 3])).await?;
        assert_eq!(layer.operation_semaphore.available_permits(), 1);

        writer.close().await?;
        assert_eq!(layer.operation_semaphore.available_permits(), 1);

        let reader = op.reader("path").await?;
        assert_eq!(layer.operation_semaphore.available_permits(), 1);

        let _ = reader.read(0..3).await?;
        assert_eq!(layer.operation_semaphore.available_permits(), 1);

        Ok(())
    }

    struct BlockingReader {
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
    }

    impl oio::Read for BlockingReader {
        async fn read(&mut self) -> Result<Buffer> {
            self.entered
                .take()
                .expect("entered sender must exist")
                .send(())
                .expect("entered receiver must exist");
            self.release
                .take()
                .expect("release receiver must exist")
                .await
                .expect("release sender must exist");

            Ok(Buffer::new())
        }
    }

    #[tokio::test]
    async fn test_io_operation_holds_permit_until_finished() -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(1));
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();

        let mut reader = ConcurrentLimitWrapper::new(
            BlockingReader {
                entered: Some(entered_tx),
                release: Some(release_rx),
            },
            semaphore.clone(),
            test_labels(),
        );

        let handle = databend_common_base::runtime::spawn(async move { reader.read().await });
        entered_rx.await.expect("read operation must start");

        assert_eq!(semaphore.available_permits(), 0);

        release_tx
            .send(())
            .expect("read task must wait for release");
        handle.await.expect("read task must not panic")?;

        assert_eq!(semaphore.available_permits(), 1);

        Ok(())
    }

    fn test_labels() -> Arc<ConcurrentLimitLabels> {
        Arc::new(ConcurrentLimitLabels {
            scheme: "test",
            namespace: Arc::from("test"),
        })
    }

    fn queued_operations(labels: &ConcurrentLimitLabels) -> i64 {
        CONCURRENT_LIMIT_QUEUED_OPERATIONS
            .get(labels)
            .map(|v| v.get())
            .unwrap_or_default()
    }

    async fn wait_for_queued_operations(labels: &ConcurrentLimitLabels, expected: i64) {
        for _ in 0..100 {
            if queued_operations(labels) == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(queued_operations(labels), expected);
    }

    #[tokio::test]
    async fn test_queued_operation_gauge_tracks_waiting_permits() -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(1));
        let labels = test_labels();
        let baseline = queued_operations(labels.as_ref());
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();

        let mut first_reader = ConcurrentLimitWrapper::new(
            BlockingReader {
                entered: Some(entered_tx),
                release: Some(release_rx),
            },
            semaphore.clone(),
            labels.clone(),
        );

        let first_handle =
            databend_common_base::runtime::spawn(async move { first_reader.read().await });
        entered_rx.await.expect("first read operation must start");

        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx2, release_rx) = oneshot::channel();
        let mut second_reader = ConcurrentLimitWrapper::new(
            BlockingReader {
                entered: Some(entered_tx),
                release: Some(release_rx),
            },
            semaphore.clone(),
            labels.clone(),
        );

        let second_handle =
            databend_common_base::runtime::spawn(async move { second_reader.read().await });
        wait_for_queued_operations(labels.as_ref(), baseline + 1).await;

        release_tx
            .send(())
            .expect("first read task must wait for release");
        first_handle
            .await
            .expect("first read task must not panic")?;

        entered_rx.await.expect("second read operation must start");
        wait_for_queued_operations(labels.as_ref(), baseline).await;

        release_tx2
            .send(())
            .expect("second read task must wait for release");
        second_handle
            .await
            .expect("second read task must not panic")?;

        assert_eq!(queued_operations(labels.as_ref()), baseline);

        Ok(())
    }
}
