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

// Ported and adapted from Apache OpenDAL 0.58 ConcurrentLimitLayer for
// Databend queue metrics (storage_concurrent_limit_queued_operations).

use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_base::runtime::metrics::FamilyGauge;
use databend_common_base::runtime::metrics::register_gauge_family;
use opendal::Buffer;
use opendal::BytesRange;
use opendal::Capability;
use opendal::Metadata;
use opendal::OperationContext;
use opendal::Result;
use opendal::raw::Layer;
use opendal::raw::OpCopier;
use opendal::raw::OpCopy;
use opendal::raw::OpCreateDir;
use opendal::raw::OpDelete;
use opendal::raw::OpList;
use opendal::raw::OpPresign;
use opendal::raw::OpRead;
use opendal::raw::OpRename;
use opendal::raw::OpStat;
use opendal::raw::OpWrite;
use opendal::raw::RpCreateDir;
use opendal::raw::RpPresign;
use opendal::raw::RpRead;
use opendal::raw::RpRename;
use opendal::raw::RpStat;
use opendal::raw::Service;
use opendal::raw::ServiceInfo;
use opendal::raw::Servicer;
use opendal::raw::oio;
use prometheus_client::encoding::EncodeLabel;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::LabelSetEncoder;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::TryAcquireError;

const LABEL_SCHEME: &str = "scheme";
const LABEL_NAMESPACE: &str = "namespace";

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ConcurrentLimitLabels {
    scheme: &'static str,
    namespace: Arc<str>,
}

impl EncodeLabelSet for ConcurrentLimitLabels {
    fn encode(&self, mut encoder: LabelSetEncoder) -> Result<(), fmt::Error> {
        (LABEL_SCHEME, self.scheme).encode(encoder.encode_label())?;
        (LABEL_NAMESPACE, self.namespace.as_ref()).encode(encoder.encode_label())?;
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

async fn acquire_operation_permit(
    semaphore: &Arc<Semaphore>,
    labels: &Arc<ConcurrentLimitLabels>,
) -> OwnedSemaphorePermit {
    match semaphore.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            let _queued = QueuedOperationGuard::new(labels.clone());
            semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore must be valid")
        }
        Err(TryAcquireError::Closed) => semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid"),
    }
}

/// Add concurrent request limit with Databend queue metrics.
///
/// Operators that reuse the same layer instance share one semaphore so the
/// process-wide concurrent-request budget stays consistent.
#[derive(Clone)]
pub struct ConcurrentLimitLayer {
    operation_semaphore: Arc<Semaphore>,
}

impl Debug for ConcurrentLimitLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcurrentLimitLayer")
            .field(
                "available_permits",
                &self.operation_semaphore.available_permits(),
            )
            .finish_non_exhaustive()
    }
}

impl ConcurrentLimitLayer {
    /// Create a new ConcurrentLimitLayer with the given permits.
    pub fn new(permits: usize) -> Self {
        Self {
            operation_semaphore: Arc::new(Semaphore::new(permits)),
        }
    }
}

impl Layer for ConcurrentLimitLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        let info = inner.info();
        Arc::new(ConcurrentLimitService {
            inner,
            semaphore: self.operation_semaphore.clone(),
            labels: Arc::new(ConcurrentLimitLabels {
                scheme: info.scheme(),
                namespace: info.name(),
            }),
        })
    }
}

#[derive(Clone)]
struct ConcurrentLimitService {
    inner: Servicer,
    semaphore: Arc<Semaphore>,
    labels: Arc<ConcurrentLimitLabels>,
}

impl Debug for ConcurrentLimitService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcurrentLimitService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Service for ConcurrentLimitService {
    type Reader = ConcurrentLimitReader;
    type Writer = ConcurrentLimitWrapper<oio::Writer>;
    type Lister = ConcurrentLimitWrapper<oio::Lister>;
    type Deleter = ConcurrentLimitWrapper<oio::Deleter>;
    type Copier = ConcurrentLimitWrapper<oio::Copier>;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.create_dir(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner
            .read(ctx, path, args)
            .map(|r| ConcurrentLimitReader::new(r, self.semaphore.clone(), self.labels.clone()))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner
            .write(ctx, path, args)
            .map(|w| ConcurrentLimitWrapper::new(w, self.semaphore.clone(), self.labels.clone()))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        self.inner
            .copy(ctx, from, to, args, opts)
            .map(|c| ConcurrentLimitWrapper::new(c, self.semaphore.clone(), self.labels.clone()))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.rename(ctx, from, to, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.stat(ctx, path, args).await
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner
            .delete(ctx)
            .map(|d| ConcurrentLimitWrapper::new(d, self.semaphore.clone(), self.labels.clone()))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner
            .list(ctx, path, args)
            .map(|s| ConcurrentLimitWrapper::new(s, self.semaphore.clone(), self.labels.clone()))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.presign(ctx, path, args).await
    }
}

struct ConcurrentLimitReader {
    inner: oio::Reader,
    semaphore: Arc<Semaphore>,
    labels: Arc<ConcurrentLimitLabels>,
}

impl ConcurrentLimitReader {
    fn new(
        inner: oio::Reader,
        semaphore: Arc<Semaphore>,
        labels: Arc<ConcurrentLimitLabels>,
    ) -> Self {
        Self {
            inner,
            semaphore,
            labels,
        }
    }
}

impl oio::Read for ConcurrentLimitReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        let (rp, stream) = self.inner.open(range).await?;
        Ok((
            rp,
            Box::new(ConcurrentLimitWrapper::new_with_permit(
                stream,
                self.semaphore.clone(),
                self.labels.clone(),
                permit,
            )) as Box<dyn oio::ReadStreamDyn>,
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.read(range).await
    }
}

struct ConcurrentLimitWrapper<R> {
    inner: R,
    semaphore: Arc<Semaphore>,
    labels: Arc<ConcurrentLimitLabels>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<R> ConcurrentLimitWrapper<R> {
    fn new(inner: R, semaphore: Arc<Semaphore>, labels: Arc<ConcurrentLimitLabels>) -> Self {
        Self {
            inner,
            semaphore,
            labels,
            permit: None,
        }
    }

    fn new_with_permit(
        inner: R,
        semaphore: Arc<Semaphore>,
        labels: Arc<ConcurrentLimitLabels>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            inner,
            semaphore,
            labels,
            permit: Some(permit),
        }
    }

    async fn acquire(&mut self) {
        if self.permit.is_none() {
            self.permit = Some(acquire_operation_permit(&self.semaphore, &self.labels).await);
        }
    }
}

impl<R: oio::ReadStream> oio::ReadStream for ConcurrentLimitWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        self.acquire().await;
        self.inner.read().await
    }
}

impl<R: oio::Write> oio::Write for ConcurrentLimitWrapper<R> {
    // Acquire per call so long-lived writers do not pin the global IO budget
    // between write/close/abort invocations (see test_io_handles_do_not_hold_*).
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.abort().await
    }
}

impl<R: oio::List> oio::List for ConcurrentLimitWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.next().await
    }
}

impl<R: oio::Delete> oio::Delete for ConcurrentLimitWrapper<R> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.close().await
    }
}

impl<R: oio::Copy> oio::Copy for ConcurrentLimitWrapper<R> {
    async fn next(&mut self) -> Result<Option<usize>> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.next().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _permit = acquire_operation_permit(&self.semaphore, &self.labels).await;
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use opendal::Buffer;
    use opendal::Operator;
    use opendal::Result;
    use opendal::services;

    use super::ConcurrentLimitLayer;

    #[tokio::test]
    async fn test_io_handles_do_not_hold_operation_permits() -> Result<()> {
        let layer = ConcurrentLimitLayer::new(1);
        let op = Operator::new(services::Memory::default())?.layer(layer.clone());

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
}
