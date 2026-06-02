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

use std::fmt::Debug;
use std::sync::Arc;

use opendal::Buffer;
use opendal::Metadata;
use opendal::Result;
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
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

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
        ConcurrentLimitAccessor {
            inner,
            semaphore: self.operation_semaphore.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentLimitAccessor<A: Access> {
    inner: A,
    semaphore: Arc<Semaphore>,
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
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ConcurrentLimitWrapper::new(r, permit)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .delete()
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .list(path, args)
            .await
            .map(|(rp, s)| (rp, ConcurrentLimitWrapper::new(s, permit)))
    }
}

pub struct ConcurrentLimitWrapper<R> {
    inner: R,

    // Hold on this permit until this reader has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl<R> ConcurrentLimitWrapper<R> {
    fn new(inner: R, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl<R: oio::Read> oio::Read for ConcurrentLimitWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
    }
}

impl<R: oio::Write> oio::Write for ConcurrentLimitWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

impl<R: oio::List> oio::List for ConcurrentLimitWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next().await
    }
}

impl<R: oio::Delete> oio::Delete for ConcurrentLimitWrapper<R> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        self.inner.flush().await
    }
}
