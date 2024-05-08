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
use std::io::SeekFrom;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::GLOBAL_TASK;
use futures::ready;
use futures::Future;
use futures::TryFutureExt;
use opendal::raw::oio;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal::raw::LayeredAccess;
use opendal::raw::MaybeSend;
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
use opendal::Buffer;
use opendal::Result;

/// # TODO
///
/// DalRuntime is used to make sure all IO task are running in the same runtime.
/// So that we will not bothered by `dispatch dropped` panic.
///
/// However, the new processor framework will make sure that all async task running
/// in the same, global, separate, IO only async runtime, so we can remove `RuntimeLayer`
/// after new processor framework finished.
#[derive(Clone)]
pub struct RuntimeLayer {
    runtime: Arc<Runtime>,
}

impl Debug for RuntimeLayer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.runtime.inner())
    }
}

impl RuntimeLayer {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        RuntimeLayer { runtime }
    }
}

impl<A: Access> Layer<A> for RuntimeLayer {
    type LayeredAccess = RuntimeAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        RuntimeAccessor {
            inner: Arc::new(inner),
            runtime: self.runtime.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RuntimeAccessor<A> {
    inner: Arc<A>,
    runtime: Arc<Runtime>,
}

impl<A> Debug for RuntimeAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.runtime.inner())
    }
}

impl<A: Access> LayeredAccess for RuntimeAccessor<A> {
    type Inner = A;
    type Reader = RuntimeIO<A::Reader>;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[async_backtrace::framed]
    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(GLOBAL_TASK, async move { op.create_dir(&path, args).await })
            .await
            .expect("join must success")
    }

    #[async_backtrace::framed]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let op = self.inner.clone();
        let path = path.to_string();

        self.runtime
            .spawn(GLOBAL_TASK, async move { op.read(&path, args).await })
            .await
            .expect("join must success")
            .map(|(rp, r)| {
                let r = RuntimeIO::new(r, self.runtime.clone());
                (rp, r)
            })
    }

    #[async_backtrace::framed]
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(GLOBAL_TASK, async move { op.write(&path, args).await })
            .await
            .expect("join must success")
    }

    #[async_backtrace::framed]
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(GLOBAL_TASK, async move { op.stat(&path, args).await })
            .await
            .expect("join must success")
    }

    #[async_backtrace::framed]
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(GLOBAL_TASK, async move { op.delete(&path, args).await })
            .await
            .expect("join must success")
    }

    #[async_backtrace::framed]
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(GLOBAL_TASK, async move { op.list(&path, args).await })
            .await
            .expect("join must success")
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }
}

pub struct RuntimeIO<R: 'static> {
    inner: Arc<R>,
    runtime: Arc<Runtime>,
}

impl<R> RuntimeIO<R> {
    fn new(inner: R, runtime: Arc<Runtime>) -> Self {
        Self {
            inner: Arc::new(inner),
            runtime,
        }
    }
}

impl<R: oio::Read> oio::Read for RuntimeIO<R> {
    fn read_at(
        &self,
        offset: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Buffer>> + MaybeSend {
        let r = self.inner.clone();

        let runtime = self.runtime.clone();
        async move {
            runtime
                .spawn(GLOBAL_TASK, async move { r.read_at(offset, limit).await })
                .await
                .expect("join must success")
        }
    }
}
