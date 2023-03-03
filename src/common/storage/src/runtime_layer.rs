// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use async_trait::async_trait;
use common_base::base::tokio::runtime::Handle;
use common_base::runtime::TrackedFuture;
use opendal::ops::*;
use opendal::raw::Accessor;
use opendal::raw::Layer;
use opendal::raw::LayeredAccessor;
use opendal::raw::RpCreate;
use opendal::raw::RpDelete;
use opendal::raw::RpList;
use opendal::raw::RpRead;
use opendal::raw::RpScan;
use opendal::raw::RpStat;
use opendal::raw::RpWrite;
use opendal::Result;

/// # TODO
///
/// DalRuntime is used to make sure all IO task are running in the same runtime.
/// So that we will not bothered by `dispatch dropped` panic.
///
/// However, the new processor framework will make sure that all async task running
/// in the same, global, separate, IO only async runtime, so we can remove `RuntimeLayer`
/// after new processor framework finished.
#[derive(Clone, Debug)]
pub struct RuntimeLayer {
    runtime: Handle,
}

impl RuntimeLayer {
    pub fn new(runtime: Handle) -> Self {
        RuntimeLayer { runtime }
    }
}

impl<A: Accessor> Layer<A> for RuntimeLayer {
    type LayeredAccessor = RuntimeAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        RuntimeAccessor {
            inner: Arc::new(inner),
            runtime: self.runtime.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeAccessor<A> {
    inner: Arc<A>,
    runtime: Handle,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for RuntimeAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.create(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.read(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.write(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.stat(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.delete(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.list(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.scan(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.inner.blocking_scan(path, args)
    }
}
