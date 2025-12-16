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

use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use opendal::Buffer;
use opendal::Metadata;
use opendal::Result;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal::raw::LayeredAccess;
use opendal::raw::OpCreateDir;
use opendal::raw::OpDelete;
use opendal::raw::OpList;
use opendal::raw::OpPresign;
use opendal::raw::OpRead;
use opendal::raw::OpStat;
use opendal::raw::OpWrite;
use opendal::raw::RpCreateDir;
use opendal::raw::RpDelete;
use opendal::raw::RpList;
use opendal::raw::RpPresign;
use opendal::raw::RpRead;
use opendal::raw::RpStat;
use opendal::raw::RpWrite;
use opendal::raw::oio;

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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.runtime.inner())
    }
}

impl<A: Access> LayeredAccess for RuntimeAccessor<A> {
    type Inner = A;
    type Reader = RuntimeIO<A::Reader>;
    type Writer = RuntimeIO<A::Writer>;
    type Lister = RuntimeIO<A::Lister>;
    type Deleter = RuntimeIO<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.create_dir(&path, args).await })
            .await
            .expect("join must success")
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let op = self.inner.clone();
        let path = path.to_string();

        self.runtime
            .spawn(async move { op.read(&path, args).await })
            .await
            .expect("join must success")
            .map(|(rp, r)| {
                let r = RuntimeIO::new(r, self.runtime.clone());
                (rp, r)
            })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.write(&path, args).await })
            .await
            .expect("join must success")
            .map(|(rp, r)| {
                let r = RuntimeIO::new(r, self.runtime.clone());
                (rp, r)
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.stat(&path, args).await })
            .await
            .expect("join must success")
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let op = self.inner.clone();

        self.runtime
            .spawn(async move { op.delete().await })
            .await
            .expect("join must success")
            .map(|(rp, r)| {
                let r = RuntimeIO::new(r, self.runtime.clone());
                (rp, r)
            })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.list(&path, args).await })
            .await
            .expect("join must success")
            .map(|(rp, r)| {
                let r = RuntimeIO::new(r, self.runtime.clone());
                (rp, r)
            })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.presign(&path, args).await })
            .await
            .expect("join must success")
    }
}

pub struct RuntimeIO<R: 'static> {
    inner: Option<R>,
    runtime: Arc<Runtime>,
}

impl<R> RuntimeIO<R> {
    fn new(inner: R, runtime: Arc<Runtime>) -> Self {
        Self {
            inner: Some(inner),
            runtime,
        }
    }
}

impl<R: oio::Read> oio::Read for RuntimeIO<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let mut r = self.inner.take().expect("reader must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn(async move {
                let res = r.read().await;
                (r, res)
            })
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }
}

impl<R: oio::Write> oio::Write for RuntimeIO<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let mut r = self.inner.take().expect("writer must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn(async move {
                let res = r.write(bs).await;
                (r, res)
            })
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }

    async fn close(&mut self) -> Result<Metadata> {
        let mut r = self.inner.take().expect("writer must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn(async move {
                let res = r.close().await;
                (r, res)
            })
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }

    async fn abort(&mut self) -> Result<()> {
        let mut r = self.inner.take().expect("writer must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn(async move {
                let res = r.abort().await;
                (r, res)
            })
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }
}

impl<R: oio::List> oio::List for RuntimeIO<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let mut r = self.inner.take().expect("lister must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn(async move {
                let res = r.next().await;
                (r, res)
            })
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }
}

impl<R: oio::Delete> oio::Delete for RuntimeIO<R> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.as_mut().unwrap().delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        let mut r = self.inner.take().expect("deleter must be valid");
        let runtime = self.runtime.clone();

        let _ = runtime
            .spawn(async move { r.close().await })
            .await
            .expect("join must success")?;
        Ok(())
    }
}
