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
use databend_common_base::runtime::ThreadTracker;
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

impl Layer for RuntimeLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(RuntimeService {
            inner,
            runtime: self.runtime.clone(),
        })
    }
}

#[derive(Clone)]
struct RuntimeService {
    inner: Servicer,
    runtime: Arc<Runtime>,
}

impl Debug for RuntimeService {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.runtime.inner())
    }
}

impl Service for RuntimeService {
    type Reader = RuntimeReader;
    type Writer = RuntimeIO<oio::Writer>;
    type Lister = RuntimeIO<oio::Lister>;
    type Deleter = RuntimeIO<oio::Deleter>;
    type Copier = RuntimeIO<oio::Copier>;

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
        if self.runtime.is_current() {
            return self.inner.create_dir(ctx, path, args).await;
        }

        let op = self.inner.clone();
        let ctx = ctx.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.create_dir(&ctx, &path, args).await })
            .await
            .expect("join must success")
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        // Reader construction is sync; wrap IO bodies so stream reads run on the target runtime.
        self.inner
            .read(ctx, path, args)
            .map(|r| RuntimeReader::new(r, self.runtime.clone()))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner
            .write(ctx, path, args)
            .map(|w| RuntimeIO::new(w, self.runtime.clone()))
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
            .map(|c| RuntimeIO::new(c, self.runtime.clone()))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        if self.runtime.is_current() {
            return self.inner.rename(ctx, from, to, args).await;
        }

        let op = self.inner.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();
        self.runtime
            .spawn(async move { op.rename(&ctx, &from, &to, args).await })
            .await
            .expect("join must success")
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        if self.runtime.is_current() {
            return self.inner.stat(ctx, path, args).await;
        }

        let op = self.inner.clone();
        let ctx = ctx.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.stat(&ctx, &path, args).await })
            .await
            .expect("join must success")
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner
            .delete(ctx)
            .map(|d| RuntimeIO::new(d, self.runtime.clone()))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner
            .list(ctx, path, args)
            .map(|s| RuntimeIO::new(s, self.runtime.clone()))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        if self.runtime.is_current() {
            return self.inner.presign(ctx, path, args).await;
        }

        let op = self.inner.clone();
        let ctx = ctx.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.presign(&ctx, &path, args).await })
            .await
            .expect("join must success")
    }
}

struct RuntimeReader {
    inner: Arc<oio::Reader>,
    runtime: Arc<Runtime>,
}

impl RuntimeReader {
    fn new(inner: oio::Reader, runtime: Arc<Runtime>) -> Self {
        Self {
            inner: Arc::new(inner),
            runtime,
        }
    }
}

impl oio::Read for RuntimeReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (rp, stream) = if self.runtime.is_current() {
            (*self.inner).open(range).await?
        } else {
            let op = self.inner.clone();
            let runtime = self.runtime.clone();
            runtime
                .spawn(async move { (*op).open(range).await })
                .await
                .expect("join must success")?
        };

        Ok((
            rp,
            Box::new(RuntimeIO::new(stream, self.runtime.clone())) as Box<dyn oio::ReadStreamDyn>,
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        if self.runtime.is_current() {
            return (*self.inner).read(range).await;
        }

        let op = self.inner.clone();
        let runtime = self.runtime.clone();
        runtime
            .spawn(async move { (*op).read(range).await })
            .await
            .expect("join must success")
    }
}

pub struct RuntimeIO<R: 'static> {
    inner: Option<R>,
    runtime: Arc<Runtime>,
    spawn_task_name: String,
}

impl<R> RuntimeIO<R> {
    fn new(inner: R, runtime: Arc<Runtime>) -> Self {
        // pre-assemble spawn task name, to avoid calling format! in heavy read loop
        let query_id = ThreadTracker::query_id();
        let spawn_task_name = if let Some(id) = query_id {
            format!("Running query {} IO task", id)
        } else {
            String::from("Running IO task")
        };

        Self {
            inner: Some(inner),
            runtime,
            spawn_task_name,
        }
    }
}

impl<R: oio::ReadStream> oio::ReadStream for RuntimeIO<R> {
    async fn read(&mut self) -> Result<Buffer> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("reader must be valid")
                .read()
                .await;
        }

        let mut r = self.inner.take().expect("reader must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.read().await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }
}

impl<R: oio::Write> oio::Write for RuntimeIO<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("writer must be valid")
                .write(bs)
                .await;
        }

        let mut r = self.inner.take().expect("writer must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.write(bs).await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }

    async fn close(&mut self) -> Result<Metadata> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("writer must be valid")
                .close()
                .await;
        }

        let mut r = self.inner.take().expect("writer must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.close().await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }

    async fn abort(&mut self) -> Result<()> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("writer must be valid")
                .abort()
                .await;
        }

        let mut r = self.inner.take().expect("writer must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.abort().await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }
}

impl<R: oio::List> oio::List for RuntimeIO<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("lister must be valid")
                .next()
                .await;
        }

        let mut r = self.inner.take().expect("lister must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.next().await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }
}

impl<R: oio::Delete> oio::Delete for RuntimeIO<R> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("deleter must be valid")
                .delete(path, args)
                .await;
        }

        let mut r = self.inner.take().expect("deleter must be valid");
        let runtime = self.runtime.clone();
        let path = path.to_string();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.delete(&path, args).await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }

    async fn close(&mut self) -> Result<()> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("deleter must be valid")
                .close()
                .await;
        }

        let mut r = self.inner.take().expect("deleter must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.close().await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }
}

impl<R: oio::Copy> oio::Copy for RuntimeIO<R> {
    async fn next(&mut self) -> Result<Option<usize>> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("copier must be valid")
                .next()
                .await;
        }

        let mut r = self.inner.take().expect("copier must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.next().await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }

    async fn close(&mut self) -> Result<Metadata> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("copier must be valid")
                .close()
                .await;
        }

        let mut r = self.inner.take().expect("copier must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.close().await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }

    async fn abort(&mut self) -> Result<()> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("copier must be valid")
                .abort()
                .await;
        }

        let mut r = self.inner.take().expect("copier must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.abort().await;
                    (r, res)
                },
                self.spawn_task_name.clone(),
            )
            .await
            .expect("join must success");
        self.inner = Some(r);
        res
    }
}
