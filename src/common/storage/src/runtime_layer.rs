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
        if self.runtime.is_current() {
            return self.inner.create_dir(path, args).await;
        }

        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.create_dir(&path, args).await })
            .await
            .expect("join must success")
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let result = if self.runtime.is_current() {
            self.inner.read(path, args).await
        } else {
            let op = self.inner.clone();
            let path = path.to_string();
            self.runtime
                .spawn(async move { op.read(&path, args).await })
                .await
                .expect("join must success")
        };

        result.map(|(rp, r)| {
            let r = RuntimeIO::new(r, self.runtime.clone());
            (rp, r)
        })
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let result = if self.runtime.is_current() {
            self.inner.write(path, args).await
        } else {
            let op = self.inner.clone();
            let path = path.to_string();
            self.runtime
                .spawn(async move { op.write(&path, args).await })
                .await
                .expect("join must success")
        };

        result.map(|(rp, r)| {
            let r = RuntimeIO::new(r, self.runtime.clone());
            (rp, r)
        })
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        if self.runtime.is_current() {
            return self.inner.stat(path, args).await;
        }

        let op = self.inner.clone();
        let path = path.to_string();
        self.runtime
            .spawn(async move { op.stat(&path, args).await })
            .await
            .expect("join must success")
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let result = if self.runtime.is_current() {
            self.inner.delete().await
        } else {
            let op = self.inner.clone();
            self.runtime
                .spawn(async move { op.delete().await })
                .await
                .expect("join must success")
        };

        result.map(|(rp, r)| {
            let r = RuntimeIO::new(r, self.runtime.clone());
            (rp, r)
        })
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let result = if self.runtime.is_current() {
            self.inner.list(path, args).await
        } else {
            let op = self.inner.clone();
            let path = path.to_string();
            self.runtime
                .spawn(async move { op.list(&path, args).await })
                .await
                .expect("join must success")
        };

        result.map(|(rp, r)| {
            let r = RuntimeIO::new(r, self.runtime.clone());
            (rp, r)
        })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        if self.runtime.is_current() {
            return self.inner.presign(path, args).await;
        }

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

impl<R: oio::Read> oio::Read for RuntimeIO<R> {
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
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.as_mut().unwrap().delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        if self.runtime.is_current() {
            return self
                .inner
                .as_mut()
                .expect("deleter must be valid")
                .flush()
                .await;
        }

        let mut r = self.inner.take().expect("deleter must be valid");
        let runtime = self.runtime.clone();

        let (r, res) = runtime
            .spawn_named(
                async move {
                    let res = r.flush().await;
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use opendal::raw::AccessorInfo;

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ExecutionContext {
        runtime_id: tokio::runtime::Id,
        task_id: tokio::task::Id,
    }

    impl ExecutionContext {
        fn current() -> Self {
            Self {
                runtime_id: tokio::runtime::Handle::current().id(),
                task_id: tokio::task::id(),
            }
        }
    }

    #[derive(Debug)]
    struct ProbeAccess {
        contexts: Arc<Mutex<Vec<ExecutionContext>>>,
    }

    impl Access for ProbeAccess {
        type Reader = ProbeReader;
        type Writer = ();
        type Lister = ();
        type Deleter = ();

        fn info(&self) -> Arc<AccessorInfo> {
            Arc::new(AccessorInfo::default())
        }

        async fn read(&self, _path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
            self.contexts
                .lock()
                .expect("contexts lock must be valid")
                .push(ExecutionContext::current());

            Ok((RpRead::new(), ProbeReader {
                contexts: self.contexts.clone(),
            }))
        }
    }

    #[derive(Debug)]
    struct ProbeReader {
        contexts: Arc<Mutex<Vec<ExecutionContext>>>,
    }

    impl oio::Read for ProbeReader {
        async fn read(&mut self) -> Result<Buffer> {
            self.contexts
                .lock()
                .expect("contexts lock must be valid")
                .push(ExecutionContext::current());
            Ok(Buffer::new())
        }
    }

    fn probe_accessor(
        runtime: Arc<Runtime>,
        contexts: Arc<Mutex<Vec<ExecutionContext>>>,
    ) -> RuntimeAccessor<ProbeAccess> {
        RuntimeLayer::new(runtime).layer(ProbeAccess { contexts })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bypass_spawn_in_target_runtime() -> anyhow::Result<()> {
        let runtime = Arc::new(Runtime::with_worker_threads(
            1,
            Some("runtime-layer-target".to_string()),
        )?);
        let contexts = Arc::new(Mutex::new(Vec::new()));
        let accessor = probe_accessor(runtime.clone(), contexts.clone());

        let caller_context = runtime
            .spawn(async move {
                assert!(accessor.runtime.is_current());
                let caller_context = ExecutionContext::current();
                let (_, mut reader) = Access::read(&accessor, "file", OpRead::new()).await?;
                oio::Read::read(&mut reader).await?;
                Ok::<_, opendal::Error>(caller_context)
            })
            .await??;

        let contexts = contexts.lock().expect("contexts lock must be valid");
        assert_eq!(contexts.as_slice(), &[caller_context, caller_context]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_spawn_into_target_runtime_from_other_runtime() -> anyhow::Result<()> {
        let runtime = Arc::new(Runtime::with_worker_threads(
            1,
            Some("runtime-layer-target".to_string()),
        )?);
        let target_runtime_id = runtime.inner().id();
        let contexts = Arc::new(Mutex::new(Vec::new()));
        let accessor = probe_accessor(runtime.clone(), contexts.clone());

        assert!(!runtime.is_current());
        let caller_context = databend_common_base::runtime::spawn(async move {
            let caller_context = ExecutionContext::current();
            let (_, mut reader) = Access::read(&accessor, "file", OpRead::new()).await?;
            oio::Read::read(&mut reader).await?;
            Ok::<_, opendal::Error>(caller_context)
        })
        .await??;

        let contexts = contexts.lock().expect("contexts lock must be valid");
        assert_eq!(contexts.len(), 2);
        assert!(
            contexts
                .iter()
                .all(|context| context.runtime_id == target_runtime_id)
        );
        assert!(
            contexts
                .iter()
                .all(|context| context.task_id != caller_context.task_id)
        );
        Ok(())
    }
}
