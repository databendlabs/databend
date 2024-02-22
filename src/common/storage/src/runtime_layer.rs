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
use databend_common_base::runtime::TrackedFuture;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::GLOBAL_TASK;
use futures::ready;
use futures::Future;
use opendal::raw::oio;
use opendal::raw::oio::ReadExt;
use opendal::raw::Accessor;
use opendal::raw::Layer;
use opendal::raw::LayeredAccessor;
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

impl<A: Accessor> Layer<A> for RuntimeLayer {
    type LayeredAccessor = RuntimeAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
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

#[async_trait]
impl<A: Accessor> LayeredAccessor for RuntimeAccessor<A> {
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
        let future = async move { op.create_dir(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime
            .spawn(GLOBAL_TASK, future)
            .await
            .expect("join must success")
    }

    #[async_backtrace::framed]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let op = self.inner.clone();
        let path = path.to_string();

        let future = async move { op.read(&path, args).await };

        let future = TrackedFuture::create(future);
        self.runtime
            .spawn(GLOBAL_TASK, future)
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
        let future = async move { op.write(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime
            .spawn(GLOBAL_TASK, future)
            .await
            .expect("join must success")
    }

    #[async_backtrace::framed]
    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.stat(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime
            .spawn(GLOBAL_TASK, future)
            .await
            .expect("join must success")
    }

    #[async_backtrace::framed]
    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.delete(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime
            .spawn(GLOBAL_TASK, future)
            .await
            .expect("join must success")
    }

    #[async_backtrace::framed]
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.list(&path, args).await };
        let future = TrackedFuture::create(future);
        self.runtime
            .spawn(GLOBAL_TASK, future)
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
    runtime: Arc<Runtime>,
    state: State<R>,
    buf: Vec<u8>,
}

impl<R> RuntimeIO<R> {
    fn new(inner: R, runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            state: State::Idle(Some(inner)),
            buf: vec![],
        }
    }
}

pub enum State<R: 'static> {
    Idle(Option<R>),
    Read(JoinHandle<(R, Result<Vec<u8>>)>),
    Seek(JoinHandle<(R, Result<u64>)>),
    Next(JoinHandle<(R, Option<Result<Bytes>>)>),
}

/// Safety: State will only be accessed under &mut.
unsafe impl<R> Sync for State<R> {}

impl<R: oio::Read> oio::Read for RuntimeIO<R> {
    /// TODO: the performance of `read` could be affected, we will improve it later.
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match &mut self.state {
            State::Idle(r) => {
                let mut r = r.take().expect("Idle must have a valid reader");
                let mut buffer = mem::take(&mut self.buf);

                buffer.reserve(buf.len());
                // Safety: buffer is reserved with buf.len() bytes.
                #[allow(clippy::uninit_vec)]
                unsafe {
                    buffer.set_len(buf.len())
                }

                let future = async move {
                    let mut buffer = buffer;
                    let res = r.read(&mut buffer).await;
                    match res {
                        Ok(size) => {
                            // Safety: we trust our reader, the returning size is correct.
                            unsafe { buffer.set_len(size) }
                            (r, Ok(buffer))
                        }
                        Err(err) => (r, Err(err)),
                    }
                };
                let future = TrackedFuture::create(future);
                self.state = State::Read(self.runtime.spawn(GLOBAL_TASK, future));

                self.poll_read(cx, buf)
            }
            State::Read(future) => {
                let (r, res) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));
                match res {
                    Ok(mut buffer) => {
                        let size = buffer.len();
                        buf[..size].copy_from_slice(&buffer);
                        // Safety: set length to 0 as we don't care the remaining content.
                        unsafe { buffer.set_len(0) }
                        // Always reuse the same buffer
                        self.buf = buffer;
                        Poll::Ready(Ok(size))
                    }
                    Err(err) => Poll::Ready(Err(err)),
                }
            }
            State::Seek(future) => {
                let (r, _) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));

                self.poll_read(cx, buf)
            }
            State::Next(future) => {
                let (r, _) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));

                self.poll_read(cx, buf)
            }
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        match &mut self.state {
            State::Idle(r) => {
                let mut r = r.take().expect("Idle must have a valid reader");
                let future = async move {
                    let res = r.seek(pos).await;
                    (r, res)
                };
                let future = TrackedFuture::create(future);
                self.state = State::Seek(self.runtime.spawn(GLOBAL_TASK, future));

                self.poll_seek(cx, pos)
            }
            State::Read(future) => {
                let (r, _) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));

                self.poll_seek(cx, pos)
            }
            State::Seek(future) => {
                let (r, res) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));

                Poll::Ready(res)
            }
            State::Next(future) => {
                let (r, _) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));

                self.poll_seek(cx, pos)
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<bytes::Bytes>>> {
        match &mut self.state {
            State::Idle(r) => {
                let mut r = r.take().expect("Idle must have a valid reader");
                let future = async move {
                    let res = r.next().await;
                    (r, res)
                };
                let future = TrackedFuture::create(future);
                self.state = State::Next(self.runtime.spawn(GLOBAL_TASK, future));

                self.poll_next(cx)
            }
            State::Read(future) => {
                let (r, _) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));

                self.poll_next(cx)
            }
            State::Seek(future) => {
                let (r, _) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));

                self.poll_next(cx)
            }
            State::Next(future) => {
                let (r, res) = ready!(Pin::new(future).poll(cx)).expect("join must success");
                self.state = State::Idle(Some(r));

                Poll::Ready(res)
            }
        }
    }
}
