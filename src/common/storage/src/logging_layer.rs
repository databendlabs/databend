// Copyright 2023-2025 The Apache Software Foundation
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

/// This layer is originally from OpenDAL's LoggingLayer.
/// OpenDAL's LoggingLayer logs detailed information at DEBUG level, which is very useful
/// for debugging storage related issues. But the cost (even `to_string` is expensive)
/// cannot be ignored with high throughput, which cannot be prevented even logging level is
/// higher than DEBUG.
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;

use log::log;
use log::log_enabled;
use log::Level;
use opendal::raw::oio;
use opendal::raw::Access;
use opendal::raw::AccessorInfo;
use opendal::raw::Layer;
use opendal::raw::LayeredAccess;
use opendal::raw::OpCopy;
use opendal::raw::OpCreateDir;
use opendal::raw::OpDelete;
use opendal::raw::OpList;
use opendal::raw::OpPresign;
use opendal::raw::OpRead;
use opendal::raw::OpRename;
use opendal::raw::OpStat;
use opendal::raw::OpWrite;
use opendal::raw::Operation;
use opendal::raw::RpCopy;
use opendal::raw::RpCreateDir;
use opendal::raw::RpDelete;
use opendal::raw::RpList;
use opendal::raw::RpPresign;
use opendal::raw::RpRead;
use opendal::raw::RpRename;
use opendal::raw::RpStat;
use opendal::raw::RpWrite;
use opendal::Buffer;
use opendal::Error;
use opendal::ErrorKind;
use opendal::Metadata;

struct LoggingContext<'a>(&'a [(&'a dyn Display, &'a dyn Display)]);

static LOGGING_TARGET: &str = "opendal::services";

#[derive(Debug, Copy, Clone, Default)]
struct Logger;

impl Display for LoggingContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (k, v) in self.0.iter() {
            write!(f, " {k}={v}")?;
        }
        Ok(())
    }
}

impl Logger {
    #[inline]
    fn debug_enabled(&self) -> bool {
        log_enabled!(target: LOGGING_TARGET, Level::Debug)
    }

    fn log(
        &self,
        info: &AccessorInfo,
        operation: Operation,
        context: &[(&dyn Display, &dyn Display)],
        message: &str,
        err: Option<&Error>,
    ) {
        if err.is_none() && !self.debug_enabled() {
            return;
        }

        if let Some(err) = err {
            // Print error if it's unexpected, otherwise in warn.
            let lvl = if err.kind() == ErrorKind::Unexpected {
                Level::Error
            } else {
                Level::Warn
            };

            log!(
                target: LOGGING_TARGET,
                lvl,
                "service={} name={}{}: {operation} {message} {}",
                info.scheme(),
                info.name(),
                LoggingContext(context),
                // Print error message with debug output while unexpected happened.
                //
                // It's super sad that we can't bind `format_args!()` here.
                // See: https://github.com/rust-lang/rust/issues/92698
                if err.kind() != ErrorKind::Unexpected {
                   format!("{err}")
                } else {
                   format!("{err:?}")
                }
            );
        }

        log!(
            target: LOGGING_TARGET,
            Level::Debug,
            "service={} name={}{}: {operation} {message}",
            info.scheme(),
            info.name(),
            LoggingContext(context),
        );
    }
}

#[derive(Debug)]
pub struct LoggingLayer {
    logger: Logger,
}

impl LoggingLayer {
    pub fn new() -> Self {
        Self { logger: Logger }
    }
}

impl<A: Access> Layer<A> for LoggingLayer {
    type LayeredAccess = LoggingAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        LoggingAccessor {
            inner,

            info,
            logger: self.logger.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoggingAccessor<A: Access> {
    inner: A,

    info: Arc<AccessorInfo>,
    logger: Logger,
}

impl<A: Access> LayeredAccess for LoggingAccessor<A> {
    type Inner = A;
    type Reader = LoggingReader<A::Reader>;
    type Writer = LoggingWriter<A::Writer>;
    type Lister = LoggingLister<A::Lister>;
    type Deleter = LoggingDeleter<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> opendal::Result<RpCreateDir> {
        self.logger.log(
            &self.info,
            Operation::CreateDir,
            &[(&"path", &path)],
            "started",
            None,
        );

        self.inner
            .create_dir(path, args)
            .await
            .inspect(|_| {
                self.logger.log(
                    &self.info,
                    Operation::CreateDir,
                    &[(&"path", &path)],
                    "finished",
                    None,
                );
            })
            .inspect_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::CreateDir,
                    &[(&"path", &path)],
                    "failed",
                    Some(err),
                );
            })
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        self.logger.log(
            &self.info,
            Operation::Read,
            &[(&"path", &path)],
            "started",
            None,
        );

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| {
                self.logger.log(
                    &self.info,
                    Operation::Read,
                    &[(&"path", &path)],
                    "created reader",
                    None,
                );
                (
                    rp,
                    LoggingReader::new(self.info.clone(), self.logger.clone(), path, r),
                )
            })
            .inspect_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Read,
                    &[(&"path", &path)],
                    "failed",
                    Some(err),
                );
            })
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        self.logger.log(
            &self.info,
            Operation::Write,
            &[(&"path", &path)],
            "started",
            None,
        );

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[(&"path", &path)],
                    "created writer",
                    None,
                );
                let w = LoggingWriter::new(self.info.clone(), self.logger.clone(), path, w);
                (rp, w)
            })
            .inspect_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[(&"path", &path)],
                    "failed",
                    Some(err),
                );
            })
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> opendal::Result<RpCopy> {
        self.logger.log(
            &self.info,
            Operation::Copy,
            &[(&"from", &from), (&"to", &to)],
            "started",
            None,
        );

        self.inner
            .copy(from, to, args)
            .await
            .inspect(|_| {
                self.logger.log(
                    &self.info,
                    Operation::Copy,
                    &[(&"from", &from), (&"to", &to)],
                    "finished",
                    None,
                );
            })
            .inspect_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Copy,
                    &[(&"from", &from), (&"to", &to)],
                    "failed",
                    Some(err),
                );
            })
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> opendal::Result<RpRename> {
        self.logger.log(
            &self.info,
            Operation::Rename,
            &[(&"from", &from), (&"to", &to)],
            "started",
            None,
        );

        self.inner
            .rename(from, to, args)
            .await
            .inspect(|_| {
                self.logger.log(
                    &self.info,
                    Operation::Rename,
                    &[(&"from", &from), (&"to", &to)],
                    "finished",
                    None,
                );
            })
            .inspect_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Rename,
                    &[(&"from", &from), (&"to", &to)],
                    "failed",
                    Some(err),
                );
            })
    }

    async fn stat(&self, path: &str, args: OpStat) -> opendal::Result<RpStat> {
        self.logger.log(
            &self.info,
            Operation::Stat,
            &[(&"path", &path)],
            "started",
            None,
        );

        self.inner
            .stat(path, args)
            .await
            .inspect(|_| {
                self.logger.log(
                    &self.info,
                    Operation::Stat,
                    &[(&"path", &path)],
                    "finished",
                    None,
                );
            })
            .inspect_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Stat,
                    &[(&"path", &path)],
                    "failed",
                    Some(err),
                );
            })
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        self.logger
            .log(&self.info, Operation::Delete, &[], "started", None);

        self.inner
            .delete()
            .await
            .map(|(rp, d)| {
                self.logger
                    .log(&self.info, Operation::Delete, &[], "finished", None);
                let d = LoggingDeleter::new(self.info.clone(), self.logger.clone(), d);
                (rp, d)
            })
            .inspect_err(|err| {
                self.logger
                    .log(&self.info, Operation::Delete, &[], "failed", Some(err));
            })
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.logger.log(
            &self.info,
            Operation::List,
            &[(&"path", &path)],
            "started",
            None,
        );

        self.inner
            .list(path, args)
            .await
            .map(|(rp, v)| {
                self.logger.log(
                    &self.info,
                    Operation::List,
                    &[(&"path", &path)],
                    "created lister",
                    None,
                );
                let streamer = LoggingLister::new(self.info.clone(), self.logger.clone(), path, v);
                (rp, streamer)
            })
            .inspect_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::List,
                    &[(&"path", &path)],
                    "failed",
                    Some(err),
                );
            })
    }

    async fn presign(&self, path: &str, args: OpPresign) -> opendal::Result<RpPresign> {
        self.logger.log(
            &self.info,
            Operation::Presign,
            &[(&"path", &path)],
            "started",
            None,
        );

        self.inner
            .presign(path, args)
            .await
            .inspect(|_| {
                self.logger.log(
                    &self.info,
                    Operation::Presign,
                    &[(&"path", &path)],
                    "finished",
                    None,
                );
            })
            .inspect_err(|err| {
                self.logger.log(
                    &self.info,
                    Operation::Presign,
                    &[(&"path", &path)],
                    "failed",
                    Some(err),
                );
            })
    }
}

pub struct LoggingReader<R> {
    info: Arc<AccessorInfo>,
    logger: Logger,
    path: String,

    read: u64,
    inner: R,
}

impl<R> LoggingReader<R> {
    fn new(info: Arc<AccessorInfo>, logger: Logger, path: &str, reader: R) -> Self {
        Self {
            info,
            logger,
            path: path.to_string(),

            read: 0,
            inner: reader,
        }
    }
}

impl<R: oio::Read> oio::Read for LoggingReader<R> {
    async fn read(&mut self) -> opendal::Result<Buffer> {
        match self.inner.read().await {
            Ok(bs) if bs.is_empty() => {
                let size = bs.len();
                self.logger.log(
                    &self.info,
                    Operation::Read,
                    &[
                        (&"path", &self.path),
                        (&"read", &self.read),
                        (&"size", &size),
                    ],
                    "finished",
                    None,
                );
                Ok(bs)
            }
            Ok(bs) => {
                self.read += bs.len() as u64;
                Ok(bs)
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::Read,
                    &[(&"path", &self.path), (&"read", &self.read)],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

pub struct LoggingWriter<W> {
    info: Arc<AccessorInfo>,
    logger: Logger,
    path: String,

    written: u64,
    inner: W,
}

impl<W> LoggingWriter<W> {
    fn new(info: Arc<AccessorInfo>, logger: Logger, path: &str, writer: W) -> Self {
        Self {
            info,
            logger,
            path: path.to_string(),

            written: 0,
            inner: writer,
        }
    }
}

impl<W: oio::Write> oio::Write for LoggingWriter<W> {
    async fn write(&mut self, bs: Buffer) -> opendal::Result<()> {
        let size = bs.len();

        match self.inner.write(bs).await {
            Ok(_) => {
                self.written += size as u64;
                Ok(())
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[
                        (&"path", &self.path),
                        (&"written", &self.written),
                        (&"size", &size),
                    ],
                    "failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        match self.inner.abort().await {
            Ok(_) => {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[(&"path", &self.path), (&"written", &self.written)],
                    "abort succeeded",
                    None,
                );
                Ok(())
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[(&"path", &self.path), (&"written", &self.written)],
                    "abort failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }

    async fn close(&mut self) -> opendal::Result<Metadata> {
        match self.inner.close().await {
            Ok(meta) => {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[(&"path", &self.path), (&"written", &self.written)],
                    "close succeeded",
                    None,
                );
                Ok(meta)
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::Write,
                    &[(&"path", &self.path), (&"written", &self.written)],
                    "close failed",
                    Some(&err),
                );
                Err(err)
            }
        }
    }
}

pub struct LoggingLister<P> {
    info: Arc<AccessorInfo>,
    logger: Logger,
    path: String,

    listed: usize,
    inner: P,
}

impl<P> LoggingLister<P> {
    fn new(info: Arc<AccessorInfo>, logger: Logger, path: &str, inner: P) -> Self {
        Self {
            info,
            logger,
            path: path.to_string(),

            listed: 0,
            inner,
        }
    }
}

impl<P: oio::List> oio::List for LoggingLister<P> {
    async fn next(&mut self) -> opendal::Result<Option<oio::Entry>> {
        let res = self.inner.next().await;

        match &res {
            Ok(Some(_)) => {
                self.listed += 1;
            }
            Ok(None) => {
                self.logger.log(
                    &self.info,
                    Operation::List,
                    &[(&"path", &self.path), (&"listed", &self.listed)],
                    "finished",
                    None,
                );
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::List,
                    &[(&"path", &self.path), (&"listed", &self.listed)],
                    "failed",
                    Some(err),
                );
            }
        };

        res
    }
}

pub struct LoggingDeleter<D> {
    info: Arc<AccessorInfo>,
    logger: Logger,

    queued: usize,
    deleted: usize,
    inner: D,
}

impl<D> LoggingDeleter<D> {
    fn new(info: Arc<AccessorInfo>, logger: Logger, inner: D) -> Self {
        Self {
            info,
            logger,

            queued: 0,
            deleted: 0,
            inner,
        }
    }
}

impl<D: oio::Delete> oio::Delete for LoggingDeleter<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> opendal::Result<()> {
        let version = args
            .version()
            .map(|v| v.to_owned())
            .unwrap_or_else(|| "<latest>".to_string());

        let res = self.inner.delete(path, args);

        match &res {
            Ok(_) => {
                self.queued += 1;
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::Delete,
                    &[
                        (&"path", &path),
                        (&"version", &version),
                        (&"queued", &self.queued),
                        (&"deleted", &self.deleted),
                    ],
                    "failed",
                    Some(err),
                );
            }
        };

        res
    }

    async fn flush(&mut self) -> opendal::Result<usize> {
        let res = self.inner.flush().await;

        match &res {
            Ok(flushed) => {
                self.queued -= flushed;
                self.deleted += flushed;
                self.logger.log(
                    &self.info,
                    Operation::Delete,
                    &[(&"queued", &self.queued), (&"deleted", &self.deleted)],
                    "succeeded",
                    None,
                );
            }
            Err(err) => {
                self.logger.log(
                    &self.info,
                    Operation::Delete,
                    &[(&"queued", &self.queued), (&"deleted", &self.deleted)],
                    "failed",
                    Some(err),
                );
            }
        };

        res
    }
}
