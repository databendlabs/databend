// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use deepsize::DeepSizeOf;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use lance_core::{error::CloneableError, Error, Result};
use object_store::{path::Path, GetOptions, GetResult, ObjectStore, Result as OSResult};
use tokio::sync::OnceCell;
use tracing::instrument;

use crate::{object_store::DEFAULT_CLOUD_IO_PARALLELISM, traits::Reader};

/// Object Reader
///
/// Object Store + Base Path
#[derive(Debug)]
pub struct CloudObjectReader {
    // Object Store.
    pub object_store: Arc<dyn ObjectStore>,
    // File path
    pub path: Path,
    // File size, if known.
    size: OnceCell<usize>,

    block_size: usize,
    download_retry_count: usize,
}

impl DeepSizeOf for CloudObjectReader {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // Skipping object_store because there is no easy way to do that and it shouldn't be too big
        self.path.as_ref().deep_size_of_children(context)
    }
}

impl CloudObjectReader {
    /// Create an ObjectReader from URI
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        block_size: usize,
        known_size: Option<usize>,
        download_retry_count: usize,
    ) -> Result<Self> {
        Ok(Self {
            object_store,
            path,
            size: OnceCell::new_with(known_size),
            block_size,
            download_retry_count,
        })
    }

    // Retries for the initial request are handled by object store, but
    // there are no retries for failures that occur during the streaming
    // of the response body. Thus we add an outer retry loop here.
    async fn do_with_retry<'a, O>(
        &self,
        f: impl Fn() -> BoxFuture<'a, OSResult<O>>,
    ) -> OSResult<O> {
        let mut retries = 3;
        loop {
            match f().await {
                Ok(val) => return Ok(val),
                Err(err) => {
                    if retries == 0 {
                        return Err(err);
                    }
                    retries -= 1;
                }
            }
        }
    }

    // We have a separate retry loop here.  This is because object_store does not
    // attempt retries on downloads that fail during streaming of the response body.
    //
    // However, this failure is pretty common (e.g. timeout) and we want to retry in these
    // situations.  In addition, we provide additional logging information in these
    // failures cases.
    async fn do_get_with_outer_retry<'a>(
        &self,
        f: impl Fn() -> BoxFuture<'a, OSResult<GetResult>> + Copy,
        desc: impl Fn() -> String,
    ) -> OSResult<Bytes> {
        let mut retries = self.download_retry_count;
        loop {
            let get_result = self.do_with_retry(f).await?;
            match get_result.bytes().await {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    if retries == 0 {
                        log::warn!("Failed to download {} from {} after {} attempts.  This may indicate that cloud storage is overloaded or your timeout settings are too restrictive.  Error details: {:?}", desc(), self.path, self.download_retry_count, err);
                        return Err(err);
                    }
                    log::debug!(
                        "Retrying {} from {} (remaining retries: {}).  Error details: {:?}",
                        desc(),
                        self.path,
                        retries,
                        err
                    );
                    retries -= 1;
                }
            }
        }
    }
}

#[async_trait]
impl Reader for CloudObjectReader {
    fn path(&self) -> &Path {
        &self.path
    }

    fn block_size(&self) -> usize {
        self.block_size
    }

    fn io_parallelism(&self) -> usize {
        DEFAULT_CLOUD_IO_PARALLELISM
    }

    /// Object/File Size.
    async fn size(&self) -> object_store::Result<usize> {
        self.size
            .get_or_try_init(|| async move {
                let meta = self
                    .do_with_retry(|| self.object_store.head(&self.path))
                    .await?;
                Ok(meta.size as usize)
            })
            .await
            .cloned()
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_range(&self, range: Range<usize>) -> OSResult<Bytes> {
        self.do_get_with_outer_retry(
            || {
                let options = GetOptions {
                    range: Some(
                        Range {
                            start: range.start as u64,
                            end: range.end as u64,
                        }
                        .into(),
                    ),
                    ..Default::default()
                };
                self.object_store.get_opts(&self.path, options)
            },
            || format!("range {:?}", range),
        )
        .await
    }

    #[instrument(level = "debug", skip_all)]
    async fn get_all(&self) -> OSResult<Bytes> {
        self.do_get_with_outer_retry(
            || {
                self.object_store
                    .get_opts(&self.path, GetOptions::default())
            },
            || "read_all".to_string(),
        )
        .await
    }
}

/// A reader for a file so small, we just eagerly read it all into memory.
///
/// When created, it represents a future that will read the whole file into memory.
///
/// On the first read call, it will start the read. Multiple threads can call read at the same time.
///
/// Once the read is complete, any thread can call read again to get the result.
#[derive(Debug)]
pub struct SmallReader {
    path: Path,
    size: usize,
    state: Arc<std::sync::Mutex<SmallReaderState>>,
}

enum SmallReaderState {
    Loading(Shared<BoxFuture<'static, std::result::Result<Bytes, CloneableError>>>),
    Finished(std::result::Result<Bytes, CloneableError>),
}

impl std::fmt::Debug for SmallReaderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loading(_) => write!(f, "Loading"),
            Self::Finished(Ok(data)) => {
                write!(f, "Finished({} bytes)", data.len())
            }
            Self::Finished(Err(err)) => {
                write!(f, "Finished({})", err.0)
            }
        }
    }
}

impl SmallReader {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        path: Path,
        download_retry_count: usize,
        size: usize,
    ) -> Self {
        let path_ref = path.clone();
        let state = SmallReaderState::Loading(
            Box::pin(async move {
                let object_reader =
                    CloudObjectReader::new(store, path_ref, 0, None, download_retry_count)
                        .map_err(CloneableError)?;
                object_reader
                    .get_all()
                    .await
                    .map_err(|err| CloneableError(Error::from(err)))
            })
            .boxed()
            .shared(),
        );
        Self {
            path,
            size,
            state: Arc::new(std::sync::Mutex::new(state)),
        }
    }

    async fn wait(&self) -> OSResult<Bytes> {
        let future = {
            let state = self.state.lock().unwrap();
            match &*state {
                SmallReaderState::Loading(future) => future.clone(),
                SmallReaderState::Finished(result) => {
                    return result.clone().map_err(|err| err.0.into());
                }
            }
        };

        let result = future.await;
        let result_to_return = result.clone().map_err(|err| err.0.into());
        let mut state = self.state.lock().unwrap();
        if matches!(*state, SmallReaderState::Loading(_)) {
            *state = SmallReaderState::Finished(result);
        }
        result_to_return
    }
}

#[async_trait]
impl Reader for SmallReader {
    fn path(&self) -> &Path {
        &self.path
    }

    fn block_size(&self) -> usize {
        64 * 1024
    }

    fn io_parallelism(&self) -> usize {
        1024
    }

    /// Object/File Size.
    async fn size(&self) -> OSResult<usize> {
        Ok(self.size)
    }

    async fn get_range(&self, range: Range<usize>) -> OSResult<Bytes> {
        self.wait().await.and_then(|bytes| {
            let start = range.start;
            let end = range.end;
            if start >= bytes.len() || end > bytes.len() {
                return Err(object_store::Error::Generic {
                    store: "memory",
                    source: format!(
                        "Invalid range {}..{} for object of size {} bytes",
                        start,
                        end,
                        bytes.len()
                    )
                    .into(),
                });
            }
            Ok(bytes.slice(range))
        })
    }

    async fn get_all(&self) -> OSResult<Bytes> {
        self.wait().await
    }
}

impl DeepSizeOf for SmallReader {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        let mut size = self.path.as_ref().deep_size_of_children(context);

        if let Ok(guard) = self.state.try_lock() {
            if let SmallReaderState::Finished(Ok(data)) = &*guard {
                size += data.len();
            }
        }

        size
    }
}
