// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::Poll;

use crate::object_store::ObjectStore as LanceObjectStore;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::MultipartUpload;
use object_store::{path::Path, Error as OSError, ObjectStore, Result as OSResult};
use rand::Rng;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::task::JoinSet;

use lance_core::{Error, Result};
use tracing::Instrument;

use crate::traits::Writer;
use snafu::location;
use tokio::runtime::Handle;

/// Start at 5MB.
const INITIAL_UPLOAD_STEP: usize = 1024 * 1024 * 5;

fn max_upload_parallelism() -> usize {
    static MAX_UPLOAD_PARALLELISM: OnceLock<usize> = OnceLock::new();
    *MAX_UPLOAD_PARALLELISM.get_or_init(|| {
        std::env::var("LANCE_UPLOAD_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10)
    })
}

fn max_conn_reset_retries() -> u16 {
    static MAX_CONN_RESET_RETRIES: OnceLock<u16> = OnceLock::new();
    *MAX_CONN_RESET_RETRIES.get_or_init(|| {
        std::env::var("LANCE_CONN_RESET_RETRIES")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(20)
    })
}

fn initial_upload_size() -> usize {
    static LANCE_INITIAL_UPLOAD_SIZE: OnceLock<usize> = OnceLock::new();
    *LANCE_INITIAL_UPLOAD_SIZE.get_or_init(|| {
        std::env::var("LANCE_INITIAL_UPLOAD_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .inspect(|size| {
                if *size < INITIAL_UPLOAD_STEP {
                    // Minimum part size in GCS and S3
                    panic!("LANCE_INITIAL_UPLOAD_SIZE must be at least 5MB");
                } else if *size > 1024 * 1024 * 1024 * 5 {
                    // Maximum part size in GCS and S3
                    panic!("LANCE_INITIAL_UPLOAD_SIZE must be at most 5GB");
                }
            })
            .unwrap_or(INITIAL_UPLOAD_STEP)
    })
}

/// Writer to an object in an object store.
///
/// If the object is small enough, the writer will upload the object in a single
/// PUT request. If the object is larger, the writer will create a multipart
/// upload and upload parts in parallel.
///
/// This implements the `AsyncWrite` trait.
pub struct ObjectWriter {
    state: UploadState,
    path: Arc<Path>,
    cursor: usize,
    connection_resets: u16,
    buffer: Vec<u8>,
    // TODO: use constant size to support R2
    use_constant_size_upload_parts: bool,
}

#[derive(Debug, Clone, Default)]
pub struct WriteResult {
    pub size: usize,
    pub e_tag: Option<String>,
}

enum UploadState {
    /// The writer has been opened but no data has been written yet. Will be in
    /// this state until the buffer is full or the writer is shut down.
    Started(Arc<dyn ObjectStore>),
    /// The writer is in the process of creating a multipart upload.
    CreatingUpload(BoxFuture<'static, OSResult<Box<dyn MultipartUpload>>>),
    /// The writer is in the process of uploading parts.
    InProgress {
        part_idx: u16,
        upload: Box<dyn MultipartUpload>,
        futures: JoinSet<std::result::Result<(), UploadPutError>>,
    },
    /// The writer is in the process of uploading data in a single PUT request.
    /// This happens when shutdown is called before the buffer is full.
    PuttingSingle(BoxFuture<'static, OSResult<WriteResult>>),
    /// The writer is in the process of completing the multipart upload.
    Completing(BoxFuture<'static, OSResult<WriteResult>>),
    /// The writer has been shut down and all data has been written.
    Done(WriteResult),
}

/// Methods for state transitions.
impl UploadState {
    fn started_to_putting_single(&mut self, path: Arc<Path>, buffer: Vec<u8>) {
        // To get owned self, we temporarily swap with Done.
        let this = std::mem::replace(self, Self::Done(WriteResult::default()));
        *self = match this {
            Self::Started(store) => {
                let fut = async move {
                    let size = buffer.len();
                    let res = store.put(&path, buffer.into()).await?;
                    Ok(WriteResult {
                        size,
                        e_tag: res.e_tag,
                    })
                };
                Self::PuttingSingle(Box::pin(fut))
            }
            _ => unreachable!(),
        }
    }

    fn in_progress_to_completing(&mut self) {
        // To get owned self, we temporarily swap with Done.
        let this = std::mem::replace(self, Self::Done(WriteResult::default()));
        *self = match this {
            Self::InProgress {
                mut upload,
                futures,
                ..
            } => {
                debug_assert!(futures.is_empty());
                let fut = async move {
                    let res = upload.complete().await?;
                    Ok(WriteResult {
                        size: 0, // This will be set properly later.
                        e_tag: res.e_tag,
                    })
                };
                Self::Completing(Box::pin(fut))
            }
            _ => unreachable!(),
        };
    }
}

impl ObjectWriter {
    pub async fn new(object_store: &LanceObjectStore, path: &Path) -> Result<Self> {
        Ok(Self {
            state: UploadState::Started(object_store.inner.clone()),
            cursor: 0,
            path: Arc::new(path.clone()),
            connection_resets: 0,
            buffer: Vec::with_capacity(initial_upload_size()),
            use_constant_size_upload_parts: object_store.use_constant_size_upload_parts,
        })
    }

    /// Returns the contents of `buffer` as a `Bytes` object and resets `buffer`.
    /// The new capacity of `buffer` is determined by the current part index.
    fn next_part_buffer(buffer: &mut Vec<u8>, part_idx: u16, constant_upload_size: bool) -> Bytes {
        let new_capacity = if constant_upload_size {
            // The store does not support variable part sizes, so use the initial size.
            initial_upload_size()
        } else {
            // Increase the upload size every 100 parts. This gives maximum part size of 2.5TB.
            initial_upload_size().max(((part_idx / 100) as usize + 1) * INITIAL_UPLOAD_STEP)
        };
        let new_buffer = Vec::with_capacity(new_capacity);
        let part = std::mem::replace(buffer, new_buffer);
        Bytes::from(part)
    }

    fn put_part(
        upload: &mut dyn MultipartUpload,
        buffer: Bytes,
        part_idx: u16,
        sleep: Option<std::time::Duration>,
    ) -> BoxFuture<'static, std::result::Result<(), UploadPutError>> {
        log::debug!(
            "MultipartUpload submitting part with {} bytes",
            buffer.len()
        );
        let fut = upload.put_part(buffer.clone().into());
        Box::pin(async move {
            if let Some(sleep) = sleep {
                tokio::time::sleep(sleep).await;
            }
            fut.await.map_err(|source| UploadPutError {
                part_idx,
                buffer,
                source,
            })?;
            Ok(())
        })
    }

    fn poll_tasks(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::result::Result<(), io::Error> {
        let mut_self = &mut *self;
        loop {
            match &mut mut_self.state {
                UploadState::Started(_) | UploadState::Done(_) => break,
                UploadState::CreatingUpload(ref mut fut) => match fut.poll_unpin(cx) {
                    Poll::Ready(Ok(mut upload)) => {
                        let mut futures = JoinSet::new();

                        let data = Self::next_part_buffer(
                            &mut mut_self.buffer,
                            0,
                            mut_self.use_constant_size_upload_parts,
                        );
                        futures.spawn(Self::put_part(upload.as_mut(), data, 0, None));

                        mut_self.state = UploadState::InProgress {
                            part_idx: 1, // We just used 0
                            futures,
                            upload,
                        };
                    }
                    Poll::Ready(Err(e)) => return Err(std::io::Error::other(e)),
                    Poll::Pending => break,
                },
                UploadState::InProgress {
                    upload, futures, ..
                } => {
                    while let Poll::Ready(Some(res)) = futures.poll_join_next(cx) {
                        match res {
                            Ok(Ok(())) => {}
                            Err(err) => return Err(std::io::Error::other(err)),
                            Ok(Err(UploadPutError {
                                source: OSError::Generic { source, .. },
                                part_idx,
                                buffer,
                            })) if source
                                .to_string()
                                .to_lowercase()
                                .contains("connection reset by peer") =>
                            {
                                if mut_self.connection_resets < max_conn_reset_retries() {
                                    // Retry, but only up to max_conn_reset_retries of them.
                                    mut_self.connection_resets += 1;

                                    // Resubmit with random jitter
                                    let sleep_time_ms = rand::rng().random_range(2_000..8_000);
                                    let sleep_time =
                                        std::time::Duration::from_millis(sleep_time_ms);

                                    futures.spawn(Self::put_part(
                                        upload.as_mut(),
                                        buffer,
                                        part_idx,
                                        Some(sleep_time),
                                    ));
                                } else {
                                    return Err(io::Error::new(
                                        io::ErrorKind::ConnectionReset,
                                        Box::new(ConnectionResetError {
                                            message: format!(
                                                "Hit max retries ({}) for connection reset",
                                                max_conn_reset_retries()
                                            ),
                                            source,
                                        }),
                                    ));
                                }
                            }
                            Ok(Err(err)) => return Err(err.source.into()),
                        }
                    }
                    break;
                }
                UploadState::PuttingSingle(ref mut fut) | UploadState::Completing(ref mut fut) => {
                    match fut.poll_unpin(cx) {
                        Poll::Ready(Ok(mut res)) => {
                            res.size = mut_self.cursor;
                            mut_self.state = UploadState::Done(res)
                        }
                        Poll::Ready(Err(e)) => return Err(std::io::Error::other(e)),
                        Poll::Pending => break,
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<WriteResult> {
        AsyncWriteExt::shutdown(self).await.map_err(|e| {
            Error::io(
                format!("failed to shutdown object writer for {}: {}", self.path, e),
                // and wrap it in here.
                location!(),
            )
        })?;
        if let UploadState::Done(result) = &self.state {
            Ok(result.clone())
        } else {
            unreachable!()
        }
    }

    pub async fn abort(&mut self) {
        let state = std::mem::replace(&mut self.state, UploadState::Done(WriteResult::default()));
        if let UploadState::InProgress { mut upload, .. } = state {
            let _ = upload.abort().await;
        }
    }
}

impl Drop for ObjectWriter {
    fn drop(&mut self) {
        // If there is a multipart upload started but not finished, we should abort it.
        if matches!(self.state, UploadState::InProgress { .. }) {
            // Take ownership of the state.
            let state =
                std::mem::replace(&mut self.state, UploadState::Done(WriteResult::default()));
            if let UploadState::InProgress { mut upload, .. } = state {
                if let Ok(handle) = Handle::try_current() {
                    handle.spawn(async move {
                        let _ = upload.abort().await;
                    });
                }
            }
        }
    }
}

/// Returned error from trying to upload a part.
/// Has the part_idx and buffer so we can pass
/// them to the retry logic.
struct UploadPutError {
    part_idx: u16,
    buffer: Bytes,
    source: OSError,
}

#[derive(Debug)]
struct ConnectionResetError {
    message: String,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl std::error::Error for ConnectionResetError {}

impl std::fmt::Display for ConnectionResetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.message, self.source)
    }
}

impl AsyncWrite for ObjectWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        self.as_mut().poll_tasks(cx)?;

        // Fill buffer up to remaining capacity.
        let remaining_capacity = self.buffer.capacity() - self.buffer.len();
        let bytes_to_write = std::cmp::min(remaining_capacity, buf.len());
        self.buffer.extend_from_slice(&buf[..bytes_to_write]);
        self.cursor += bytes_to_write;

        // Rust needs a little help to borrow self mutably and immutably at the same time
        // through a Pin.
        let mut_self = &mut *self;

        // Instantiate next request, if available.
        if mut_self.buffer.capacity() == mut_self.buffer.len() {
            match &mut mut_self.state {
                UploadState::Started(store) => {
                    let path = mut_self.path.clone();
                    let store = store.clone();
                    let fut = Box::pin(async move { store.put_multipart(path.as_ref()).await });
                    self.state = UploadState::CreatingUpload(fut);
                }
                UploadState::InProgress {
                    upload,
                    part_idx,
                    futures,
                    ..
                } => {
                    // TODO: Make max concurrency configurable from storage options.
                    if futures.len() < max_upload_parallelism() {
                        let data = Self::next_part_buffer(
                            &mut mut_self.buffer,
                            *part_idx,
                            mut_self.use_constant_size_upload_parts,
                        );
                        futures.spawn(
                            Self::put_part(upload.as_mut(), data, *part_idx, None)
                                .instrument(tracing::Span::current()),
                        );
                        *part_idx += 1;
                    }
                }
                _ => {}
            }
        }

        self.poll_tasks(cx)?;

        match bytes_to_write {
            0 => Poll::Pending,
            _ => Poll::Ready(Ok(bytes_to_write)),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        self.as_mut().poll_tasks(cx)?;

        match &self.state {
            UploadState::Started(_) | UploadState::Done(_) => Poll::Ready(Ok(())),
            UploadState::CreatingUpload(_)
            | UploadState::Completing(_)
            | UploadState::PuttingSingle(_) => Poll::Pending,
            UploadState::InProgress { futures, .. } => {
                if futures.is_empty() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        loop {
            self.as_mut().poll_tasks(cx)?;

            // Rust needs a little help to borrow self mutably and immutably at the same time
            // through a Pin.
            let mut_self = &mut *self;
            match &mut mut_self.state {
                UploadState::Done(_) => return Poll::Ready(Ok(())),
                UploadState::CreatingUpload(_)
                | UploadState::PuttingSingle(_)
                | UploadState::Completing(_) => return Poll::Pending,
                UploadState::Started(_) => {
                    // If we didn't start a multipart upload, we can just do a single put.
                    let part = std::mem::take(&mut mut_self.buffer);
                    let path = mut_self.path.clone();
                    self.state.started_to_putting_single(path, part);
                }
                UploadState::InProgress {
                    upload,
                    futures,
                    part_idx,
                } => {
                    // Flush final batch
                    if !mut_self.buffer.is_empty() && futures.len() < max_upload_parallelism() {
                        // We can just use `take` since we don't need the buffer anymore.
                        let data = Bytes::from(std::mem::take(&mut mut_self.buffer));
                        futures.spawn(
                            Self::put_part(upload.as_mut(), data, *part_idx, None)
                                .instrument(tracing::Span::current()),
                        );
                        // We need to go back to beginning of loop to poll the
                        // new feature and get the waker registered on the ctx.
                        continue;
                    }

                    // We handle the transition from in progress to completing here.
                    if futures.is_empty() {
                        self.state.in_progress_to_completing();
                    } else {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Writer for ObjectWriter {
    async fn tell(&mut self) -> Result<usize> {
        Ok(self.cursor)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_write() {
        let store = LanceObjectStore::memory();

        let mut object_writer = ObjectWriter::new(&store, &Path::from("/foo"))
            .await
            .unwrap();
        assert_eq!(object_writer.tell().await.unwrap(), 0);

        let buf = vec![0; 256];
        assert_eq!(object_writer.write(buf.as_slice()).await.unwrap(), 256);
        assert_eq!(object_writer.tell().await.unwrap(), 256);

        assert_eq!(object_writer.write(buf.as_slice()).await.unwrap(), 256);
        assert_eq!(object_writer.tell().await.unwrap(), 512);

        assert_eq!(object_writer.write(buf.as_slice()).await.unwrap(), 256);
        assert_eq!(object_writer.tell().await.unwrap(), 256 * 3);

        let res = object_writer.shutdown().await.unwrap();
        assert_eq!(res.size, 256 * 3);

        // Trigger multi part upload
        let mut object_writer = ObjectWriter::new(&store, &Path::from("/bar"))
            .await
            .unwrap();
        let buf = vec![0; INITIAL_UPLOAD_STEP / 3 * 2];
        for i in 0..5 {
            // Write more data to trigger the multipart upload
            // This should be enough to trigger a multipart upload
            object_writer.write_all(buf.as_slice()).await.unwrap();
            // Check the cursor
            assert_eq!(object_writer.tell().await.unwrap(), (i + 1) * buf.len());
        }
        let res = object_writer.shutdown().await.unwrap();
        assert_eq!(res.size, buf.len() * 5);
    }

    #[tokio::test]
    async fn test_abort_write() {
        let store = LanceObjectStore::memory();

        let mut object_writer = ObjectWriter::new(&store, &Path::from("/foo"))
            .await
            .unwrap();
        object_writer.abort().await;
    }
}
