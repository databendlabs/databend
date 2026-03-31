// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Optimized local I/Os

use std::fs::File;
use std::io::{ErrorKind, Read, SeekFrom};
use std::ops::Range;
use std::sync::Arc;

// TODO: Clean up windows/unix stuff
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use object_store::path::Path;
use snafu::location;
use tokio::io::AsyncSeekExt;
use tokio::sync::OnceCell;
use tracing::instrument;

use crate::object_store::DEFAULT_LOCAL_IO_PARALLELISM;
use crate::traits::{Reader, Writer};
use crate::utils::tracking_store::IOTracker;

/// Convert an [`object_store::path::Path`] to a [`std::path::Path`].
pub fn to_local_path(path: &Path) -> String {
    if cfg!(windows) {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

/// Recursively remove a directory, specified by [`object_store::path::Path`].
pub fn remove_dir_all(path: &Path) -> Result<()> {
    let local_path = to_local_path(path);
    std::fs::remove_dir_all(local_path).map_err(|err| match err.kind() {
        ErrorKind::NotFound => Error::NotFound {
            uri: path.to_string(),
            location: location!(),
        },
        _ => Error::from(err),
    })?;
    Ok(())
}

/// Copy a file from one location to another, supporting cross-filesystem copies.
///
/// Unlike hard links, this function works across filesystem boundaries.
pub fn copy_file(from: &Path, to: &Path) -> Result<()> {
    let from_path = to_local_path(from);
    let to_path = to_local_path(to);

    // Ensure the parent directory exists
    if let Some(parent) = std::path::Path::new(&to_path).parent() {
        std::fs::create_dir_all(parent).map_err(Error::from)?;
    }

    std::fs::copy(&from_path, &to_path).map_err(|err| match err.kind() {
        ErrorKind::NotFound => Error::NotFound {
            uri: from.to_string(),
            location: location!(),
        },
        _ => Error::from(err),
    })?;
    Ok(())
}

/// [ObjectReader] for local file system.
#[derive(Debug)]
pub struct LocalObjectReader {
    /// File handler.
    file: Arc<File>,

    /// Fie path.
    path: Path,

    /// Known size of the file. This is either passed in on construction or
    /// cached on the first metadata call.
    size: OnceCell<usize>,

    /// Block size, in bytes.
    block_size: usize,

    /// IO tracker for monitoring read operations.
    io_tracker: Arc<IOTracker>,
}

impl DeepSizeOf for LocalObjectReader {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // Skipping `file` as it should just be a file handle
        self.path.as_ref().deep_size_of_children(context)
    }
}

impl LocalObjectReader {
    pub async fn open_local_path(
        path: impl AsRef<std::path::Path>,
        block_size: usize,
        known_size: Option<usize>,
    ) -> Result<Box<dyn Reader>> {
        let path = path.as_ref().to_owned();
        let object_store_path = Path::from_filesystem_path(&path)?;
        Self::open(&object_store_path, block_size, known_size).await
    }

    /// Open a local object reader, with default prefetch size.
    ///
    /// For backward compatibility with existing code that doesn't need tracking.
    #[instrument(level = "debug")]
    pub async fn open(
        path: &Path,
        block_size: usize,
        known_size: Option<usize>,
    ) -> Result<Box<dyn Reader>> {
        Self::open_with_tracker(path, block_size, known_size, Default::default()).await
    }

    /// Open a local object reader with optional IO tracking.
    #[instrument(level = "debug")]
    pub(crate) async fn open_with_tracker(
        path: &Path,
        block_size: usize,
        known_size: Option<usize>,
        io_tracker: Arc<IOTracker>,
    ) -> Result<Box<dyn Reader>> {
        let path = path.clone();
        let local_path = to_local_path(&path);
        tokio::task::spawn_blocking(move || {
            let file = File::open(&local_path).map_err(|e| match e.kind() {
                ErrorKind::NotFound => Error::NotFound {
                    uri: path.to_string(),
                    location: location!(),
                },
                _ => e.into(),
            })?;
            let size = OnceCell::new_with(known_size);
            Ok(Box::new(Self {
                file: Arc::new(file),
                block_size,
                size,
                path,
                io_tracker,
            }) as Box<dyn Reader>)
        })
        .await?
    }
}

#[async_trait]
impl Reader for LocalObjectReader {
    fn path(&self) -> &Path {
        &self.path
    }

    fn block_size(&self) -> usize {
        self.block_size
    }

    fn io_parallelism(&self) -> usize {
        DEFAULT_LOCAL_IO_PARALLELISM
    }

    /// Returns the file size.
    async fn size(&self) -> object_store::Result<usize> {
        let file = self.file.clone();
        self.size
            .get_or_try_init(|| async move {
                let metadata = tokio::task::spawn_blocking(move || {
                    file.metadata().map_err(|err| object_store::Error::Generic {
                        store: "LocalFileSystem",
                        source: err.into(),
                    })
                })
                .await??;
                Ok(metadata.len() as usize)
            })
            .await
            .cloned()
    }

    /// Reads a range of data.
    #[instrument(level = "debug", skip(self))]
    async fn get_range(&self, range: Range<usize>) -> object_store::Result<Bytes> {
        let file = self.file.clone();
        let io_tracker = self.io_tracker.clone();
        let path = self.path.clone();
        let num_bytes = range.len() as u64;
        let range_u64 = (range.start as u64)..(range.end as u64);

        let result = tokio::task::spawn_blocking(move || {
            let mut buf = BytesMut::with_capacity(range.len());
            // Safety: `buf` is set with appropriate capacity above. It is
            // written to below and we check all data is initialized at that point.
            unsafe { buf.set_len(range.len()) };
            #[cfg(unix)]
            file.read_exact_at(buf.as_mut(), range.start as u64)?;
            #[cfg(windows)]
            read_exact_at(file, buf.as_mut(), range.start as u64)?;

            Ok(buf.freeze())
        })
        .await?
        .map_err(|err: std::io::Error| object_store::Error::Generic {
            store: "LocalFileSystem",
            source: err.into(),
        });

        if result.is_ok() {
            io_tracker.record_read("get_range", path, num_bytes, Some(range_u64));
        }

        result
    }

    /// Reads the entire file.
    #[instrument(level = "debug", skip(self))]
    async fn get_all(&self) -> object_store::Result<Bytes> {
        let mut file = self.file.clone();
        let io_tracker = self.io_tracker.clone();
        let path = self.path.clone();

        let result = tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            file.read_to_end(buf.as_mut())?;
            Ok(Bytes::from(buf))
        })
        .await?
        .map_err(|err: std::io::Error| object_store::Error::Generic {
            store: "LocalFileSystem",
            source: err.into(),
        });

        if let Ok(bytes) = &result {
            io_tracker.record_read("get_all", path, bytes.len() as u64, None);
        }

        result
    }
}

#[cfg(windows)]
fn read_exact_at(file: Arc<File>, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    let expected_len = buf.len();
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    if !buf.is_empty() {
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "failed to fill whole buffer. Expected {} bytes, got {}",
                expected_len, offset
            ),
        ))
    } else {
        Ok(())
    }
}

#[async_trait]
impl Writer for tokio::fs::File {
    async fn tell(&mut self) -> Result<usize> {
        Ok(self.seek(SeekFrom::Current(0)).await? as usize)
    }
}
