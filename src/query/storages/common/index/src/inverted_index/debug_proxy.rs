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

// Derived from Quickwit's DebugProxyDirectory.
// Copyright 2021-Present Datadog, Inc.
// Modified by Datafuse Labs to record opaque ranges needed to open an index.

use std::fmt;
use std::io;
use std::mem;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use tantivy::Directory;
use tantivy::HasLen;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::directory::error::OpenReadError;

use super::read_only_directory;

/// One byte range read observed through a [`DebugProxyDirectory`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadOperation {
    /// Logical Tantivy file path.
    pub path: PathBuf,
    /// Start offset in the logical file.
    pub offset: usize,
    /// Number of bytes read.
    pub num_bytes: usize,
}

#[derive(Clone, Default)]
struct OperationBuffer(Arc<Mutex<Vec<ReadOperation>>>);

impl fmt::Debug for OperationBuffer {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("OperationBuffer")
    }
}

impl OperationBuffer {
    fn push(&self, operation: ReadOperation) {
        self.0
            .lock()
            .expect("operation mutex poisoned")
            .push(operation);
    }

    fn drain(&self) -> impl Iterator<Item = ReadOperation> + 'static {
        let mut guard = self.0.lock().expect("operation mutex poisoned");
        let operations = mem::take(&mut *guard);
        operations.into_iter()
    }
}

/// Read-only proxy that records all successful reads.
#[derive(Debug)]
pub struct DebugProxyDirectory<D: Directory> {
    underlying: Arc<D>,
    operations: OperationBuffer,
}

impl<D: Directory> Clone for DebugProxyDirectory<D> {
    fn clone(&self) -> Self {
        Self {
            underlying: self.underlying.clone(),
            operations: self.operations.clone(),
        }
    }
}

impl<D: Directory> DebugProxyDirectory<D> {
    /// Wraps a directory.
    pub fn wrap(directory: D) -> Self {
        Self {
            underlying: Arc::new(directory),
            operations: OperationBuffer::default(),
        }
    }

    /// Drains recorded operations.
    pub fn drain_read_operations(&self) -> impl Iterator<Item = ReadOperation> + '_ {
        self.operations.drain()
    }
}

struct DebugProxyFileHandle<D: Directory> {
    directory: DebugProxyDirectory<D>,
    underlying: Arc<dyn FileHandle>,
    path: PathBuf,
}

impl<D: Directory> fmt::Debug for DebugProxyFileHandle<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("DebugProxyFileHandle")
            .field("path", &self.path)
            .finish()
    }
}

impl<D: Directory> HasLen for DebugProxyFileHandle<D> {
    fn len(&self) -> usize {
        self.underlying.len()
    }
}

#[async_trait]
impl<D: Directory> FileHandle for DebugProxyFileHandle<D> {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        let bytes = self.underlying.read_bytes(byte_range.clone())?;
        self.directory.operations.push(ReadOperation {
            path: self.path.clone(),
            offset: byte_range.start,
            num_bytes: bytes.len(),
        });
        Ok(bytes)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        let bytes = self.underlying.read_bytes_async(byte_range.clone()).await?;
        self.directory.operations.push(ReadOperation {
            path: self.path.clone(),
            offset: byte_range.start,
            num_bytes: bytes.len(),
        });
        Ok(bytes)
    }
}

impl<D: Directory> Directory for DebugProxyDirectory<D> {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(DebugProxyFileHandle {
            directory: self.clone(),
            underlying: self.underlying.get_file_handle(path)?,
            path: path.to_path_buf(),
        }))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let bytes = self.underlying.atomic_read(path)?;
        self.operations.push(ReadOperation {
            path: path.to_path_buf(),
            offset: 0,
            num_bytes: bytes.len(),
        });
        Ok(bytes)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.underlying.exists(path)
    }

    read_only_directory!();
}

#[cfg(test)]
mod tests {
    use tantivy::directory::RamDirectory;

    use super::*;

    #[test]
    fn test_records_read_range() {
        let directory = RamDirectory::default();
        directory
            .atomic_write(Path::new("file"), b"abcdef")
            .unwrap();
        let proxy = DebugProxyDirectory::wrap(directory);
        assert_eq!(
            proxy
                .open_read(Path::new("file"))
                .unwrap()
                .read_bytes_slice(2..5)
                .unwrap()
                .as_ref(),
            b"cde"
        );
        assert_eq!(proxy.drain_read_operations().collect::<Vec<_>>(), vec![
            ReadOperation {
                path: PathBuf::from("file"),
                offset: 2,
                num_bytes: 3,
            }
        ]);
    }
}
