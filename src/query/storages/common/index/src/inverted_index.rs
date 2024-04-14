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

// Copyright (c) 2018 by the tantivy project authors
// (https://github.com/quickwit-oss/tantivy), as listed in the AUTHORS file.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

use std::io;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::result;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::testify_version;
use databend_storages_common_table_meta::meta::Versioned;
use log::warn;
use tantivy::directory::error::DeleteError;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::error::OpenWriteError;
use tantivy::directory::AntiCallToken;
use tantivy::directory::FileHandle;
use tantivy::directory::FileSlice;
use tantivy::directory::OwnedBytes;
use tantivy::directory::TerminatingWrite;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::directory::WritePtr;
use tantivy::Directory;

/// The Writer just writes a buffer.
struct VecWriter {
    path: PathBuf,
    data: Cursor<Vec<u8>>,
    is_flushed: bool,
}

impl VecWriter {
    fn new(path_buf: PathBuf) -> VecWriter {
        VecWriter {
            path: path_buf,
            data: Cursor::new(Vec::new()),
            is_flushed: true,
        }
    }
}

impl Drop for VecWriter {
    fn drop(&mut self) {
        if !self.is_flushed {
            warn!(
                "You forgot to flush {:?} before its writer got Drop. Do not rely on drop. This \
                 also occurs when the indexer crashed, so you may want to check the logs for the \
                 root cause.",
                self.path
            )
        }
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.is_flushed = false;
        self.data.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.is_flushed = true;
        Ok(())
    }
}

impl TerminatingWrite for VecWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

// InvertedIndexDirectory holds all indexed data for tantivy queries to search for relevant data.
// The data is read-only, write and delete operations will be ignored and return success directly.
#[derive(Clone, Debug)]
pub struct InvertedIndexDirectory {
    data: OwnedBytes,

    fast_fields_range: Range<usize>,
    store_range: Range<usize>,
    fieldnorm_range: Range<usize>,
    position_range: Range<usize>,
    posting_range: Range<usize>,
    term_range: Range<usize>,
    meta_range: Range<usize>,
    managed_range: Range<usize>,

    meta_path: PathBuf,
    managed_path: PathBuf,
}

impl InvertedIndexDirectory {
    pub fn try_create(data: Vec<u8>) -> Result<Self> {
        if data.len() < 36 {
            return Err(OpenReadError::IoError {
                io_error: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid index data",
                )
                .into(),
                filepath: PathBuf::from("InvertedIndexDirectory"),
            }
            .into());
        }

        let mut reader = Cursor::new(data.clone());
        reader.seek(SeekFrom::End(-36))?;

        let mut buf = vec![0u8; 4];
        let fast_fields_offset = read_u32(&mut reader, buf.as_mut_slice())? as usize;
        let store_offset = read_u32(&mut reader, buf.as_mut_slice())? as usize;
        let field_norms_offset = read_u32(&mut reader, buf.as_mut_slice())? as usize;
        let positions_offset = read_u32(&mut reader, buf.as_mut_slice())? as usize;
        let postings_offset = read_u32(&mut reader, buf.as_mut_slice())? as usize;
        let terms_offset = read_u32(&mut reader, buf.as_mut_slice())? as usize;
        let meta_offset = read_u32(&mut reader, buf.as_mut_slice())? as usize;
        let managed_offset = read_u32(&mut reader, buf.as_mut_slice())? as usize;

        if data.len() < managed_offset {
            return Err(OpenReadError::IoError {
                io_error: std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid data")
                    .into(),
                filepath: PathBuf::from("CacheDirectory"),
            }
            .into());
        }

        let data = OwnedBytes::new(data);

        let fast_fields_range = Range {
            start: 0,
            end: fast_fields_offset as usize,
        };
        let store_range = Range {
            start: fast_fields_offset as usize,
            end: store_offset as usize,
        };
        let fieldnorm_range = Range {
            start: store_offset as usize,
            end: field_norms_offset as usize,
        };
        let position_range = Range {
            start: field_norms_offset as usize,
            end: positions_offset as usize,
        };
        let posting_range = Range {
            start: positions_offset as usize,
            end: postings_offset as usize,
        };
        let term_range = Range {
            start: postings_offset as usize,
            end: terms_offset as usize,
        };
        let meta_range = Range {
            start: terms_offset as usize,
            end: meta_offset as usize,
        };
        let managed_range = Range {
            start: meta_offset as usize,
            end: managed_offset as usize,
        };

        let meta_path = PathBuf::from("meta.json");
        let managed_path = PathBuf::from(".managed.json");

        Ok(Self {
            data,
            fast_fields_range,
            store_range,
            fieldnorm_range,
            position_range,
            posting_range,
            term_range,
            meta_range,
            managed_range,
            meta_path,
            managed_path,
        })
    }

    pub fn size(&self) -> usize {
        self.data.len()
            + 8 * std::mem::size_of::<Range<usize>>()
            + self.meta_path.capacity()
            + self.managed_path.capacity()
    }
}

impl Directory for InvertedIndexDirectory {
    fn get_file_handle(&self, path: &Path) -> result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> result::Result<FileSlice, OpenReadError> {
        if path == self.meta_path.as_path() {
            let bytes = self.data.slice(self.meta_range.clone());
            return Ok(FileSlice::new(Arc::new(bytes)));
        } else if path == self.managed_path.as_path() {
            let bytes = self.data.slice(self.managed_range.clone());
            return Ok(FileSlice::new(Arc::new(bytes)));
        }

        if let Some(ext) = path.extension() {
            let bytes = match ext.to_str() {
                Some("term") => self.data.slice(self.term_range.clone()),
                Some("idx") => self.data.slice(self.posting_range.clone()),
                Some("pos") => self.data.slice(self.position_range.clone()),
                Some("fieldnorm") => self.data.slice(self.fieldnorm_range.clone()),
                Some("store") => self.data.slice(self.store_range.clone()),
                Some("fast") => self.data.slice(self.fast_fields_range.clone()),
                _ => {
                    return Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)));
                }
            };
            Ok(FileSlice::new(Arc::new(bytes)))
        } else {
            Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)))
        }
    }

    fn delete(&self, _path: &Path) -> result::Result<(), DeleteError> {
        Ok(())
    }

    fn exists(&self, path: &Path) -> result::Result<bool, OpenReadError> {
        if path == self.meta_path.as_path() || path == self.managed_path.as_path() {
            return Ok(true);
        }
        if let Some(ext) = path.extension() {
            match ext.to_str() {
                Some("term") => Ok(true),
                Some("idx") => Ok(true),
                Some("pos") => Ok(true),
                Some("fieldnorm") => Ok(true),
                Some("store") => Ok(true),
                Some("fast") => Ok(true),
                _ => Ok(false),
            }
        } else {
            Ok(false)
        }
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        let path_buf = PathBuf::from(path);
        let vec_writer = VecWriter::new(path_buf);
        Ok(BufWriter::new(Box::new(vec_writer)))
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        let bytes =
            self.open_read(path)?
                .read_bytes()
                .map_err(|io_error| OpenReadError::IoError {
                    io_error: Arc::new(io_error),
                    filepath: path.to_path_buf(),
                })?;
        Ok(bytes.as_slice().to_owned())
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        let watch_handle = WatchHandle::new(Arc::new(watch_callback));
        Ok(watch_handle)
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}

#[inline(always)]
fn read_u32<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u32> {
    r.read_exact(buf)?;
    Ok(u32::from_le_bytes(buf.try_into().unwrap()))
}

impl Versioned<0> for InvertedIndexDirectory {}

pub enum InvertedIndexFilterVersion {
    V0(PhantomData<InvertedIndexDirectory>),
}

impl TryFrom<u64> for InvertedIndexFilterVersion {
    type Error = ErrorCode;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(InvertedIndexFilterVersion::V0(testify_version::<_, 0>(
                PhantomData,
            ))),
            _ => Err(ErrorCode::Internal(format!(
                "unknown inverted index filer version {value}, versions supported: 0"
            ))),
        }
    }
}
