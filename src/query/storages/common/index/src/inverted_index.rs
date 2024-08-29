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

use std::collections::BTreeMap;
use std::io;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use std::result;
use std::sync::Arc;

use crc32fast::Hasher;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::testify_version;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
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
use tantivy_common::BinarySerializable;
use tantivy_common::VInt;

// tantivy version is used to generate the footer data

// Index major version.
const INDEX_MAJOR_VERSION: u32 = 0;
// Index minor version.
const INDEX_MINOR_VERSION: u32 = 22;
// Index patch version.
const INDEX_PATCH_VERSION: u32 = 0;
// Index format version.
const INDEX_FORMAT_VERSION: u32 = 6;

// The magic byte of the footer to identify corruption
// or an old version of the footer.
const FOOTER_MAGIC_NUMBER: u32 = 1337;

type CrcHashU32 = u32;

/// Structure version for the index.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Version {
    major: u32,
    minor: u32,
    patch: u32,
    index_format_version: u32,
}

/// A Footer is appended every part of data, like tantivy file.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Footer {
    version: Version,
    crc: CrcHashU32,
}

impl Footer {
    fn new(crc: CrcHashU32) -> Self {
        let version = Version {
            major: INDEX_MAJOR_VERSION,
            minor: INDEX_MINOR_VERSION,
            patch: INDEX_PATCH_VERSION,
            index_format_version: INDEX_FORMAT_VERSION,
        };
        Footer { version, crc }
    }

    fn append_footer<W: std::io::Write>(&self, write: &mut W) -> Result<()> {
        let footer_payload_len = write.write(serde_json::to_string(&self)?.as_ref())?;
        BinarySerializable::serialize(&(footer_payload_len as u32), write)?;
        BinarySerializable::serialize(&FOOTER_MAGIC_NUMBER, write)?;
        Ok(())
    }
}

// Build footer for tantivy files.
// Footer is used to check whether the data is valid when open a file.
pub fn build_tantivy_footer(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    let crc = hasher.finalize();

    let footer = Footer::new(crc);
    let mut buf = Vec::new();
    footer.append_footer(&mut buf)?;
    Ok(buf)
}

#[derive(
    Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
struct Field(u32);

impl Field {
    /// Create a new field object for the given FieldId.
    const fn from_field_id(field_id: u32) -> Field {
        Field(field_id)
    }

    /// Returns a u32 identifying uniquely a field within a schema.
    #[allow(dead_code)]
    const fn field_id(self) -> u32 {
        self.0
    }
}

impl BinarySerializable for Field {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.0.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Field> {
        u32::deserialize(reader).map(Field)
    }
}

#[derive(Eq, PartialEq, Hash, Copy, Ord, PartialOrd, Clone, Debug)]
struct FileAddr {
    field: Field,
    idx: usize,
}

impl FileAddr {
    fn new(field: Field, idx: usize) -> FileAddr {
        FileAddr { field, idx }
    }
}

impl BinarySerializable for FileAddr {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.field.serialize(writer)?;
        VInt(self.idx as u64).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let field = Field::deserialize(reader)?;
        let idx = VInt::deserialize(reader)?.0 as usize;
        Ok(FileAddr { field, idx })
    }
}

// Build empty position data to be used when there are no phrase terms in the query.
// This can reduce data reading and speed up the query
fn build_empty_position_data(field_nums: usize) -> Result<OwnedBytes> {
    let offsets: Vec<_> = (0..field_nums)
        .map(|i| {
            let field = Field::from_field_id(i as u32);
            let file_addr = FileAddr::new(field, 0);
            (file_addr, 0)
        })
        .collect();

    let mut buf = Vec::new();
    VInt(offsets.len() as u64).serialize(&mut buf)?;

    let mut prev_offset = 0;
    for (file_addr, offset) in offsets {
        VInt(offset - prev_offset).serialize(&mut buf)?;
        file_addr.serialize(&mut buf)?;
        prev_offset = offset;
    }

    let footer_len = buf.len() as u32;
    footer_len.serialize(&mut buf)?;

    let mut footer = build_tantivy_footer(&buf)?;
    buf.append(&mut footer);

    Ok(OwnedBytes::new(buf))
}

#[derive(Clone)]
pub struct InvertedIndexMeta {
    pub columns: Vec<(String, SingleColumnMeta)>,
}

#[derive(Clone, Debug)]
pub struct InvertedIndexFile {
    pub name: String,
    pub data: OwnedBytes,
}

impl InvertedIndexFile {
    pub fn try_create(name: String, data: Vec<u8>) -> Result<Self> {
        let data = OwnedBytes::new(data);
        Ok(Self { name, data })
    }
}

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
    meta_path: PathBuf,
    managed_path: PathBuf,

    fast_data: OwnedBytes,
    store_data: OwnedBytes,
    fieldnorm_data: OwnedBytes,
    pos_data: OwnedBytes,
    idx_data: OwnedBytes,
    term_data: OwnedBytes,
    meta_data: OwnedBytes,
    managed_data: OwnedBytes,
}

impl InvertedIndexDirectory {
    pub fn try_create(field_nums: usize, files: Vec<Arc<InvertedIndexFile>>) -> Result<Self> {
        let mut file_map = BTreeMap::<String, OwnedBytes>::new();

        for file in files.into_iter() {
            let file = Arc::unwrap_or_clone(file.clone());
            if file.data.is_empty() {
                continue;
            }
            file_map.insert(file.name, file.data);
        }

        let fast_data = file_map.remove("fast").unwrap();
        let store_data = file_map.remove("store").unwrap();
        let fieldnorm_data = file_map.remove("fieldnorm").unwrap();
        // If there are no phrase terms in the query,
        // we can use empty position data instead.
        let pos_data = match file_map.remove("pos") {
            Some(pos_data) => pos_data,
            None => build_empty_position_data(field_nums)?,
        };
        let idx_data = file_map.remove("idx").unwrap();
        let term_data = file_map.remove("term").unwrap();
        let meta_data = file_map.remove("meta.json").unwrap();
        let managed_data = file_map.remove(".managed.json").unwrap();

        let meta_path = PathBuf::from("meta.json");
        let managed_path = PathBuf::from(".managed.json");

        Ok(Self {
            meta_path,
            managed_path,

            fast_data,
            store_data,
            fieldnorm_data,
            pos_data,
            idx_data,
            term_data,
            meta_data,
            managed_data,
        })
    }

    pub fn size(&self) -> usize {
        self.fast_data.len()
            + self.store_data.len()
            + self.fieldnorm_data.len()
            + self.pos_data.len()
            + self.idx_data.len()
            + self.term_data.len()
            + self.meta_data.len()
            + self.managed_data.len()
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
            return Ok(FileSlice::new(Arc::new(self.meta_data.clone())));
        } else if path == self.managed_path.as_path() {
            return Ok(FileSlice::new(Arc::new(self.managed_data.clone())));
        }

        if let Some(ext) = path.extension() {
            let bytes = match ext.to_str() {
                Some("term") => self.term_data.clone(),
                Some("idx") => self.idx_data.clone(),
                Some("pos") => self.pos_data.clone(),
                Some("fieldnorm") => self.fieldnorm_data.clone(),
                Some("store") => self.store_data.clone(),
                Some("fast") => self.fast_data.clone(),
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

impl Versioned<0> for InvertedIndexFile {}

pub enum InvertedIndexFileVersion {
    V0(PhantomData<InvertedIndexFile>),
}

impl TryFrom<u64> for InvertedIndexFileVersion {
    type Error = ErrorCode;
    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(InvertedIndexFileVersion::V0(testify_version::<_, 0>(
                PhantomData,
            ))),
            _ => Err(ErrorCode::Internal(format!(
                "unknown inverted index file version {value}, versions supported: 0"
            ))),
        }
    }
}
