// Copyright 2024 RisingWave Labs
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

//! This module provides an in-memory file that implements the `WasiFile` trait.

use std::{
    any::Any,
    ops::Deref,
    sync::{Arc, Mutex},
};

use wasi_common::{file::FileType, Error, ErrorExt, WasiFile};

pub struct RamFile {
    data: Mutex<Vec<u8>>,
    /// Maximum size of the file.
    size_limit: usize,
}

impl RamFile {
    /// Create a new file with the given size limit.
    pub fn with_size_limit(size_limit: usize) -> Self {
        RamFile {
            data: Default::default(),
            size_limit,
        }
    }

    /// Take the file's contents.
    pub fn take(&self) -> Vec<u8> {
        let mut data = self.data.lock().unwrap();
        std::mem::take(&mut *data)
    }
}

#[async_trait::async_trait]
impl WasiFile for RamFile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn get_filetype(&self) -> Result<FileType, Error> {
        Ok(FileType::RegularFile)
    }

    async fn writable(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write_vectored<'a>(&self, bufs: &[std::io::IoSlice<'a>]) -> Result<u64, Error> {
        let mut data = self.data.lock().unwrap();
        let written: u64 = bufs.iter().map(|buf| buf.len() as u64).sum();
        if data.len() + written as usize > self.size_limit {
            return Err(Error::overflow());
        }
        for buf in bufs {
            data.extend_from_slice(buf);
        }
        Ok(written)
    }
}

/// A reference-count to a `RamFile`.
#[derive(Clone)]
pub struct RamFileRef(Arc<RamFile>);

impl RamFileRef {
    /// Create a new reference-count to a `RamFile`.
    pub fn new(file: RamFile) -> Self {
        RamFileRef(Arc::new(file))
    }
}

impl Deref for RamFileRef {
    type Target = RamFile;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait::async_trait]
impl WasiFile for RamFileRef {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn get_filetype(&self) -> Result<FileType, Error> {
        Ok(FileType::RegularFile)
    }

    async fn writable(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write_vectored<'a>(&self, bufs: &[std::io::IoSlice<'a>]) -> Result<u64, Error> {
        self.0.write_vectored(bufs).await
    }
}
