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

use std::fs;
use std::io;
use std::io::Seek;
use std::io::Write;

use common_meta_types::SnapshotMeta;

use crate::sm_v002::snapshot_store::SnapshotStoreV002;

pub struct WriterV002<'a> {
    /// The temp path to write to, which will be renamed to the final path.
    /// So that the readers could only see a complete snapshot.
    temp_path: String,

    inner: fs::File,

    meta: SnapshotMeta,

    // Keep a mutable ref so that there could only be one writer at a time.
    snapshot_store: &'a mut SnapshotStoreV002,
}

impl<'a> io::Write for WriterV002<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'a> WriterV002<'a> {
    /// Create a singleton writer for the snapshot.
    pub fn new(
        snapshot_store: &'a mut SnapshotStoreV002,
        meta: SnapshotMeta,
    ) -> Result<Self, io::Error> {
        let temp_path = snapshot_store.snapshot_temp_path();

        let f = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&temp_path)?;

        let writer = WriterV002 {
            temp_path,
            inner: f,
            meta,
            snapshot_store,
        };

        Ok(writer)
    }

    /// Commit the snapshot so that it is visible to the readers.
    ///
    /// Returns the file size written.
    pub fn commit(&mut self) -> Result<u64, io::Error> {
        self.inner.flush()?;
        self.inner.sync_all()?;

        let file_size = self.inner.seek(io::SeekFrom::End(0))?;

        let path = self.snapshot_store.snapshot_path(&self.meta.snapshot_id);

        fs::rename(&self.temp_path, &path)?;

        Ok(file_size)
    }
}
