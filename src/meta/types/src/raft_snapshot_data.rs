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

use std::io;
use std::io::SeekFrom;
use std::path::Path;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use tokio::fs;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncSeek;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::ReadBuf;

/// A typed temporary snapshot data.
pub struct TempSnapshotData {
    inner: SnapshotData,
}

impl TempSnapshotData {
    pub fn new(snapshot_data: SnapshotData) -> Self {
        assert!(snapshot_data.is_temp());
        TempSnapshotData {
            inner: snapshot_data,
        }
    }

    /// Commit the temp snapshot to a final snapshot file.
    ///
    /// It requires the input snapshot is a temp snapshot.
    pub fn commit<P: AsRef<Path>>(self, final_path: P) -> Result<SnapshotData, io::Error> {
        let final_path = final_path.as_ref().to_string_lossy().to_string();

        std::fs::rename(self.inner.path(), &final_path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "{}: while TempSnapshotData::commit(); temp path: {}; final path: {}",
                    e,
                    self.path(),
                    final_path
                ),
            )
        })?;

        let d = SnapshotData::open(final_path)?;

        Ok(d)
    }

    pub fn into_inner(self) -> SnapshotData {
        self.inner
    }

    pub fn path(&self) -> &str {
        self.inner.path()
    }
}

pub struct SnapshotData {
    /// Whether it is a temp file that may contain partial data.
    is_temp: bool,
    path: String,
    f: fs::File,
}

impl SnapshotData {
    pub fn new(path: impl ToString, f: std::fs::File, is_temp: bool) -> Self {
        SnapshotData {
            is_temp,
            path: path.to_string(),
            f: fs::File::from_std(f),
        }
    }

    /// Open an existing snapshot file as a temp snapshot.
    pub fn open_temp(path: String) -> Result<Self, io::Error> {
        let mut d = Self::open(path)?;
        d.is_temp = true;
        Ok(d)
    }

    pub fn open(path: String) -> Result<Self, io::Error> {
        let f = std::fs::OpenOptions::new()
            .create(false)
            .create_new(false)
            .read(true)
            .open(&path)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}: while open(); path: {}", e, path))
            })?;

        Ok(SnapshotData {
            is_temp: false,
            path,
            f: fs::File::from_std(f),
        })
    }

    pub async fn new_temp(path: String) -> Result<Self, io::Error> {
        let f = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .await
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}: while new_temp(); path: {}", e, path))
            })?;

        Ok(SnapshotData {
            is_temp: true,
            path,
            f,
        })
    }

    pub async fn data_size(&self) -> Result<u64, io::Error> {
        self.f.metadata().await.map(|m| m.len()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("{}: while data_size(); path: {}", e, self.path),
            )
        })
    }

    pub fn is_temp(&self) -> bool {
        self.is_temp
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub async fn into_std(self) -> std::fs::File {
        self.f.into_std().await
    }

    pub async fn sync_all(&mut self) -> Result<(), io::Error> {
        self.f.flush().await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("{}: while flush(); path: {}", e, self.path),
            )
        })?;

        self.f.sync_all().await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("{}: while sync_all(); path: {}", e, self.path),
            )
        })
    }

    pub async fn read_to_lines(self: Box<SnapshotData>) -> Result<Vec<String>, io::Error> {
        let mut res = vec![];

        let path = self.path.clone();

        let b = BufReader::new(self);
        let mut lines = AsyncBufReadExt::lines(b);

        while let Some(l) = lines.next_line().await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("{}: while read_to_lines(); path: {}", e, path),
            )
        })? {
            res.push(l)
        }

        Ok(res)
    }
}

impl AsyncRead for SnapshotData {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().f).poll_read(cx, buf)
    }
}

impl AsyncWrite for SnapshotData {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.get_mut().f).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().f).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().f).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotData {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.get_mut().f).start_seek(position)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.get_mut().f).poll_complete(cx)
    }
}
