// Copyright 2021 Datafuse Labs.
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

use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_compat::CompatExt;
use async_trait::async_trait;
use tokio::fs;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

use crate::error::Error;
use crate::error::Result;
use crate::ops::OpDelete;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::Object;
use crate::Reader;

#[derive(Default)]
pub struct Builder {
    root: Option<String>,
}

impl Builder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());

        self
    }

    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        // Make `/` as the default of root.
        let root = self.root.clone().unwrap_or_else(|| "/".to_string());

        // If root dir is not exist, we must create it.
        if let Err(e) = fs::metadata(&root).await {
            if e.kind() == std::io::ErrorKind::NotFound {
                fs::create_dir_all(&root)
                    .await
                    .map_err(|e| parse_io_error(&e, PathBuf::from(&root).as_path()))?;
            }
        }

        Ok(Arc::new(Backend { root }))
    }
}

pub struct Backend {
    root: String,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }
}

#[async_trait]
impl Accessor for Backend {
    async fn read(&self, args: &OpRead) -> Result<Reader> {
        let path = PathBuf::from(&self.root).join(&args.path);

        let mut f = fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(|e| parse_io_error(&e, &path))?;

        if let Some(offset) = args.offset {
            f.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| parse_io_error(&e, &path))?;
        };

        let f: Reader = match args.size {
            Some(size) => Box::new(f.take(size).compat()),
            None => Box::new(f.compat()),
        };

        Ok(f)
    }

    async fn write(&self, mut r: Reader, args: &OpWrite) -> Result<usize> {
        let path = PathBuf::from(&self.root).join(&args.path);

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
        let parent = path
            .parent()
            .ok_or_else(|| Error::Unexpected(format!("malformed path: {:?}", path.to_str())))?;
        fs::create_dir_all(parent)
            .await
            .map_err(|e| parse_io_error(&e, parent))?;

        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| parse_io_error(&e, &path))?;

        // TODO: we should respect the input size.
        let s = io::copy(&mut r.compat_mut(), &mut f)
            .await
            .map_err(|e| parse_io_error(&e, &path))?;

        // `std::fs::File`'s errors detected on closing are ignored by
        // the implementation of Drop.
        // So we need to call `sync_all` to make sure all internal metadata
        // have been flushed to fs successfully.
        f.sync_all().await.map_err(|e| parse_io_error(&e, &path))?;

        Ok(s as usize)
    }

    async fn stat(&self, args: &OpStat) -> Result<Object> {
        let path = PathBuf::from(&self.root).join(&args.path);

        let meta = fs::metadata(&path)
            .await
            .map_err(|e| parse_io_error(&e, &path))?;
        let o = Object {
            path: path.to_string_lossy().into_owned(),
            size: meta.len(),
        };

        Ok(o)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = PathBuf::from(&self.root).join(&args.path);

        // PathBuf.is_dir() is not free, call metadata directly instead.
        let meta = fs::metadata(&path).await;

        if let Err(err) = &meta {
            if err.kind() == std::io::ErrorKind::NotFound {
                return Ok(());
            }
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let f = if meta.is_dir() {
            fs::remove_dir(&path).await
        } else {
            fs::remove_file(&path).await
        };

        f.map_err(|e| parse_io_error(&e, &path))
    }
}

/// Parse all path related errors.
///
/// ## Notes
///
/// Skip utf-8 check to allow invalid path input.
fn parse_io_error(err: &std::io::Error, path: &Path) -> Error {
    use std::io::ErrorKind;

    match err.kind() {
        ErrorKind::NotFound => Error::ObjectNotExist(path.to_string_lossy().into_owned()),
        ErrorKind::PermissionDenied => Error::PermissionDenied(path.to_string_lossy().into_owned()),
        _ => Error::Unexpected(err.to_string()),
    }
}
