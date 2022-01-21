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

use async_compat::CompatExt;
use async_trait::async_trait;
use tokio::fs;
use tokio::io;
use tokio::io::AsyncSeekExt;

use crate::error::Error;
use crate::error::Result;
use crate::ops::Delete;
use crate::ops::Object;
use crate::ops::Read;
use crate::ops::ReadBuilder;
use crate::ops::Reader;
use crate::ops::Stat;
use crate::ops::Write;
use crate::ops::WriteBuilder;

#[derive(Default)]
pub struct Builder {
    root: Option<String>,
}

impl Builder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());

        self
    }

    pub fn finish(self) -> Backend {
        Backend {
            // Make `/` as the default of root.
            root: self.root.unwrap_or_else(|| "/".to_string()),
        }
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
impl<S: Send + Sync> Read<S> for Backend {
    async fn read(&self, args: &ReadBuilder<S>) -> Result<Reader> {
        let path = PathBuf::from(&self.root).join(args.path);

        let mut f = fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(|e| parse_io_error(&e, &path))?;

        if let Some(offset) = args.offset {
            f.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| parse_io_error(&e, &path))?;
        }

        // TODO: we need to respect input size.
        Ok(Box::new(f.compat()))
    }
}

#[async_trait]
impl<S: Send + Sync> Write<S> for Backend {
    async fn write(&self, mut r: Reader, args: &WriteBuilder<S>) -> Result<usize> {
        let path = PathBuf::from(&self.root).join(args.path);

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

        Ok(s as usize)
    }
}

#[async_trait]
impl<S: Send + Sync> Stat<S> for Backend {
    async fn stat(&self, path: &str) -> Result<Object> {
        let path = PathBuf::from(&self.root).join(path);

        let meta = fs::metadata(&path)
            .await
            .map_err(|e| parse_io_error(&e, &path))?;
        let o = Object {
            path: path.to_string_lossy().into_owned(),
            size: meta.len(),
        };

        Ok(o)
    }
}

#[async_trait]
impl<S: Send + Sync> Delete<S> for Backend {
    async fn delete(&self, path: &str) -> Result<()> {
        let path = PathBuf::from(&self.root).join(path);

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
