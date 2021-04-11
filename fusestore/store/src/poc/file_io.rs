// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::path::Path;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use tokio::fs;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

use crate::io::AsyncSeekableRead;
use crate::io::FS;

struct Namtso {
    root: PathBuf,
    staging: PathBuf,
}

#[async_trait]
impl FS for Namtso {
    async fn put_if_absence(&self, path: &Path, name: &str, content: &[u8]) -> Result<()> {
        //assuming rename is atomic, (HDFS and some local file systems)

        let unique_name = uuid::Uuid::new_v4().to_simple().to_string();
        let staging_file = self.staging.join(unique_name);
        std::fs::write(&staging_file, content)?;

        let dest_file = self.root.join(path).join(name);
        std::fs::rename(&staging_file, dest_file)?;
        Ok(())
    }

    async fn writer(&self, path: &Path) -> Result<Box<dyn AsyncWrite + Unpin>> {
        let f = fs::File::create(path).await?;
        Ok(Box::new(f))
    }

    async fn reader(&self, path: &Path) -> Result<Box<dyn AsyncRead>> {
        let f = fs::File::create(path).await?;
        Ok(Box::new(f))
    }

    async fn seekable_reader(&self, path: &Path) -> Result<Box<dyn AsyncSeekableRead>> {
        let f = fs::File::create(path).await?;
        Ok(Box::new(f))
    }
}

impl AsyncSeekableRead for fs::File {}
