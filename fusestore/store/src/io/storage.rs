// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-Lise-Identifier: Apache-2.0.

use std::path::Path;

use anyhow::Result;
use async_trait::async_trait;
use tokio::io::AsyncRead;
use tokio::io::AsyncSeek;
use tokio::io::AsyncWrite;

pub trait AsyncSeekableRead: AsyncRead + AsyncSeek {}

// TODO
// 1. depends on std::path::Path is no a good idea
// 2. replicated FileSystem

///
///
///

#[async_trait]
pub trait FS: Send + Sync {
    /// MUST be atomic
    async fn put_if_absence(&self, path: &Path, name: &str, content: &[u8]) -> Result<()>;

    /// concurrent write to the same path is undefined behavior, caller should provide a unique path
    async fn writer(&self, path: &Path) -> Result<Box<dyn AsyncWrite + Unpin>>;
    async fn reader(&self, path: &Path) -> Result<Box<dyn AsyncRead>>;
    async fn seekable_reader(&self, path: &Path) -> Result<Box<dyn AsyncSeekableRead>>;
}
