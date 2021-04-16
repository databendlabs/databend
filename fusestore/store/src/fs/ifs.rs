// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::path::Path;

use async_trait::async_trait;

use crate::fs::ListResult;

/// Abstract storage layer API.
#[async_trait]
pub trait IFileSystem {
    /// Add file atomically.
    /// AKA put_if_absent
    async fn add<'a>(
        &'a self,
        path: impl AsRef<Path> + Send + 'a,
        data: &[u8],
    ) -> anyhow::Result<()>;

    /// read all bytes from a file
    async fn read_all<'a>(&'a self, path: impl AsRef<Path> + Send + 'a) -> anyhow::Result<Vec<u8>>;

    /// List dir and returns directories and files.
    async fn list<'a>(&'a self, path: impl AsRef<Path> + Send + 'a) -> anyhow::Result<ListResult>;

    // async fn read(
    //     path: &str,
    //     offset: usize,
    //     length: usize,
    //     buf: &mut [u8],
    // ) -> anyhow::Result<usize>;
    // async fn list(path: &str) -> anyhow::Result<Vec<String>>;
}
