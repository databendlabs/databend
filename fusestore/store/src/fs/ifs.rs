// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::exception;

use crate::fs::ListResult;

/// Abstract storage layer API.
#[async_trait]
pub trait IFileSystem
where Self: Sync + Send
{
    /// Add file atomically.
    /// AKA put_if_absent
    async fn add(&self, path: &str, data: &[u8]) -> anyhow::Result<()>;

    /// read all bytes from a file
    async fn read_all(&self, path: &str) -> exception::Result<Vec<u8>>;

    /// List dir and returns directories and files.
    async fn list(&self, prefix: &str) -> anyhow::Result<ListResult>;

    // async fn read(
    //     path: &str,
    //     offset: usize,
    //     length: usize,
    //     buf: &mut [u8],
    // ) -> anyhow::Result<usize>;
}
