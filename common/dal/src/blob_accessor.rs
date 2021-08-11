// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::Result;
use tokio::io::AsyncRead;
use tokio::io::AsyncSeek;

pub type Bytes = Vec<u8>;

// TODO Add Send, Sync, or maybe 'static here and there

#[async_trait::async_trait]
pub trait BlobAccessor {
    type InputStream: AsyncRead + AsyncSeek;

    async fn get(&self, path: &str) -> Result<Bytes>;

    async fn get_stream(&self, path: &str) -> Result<Self::InputStream>;

    async fn put(&self, path: &str, content: &[u8]) -> Result<()>;
    async fn put_stream<R: AsyncRead + Sync + Send + Unpin>(
        &self,
        path: &str,
        content_stream: &mut R,
    ) -> Result<()>;
}
