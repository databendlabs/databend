// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::Result;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::blob_accessor::BlobAccessor;
use crate::blob_accessor::Bytes;

pub struct Local;

#[async_trait::async_trait]
impl BlobAccessor for Local {
    type InputStream = File;

    async fn get(&self, path: &str) -> Result<Bytes> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut contents = vec![];
        let _buffer = file.read_to_end(&mut contents).await?;
        Ok(contents)
    }

    async fn get_stream(&self, path: &str) -> Result<Self::InputStream> {
        Ok(tokio::fs::File::open(path).await?)
    }

    async fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        let mut new_file = tokio::fs::File::create(path).await?;
        new_file.write_all(content).await?;
        Ok(())
    }

    async fn put_stream<R: AsyncRead + Sync + Send + Unpin>(
        &self,
        path: &str,
        content_stream: &mut R,
    ) -> Result<()> {
        let mut new_file = tokio::fs::File::create(path).await?;
        tokio::io::copy(content_stream, &mut new_file).await?;
        Ok(())
    }
}
