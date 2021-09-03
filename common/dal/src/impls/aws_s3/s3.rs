//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::io::Error;

use common_exception::ErrorCode;
use futures::Stream;
use futures::StreamExt;
use rusoto_core::ByteStream;
use rusoto_core::Region;
use rusoto_s3::GetObjectRequest;
use rusoto_s3::PutObjectRequest;
use rusoto_s3::S3Client;
use rusoto_s3::S3 as RusotoS3;
use tokio::io::AsyncReadExt;

use crate::impls::aws_s3::s3_input_stream::S3InputStream;
use crate::Bytes;
use crate::DataAccessor;

pub struct S3 {
    client: S3Client,
    bucket: String,
}

impl S3 {
    pub fn new(region: Region, bucket: String) -> Self {
        let client = S3Client::new(region);
        S3 { client, bucket }
    }

    async fn put_byte_stream(
        &self,
        path: &str,
        input_stream: ByteStream,
    ) -> common_exception::Result<()> {
        let req = PutObjectRequest {
            key: path.to_string(),
            bucket: self.bucket.to_string(),
            body: Some(input_stream),
            ..Default::default()
        };
        self.client
            .put_object(req)
            .await
            .map_err(|e| ErrorCode::DALTransportError(e.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl DataAccessor for S3 {
    type InputStream = S3InputStream;

    async fn get_input_stream(
        &self,
        path: &str,
        stream_len: Option<u64>,
    ) -> common_exception::Result<Self::InputStream> {
        Ok(S3InputStream::new(
            &self.client,
            &self.bucket,
            path,
            stream_len,
        ))
    }

    async fn get(&self, path: &str) -> common_exception::Result<Bytes> {
        let req = GetObjectRequest {
            key: path.to_string(),
            bucket: self.bucket.to_string(),
            ..Default::default()
        };
        let output = self
            .client
            .get_object(req)
            .await
            .map_err(|e| ErrorCode::DALTransportError(e.to_string()))?;
        match output.body {
            Some(stream) => {
                let mut res = vec![];
                stream.into_async_read().read_to_end(&mut res).await?;
                Ok(res)
            }
            None => Ok(Vec::new()),
        }
    }

    async fn put(&self, path: &str, content: Vec<u8>) -> common_exception::Result<()> {
        self.put_byte_stream(path, ByteStream::from(content)).await
    }

    async fn put_stream<S>(
        &self,
        path: &str,
        input_stream: S,
        stream_len: usize,
    ) -> common_exception::Result<()>
    where
        S: Stream<Item = Result<Bytes, Error>> + Send + 'static,
    {
        let s = input_stream.map(|bytes| bytes.map(|b| bytes::Bytes::copy_from_slice(&b)));
        self.put_byte_stream(path, ByteStream::new_with_size(s, stream_len))
            .await
    }
}
