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

use std::str::FromStr;

use common_exception::ErrorCode;
use common_exception::Result;
use futures::Stream;
use rusoto_core::credential::DefaultCredentialsProvider;
use rusoto_core::credential::StaticProvider;
use rusoto_core::ByteStream;
use rusoto_core::Client;
use rusoto_core::HttpClient;
use rusoto_core::Region;
use rusoto_s3::PutObjectRequest;
use rusoto_s3::S3Client;
use rusoto_s3::S3 as RusotoS3;

use crate::DataAccessor;
use crate::InputStream;
use crate::S3InputStream;

pub struct S3 {
    client: S3Client,
    bucket: String,
}

impl S3 {
    pub fn try_create(
        region_name: &str,
        endpoint_url: &str,
        bucket: &str,
        access_key_id: &str,
        secret_accesses_key: &str,
    ) -> Result<Self> {
        let region = Self::parse_region(region_name, endpoint_url)?;

        let dispatcher = HttpClient::new().map_err(|e| {
            ErrorCode::DALTransportError(format!("failed to create http client of s3, {}", e))
        })?;

        let client = match Self::credential_provider(access_key_id, secret_accesses_key) {
            Some(provider) => Client::new_with(provider, dispatcher),
            None => {
                // check on k8s admission webhook injection
                if std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE").is_ok() {
                    let provider = rusoto_sts::WebIdentityProvider::from_k8s_env();
                    let provider = rusoto_credential::AutoRefreshingProvider::new(provider)
                        .map_err(|e| {
                            ErrorCode::DALTransportError(format!(
                                "failed to create Web Identity credential provider of s3, {}",
                                e
                            ))
                        })?;
                    Client::new_with(provider, dispatcher)
                } else {
                    // Otherwise, return the default.
                    Client::new_with(
                        DefaultCredentialsProvider::new().map_err(|e| {
                            ErrorCode::DALTransportError(format!(
                                "failed to create default credentials provider, {}",
                                e
                            ))
                        })?,
                        dispatcher,
                    )
                }
            }
        };

        let s3_client = S3Client::new_with_client(client, region);
        Ok(S3 {
            client: s3_client,
            bucket: bucket.to_owned(),
        })
    }

    fn parse_region(name: &str, endpoint: &str) -> Result<Region> {
        if endpoint.is_empty() {
            Region::from_str(name).map_err(|e| {
                ErrorCode::DALTransportError(format!(
                    "invalid region {}, error details {}",
                    name, e
                ))
            })
        } else {
            Ok(Region::Custom {
                name: name.to_string(),
                endpoint: endpoint.to_string(),
            })
        }
    }

    fn credential_provider(key_id: &str, secret: &str) -> Option<StaticProvider> {
        if key_id.is_empty() {
            None
        } else {
            Some(StaticProvider::new(
                key_id.to_owned(),
                secret.to_owned(),
                None,
                None,
            ))
        }
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
    fn get_input_stream(
        &self,
        path: &str,
        stream_len: Option<u64>,
    ) -> common_exception::Result<InputStream> {
        Ok(Box::new(S3InputStream::new(
            &self.client,
            &self.bucket,
            path,
            stream_len,
        )))
    }

    async fn put(&self, path: &str, content: Vec<u8>) -> common_exception::Result<()> {
        self.put_byte_stream(path, ByteStream::from(content)).await
    }

    async fn put_stream(
        &self,
        path: &str,
        input_stream: Box<
            dyn Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>>
                + Send
                + Unpin
                + 'static,
        >,
        stream_len: usize,
    ) -> common_exception::Result<()> {
        self.put_byte_stream(path, ByteStream::new_with_size(input_stream, stream_len))
            .await
    }
}
