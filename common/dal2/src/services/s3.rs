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

use std::borrow::Cow;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use aws_sdk_s3 as AwsS3;
use aws_smithy_http::body::SdkBody;
use aws_smithy_http::byte_stream::ByteStream;
use futures::TryStreamExt;

use crate::credential::Credential;
use crate::error::Error;
use crate::error::Result;
use crate::ops::HeaderRange;
use crate::ops::OpDelete;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::readers::ReaderStream;
use crate::Accessor;
use crate::Object;
use crate::Reader;

/// # TODO
///
/// enable_path_style and enable_signature_v2 need sdk support.
///
/// ref: https://github.com/awslabs/aws-sdk-rust/issues/390
#[derive(Default, Debug, Clone)]
pub struct Builder {
    root: Option<String>,

    bucket: String,
    region: String,
    credential: Option<Credential>,
    /// endpoint must be full uri or a uri template, e.g.
    /// - https://s3.amazonaws.com
    /// - http://127.0.0.1:3000
    endpoint: Option<String>,
}

impl Builder {
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());

        self
    }

    pub fn bucket(&mut self, bucket: &str) -> &mut Self {
        self.bucket = bucket.to_string();

        self
    }

    pub fn region(&mut self, region: &str) -> &mut Self {
        self.region = region.to_string();

        self
    }

    pub fn credential(&mut self, credential: Credential) -> &mut Self {
        self.credential = Some(credential);

        self
    }

    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = Some(endpoint.to_string());

        self
    }

    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        if self.bucket.is_empty() || self.region.is_empty() {
            return Err(Error::BackendConfigurationInvalid {
                key: "bucket".to_string(),
                value: "".to_string(),
            });
        }

        // strip the prefix of "/" in root only once.
        let root = if let Some(root) = &self.root {
            root.strip_prefix('/').unwrap_or(root).to_string()
        } else {
            String::new()
        };

        // Load config from environment, including:
        // - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION
        // - The default credentials files located in ~/.aws/config and ~/.aws/credentials (location can vary per platform)
        // - Web Identity Token credentials from the environment or container (including EKS)
        // - ECS Container Credentials (IAM roles for tasks)
        // - EC2 Instance Metadata Service (IAM Roles attached to instance)
        let cfg = aws_config::load_from_env().await;

        let mut cfg = AwsS3::config::Builder::from(&cfg);

        // TODO: Maybe we can
        //
        // - use "default" as the default region.
        // - use "us-east-1" as the default region.
        // - detect the region at runtime via `ListBuckets`.
        cfg = cfg.region(AwsS3::Region::new(Cow::from(self.region.clone())));

        // Load users input first, if user not input, we will fallback to aws
        // default load logic.
        if let Some(endpoint) = &self.endpoint {
            cfg = cfg.endpoint_resolver(AwsS3::Endpoint::immutable(
                http::Uri::from_str(endpoint).map_err(|_| Error::BackendConfigurationInvalid {
                    key: "endpoint".to_string(),
                    value: endpoint.clone(),
                })?,
            ));
        }

        // Load users input first, if user not input, we will fallback to aws
        // default load logic.
        if let Some(cred) = &self.credential {
            match cred {
                Credential::HMAC {
                    access_key_id,
                    secret_access_key,
                } => {
                    cfg = cfg.credentials_provider(AwsS3::Credentials::from_keys(
                        access_key_id,
                        secret_access_key,
                        None,
                    ));
                }
                _ => {
                    return Err(Error::BackendConfigurationInvalid {
                        key: "credential".to_string(),
                        value: "".to_string(),
                    })
                }
            }
        }

        Ok(Arc::new(Backend {
            // Make `/` as the default of root.
            root,
            bucket: self.bucket.clone(),
            client: AwsS3::Client::from_conf(cfg.build()),
        }))
    }
}

pub struct Backend {
    bucket: String,

    client: AwsS3::Client,
    root: String,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }

    /// get_abs_path will return the absolute path of the given path in the s3 format.
    /// If user input an absolute path, we will return it as it is with the prefix `/` striped.
    /// If user input a relative path, we will calculate the absolute path with the root.
    fn get_abs_path(&self, path: &str) -> String {
        if path.starts_with('/') {
            return path.strip_prefix('/').unwrap().to_string();
        }
        if self.root.is_empty() {
            return path.to_string();
        }

        format!("{}/{}", self.root, path)
    }
}

#[async_trait]
impl Accessor for Backend {
    async fn read(&self, args: &OpRead) -> Result<Reader> {
        let p = self.get_abs_path(&args.path);

        let mut req = self
            .client
            .get_object()
            .bucket(&self.bucket.clone())
            .key(&p);

        if args.offset.is_some() || args.size.is_some() {
            req = req.range(HeaderRange::new(args.offset, args.size).to_string());
        }

        // TODO: we need a better way to handle errors here.
        let resp = req.send().await.unwrap();

        Ok(Box::new(S3Stream(resp.body).into_async_read()))
    }

    async fn write(&self, r: Reader, args: &OpWrite) -> Result<usize> {
        let p = self.get_abs_path(&args.path);

        let _ = self
            .client
            .put_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .content_length(args.size as i64)
            .body(ByteStream::from(SdkBody::from(
                hyper::body::Body::wrap_stream(ReaderStream::new(r)),
            )))
            .send()
            .await
            .unwrap(); // TODO: we need a better way to handle errors here.

        Ok(args.size as usize)
    }

    async fn stat(&self, args: &OpStat) -> Result<Object> {
        let p = self.get_abs_path(&args.path);

        let meta = self
            .client
            .head_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .send()
            .await
            .unwrap(); // TODO: we need a better way to handle errors here.
        let o = Object {
            path: args.path.to_string(),
            size: meta.content_length as u64,
        };

        Ok(o)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let p = self.get_abs_path(&args.path);

        let _ = self
            .client
            .delete_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .send()
            .await
            .unwrap(); // TODO: we need a better way to handle errors here.

        Ok(())
    }
}

struct S3Stream(aws_smithy_http::byte_stream::ByteStream);

impl futures::Stream for S3Stream {
    type Item = std::result::Result<bytes::Bytes, std::io::Error>;

    /// ## TODO
    ///
    /// This hack is ugly, we should find a better way to do this.
    ///
    /// The problem is `into_async_read` requires the stream returning
    /// `std::io::Error`, the the `ByteStream` returns
    /// `aws_smithy_http::byte_stream::Error` instead.
    ///
    /// I don't know why aws sdk should wrap the error into their own type...
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_next(cx)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
