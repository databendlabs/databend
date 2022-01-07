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

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use aws_sdk_s3 as AwsS3;
use futures::TryStreamExt;

use crate::credential::Credential;
use crate::error::Error;
use crate::error::Result;
use crate::ops::Read;
use crate::ops::ReadBuilder;
use crate::ops::Reader;

#[derive(Default, Debug, Clone)]
pub struct Builder {
    root: Option<String>,

    bucket: String,
    region: String,
    credential: Option<Credential>,
    endpoint: Option<String>,

    /// disable_ssl controls whether to use SSL when connecting.
    disable_ssl: bool,
    /// enable_path_style controls whether to use path style or virtual host style.
    ///
    /// ## TODO
    ///
    /// It seems sdk doesn't support path style so far.
    enable_path_style: bool,
    /// enable_signature_v2 whether to use signature v2 or not.
    ///
    /// ## TODO
    ///
    /// It seems sdk doesn't support path style so far.
    enable_signature_v2: bool,
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

    pub fn disable_ssl(&mut self) -> &mut Self {
        self.disable_ssl = true;

        self
    }

    pub fn enable_path_style(&mut self) -> &mut Self {
        self.enable_path_style = true;

        self
    }

    pub fn enable_signature_v2(&mut self) -> &mut Self {
        self.enable_signature_v2 = true;

        self
    }

    pub async fn finish(&mut self) -> Result<Backend> {
        use aws_endpoint::partition::endpoint::Metadata;
        use aws_endpoint::partition::endpoint::Protocol;
        use aws_endpoint::partition::endpoint::SignatureVersion;

        if self.bucket.is_empty() || self.region.is_empty() {
            return Err(Error::BackendConfigurationInvalid {
                key: "bucket".to_string(),
                value: "".to_string(),
            });
        }

        // strip the prefix of "/" in root only once.
        let root = if let Some(root) = &self.root {
            root.strip_prefix('/').unwrap_or(&root).to_string()
        } else {
            String::new()
        };

        // Load from runtime env as default.
        let aws_cfg = aws_config::load_from_env().await;

        let mut cfg = AwsS3::config::Builder::from(&aws_cfg);

        // ## Safety
        //
        // aws s3 sdk requires a static str for region, so we crate a static string here.
        // This value is scoped under `finish` function only, changed once and used
        // once while initializing the client, so it must be safe.
        static mut REGION: String = String::new();
        unsafe { REGION = self.region.clone() };
        // TODO: Maybe we can
        //
        // - use "default" as the default region.
        // - use "us-east-1" as the default region.
        // - detect the region at runtime via `ListBuckets`.
        cfg = cfg.region(AwsS3::Region::new(unsafe { REGION.as_str() }));

        // ## Safety
        //
        // aws s3 sdk requires a static str for uri_template, so we crate a static string here.
        // This value is scoped under `finish` function only, changed once and used
        // once while initializing the client, so it must be safe.
        // The default value for it is "https://s3.{region}.amazonaws.com".
        static mut URI_TEMPLATE: String = String::new();
        unsafe {
            URI_TEMPLATE = match &self.endpoint {
                Some(v) => v.clone(),
                None => "https://s3.{region}.amazonaws.com".to_string(),
            };
        }

        let endpoint_metadata = Metadata {
            uri_template: unsafe { URI_TEMPLATE.as_str() },
            protocol: if self.disable_ssl {
                Protocol::Http
            } else {
                Protocol::Https
            },
            signature_versions: SignatureVersion::V4,
            credential_scope: aws_endpoint::CredentialScope::builder().build(),
        };

        cfg = cfg.endpoint_resolver(aws_endpoint::PartitionResolver::new(
            aws_endpoint::Partition::builder()
                // Use the same value from aws sdk.
                .id("aws")
                // Capture all valid ascii strings.
                .region_regex(r#"[\w\d-]+"#)
                .default_endpoint(endpoint_metadata)
                .build()
                .expect("invalid partition"),
            vec![],
        ));

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

        Ok(Backend {
            // Make `/` as the default of root.
            root,
            bucket: self.bucket.clone(),
            client: AwsS3::Client::from_conf(cfg.build()),
        })
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
            path.strip_prefix('/').unwrap().to_string()
        } else {
            format!("{}/{}", self.root, path)
        }
    }
}

#[async_trait]
impl<S: Send + Sync> Read<S> for Backend {
    async fn read(&self, args: &ReadBuilder<S>) -> Result<Reader> {
        let p = self.get_abs_path(args.path);

        // TODO: Handle range header here.
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket.clone())
            .key(&p)
            .send()
            .await
            .unwrap(); // TODO: we need a better way to handle errors here.

        Ok(Box::new(S3Stream(resp.body).into_async_read()))
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
