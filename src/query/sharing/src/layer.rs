// Copyright 2022 Datafuse Labs.
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

use std::fmt::Debug;
use std::io;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use base64::encode_config;
use http::Request;
use http::StatusCode;
use opendal::http_util::new_request_build_error;
use opendal::http_util::parse_content_length;
use opendal::http_util::parse_error_response;
use opendal::http_util::parse_etag;
use opendal::http_util::parse_last_modified;
use opendal::http_util::AsyncBody;
use opendal::http_util::ErrorResponse;
use opendal::http_util::HttpClient;
use opendal::ops::BytesRange;
use opendal::ops::OpRead;
use opendal::ops::OpStat;
use opendal::ops::Operation;
use opendal::ops::PresignedRequest;
use opendal::Accessor;
use opendal::AccessorCapability;
use opendal::AccessorMetadata;
use opendal::BytesReader;
use opendal::Layer;
use opendal::ObjectMetadata;
use opendal::ObjectMode;
use opendal::Operator;
use opendal::Scheme;
use reqwest::header::RANGE;
use thiserror::Error;

use crate::SharedSigner;

/// SharedLayer is used to handle databend cloud's sharing logic.
///
/// We will inject all read request to:
///
/// - Get presgined url from sharing endpoint.
/// - Read data from the url instead.
///
/// # Example:
///
/// ```no_build
/// use anyhow::Result;
/// use common_sharing::SharedLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Memory)
///     .expect("must init")
///     .layer(SharedLayer::new(signer));
/// ```
#[derive(Debug, Clone)]
pub struct SharedLayer {
    signer: SharedSigner,
}

impl SharedLayer {
    /// Create a new SharedLayer.
    pub fn new(signer: SharedSigner) -> Self {
        Self { signer }
    }
}

impl Layer for SharedLayer {
    fn layer(&self, _: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(SharedAccessor {
            signer: self.signer.clone(),
            client: HttpClient::new(),
        })
    }
}

pub fn create_share_table_operator(
    share_endpoint_address: Option<String>,
    from_tenant_id: &str,
    share_tenant_id: &str,
    share_name: &str,
    table_name: &str,
) -> Operator {
    match share_endpoint_address {
        Some(share_endpoint_address) => {
            let token = encode_config(from_tenant_id, base64::URL_SAFE);
            let signer = SharedSigner::new(
                &format!(
                    "http://{}/tenant/{}/{}/table/{}/presign",
                    share_endpoint_address, share_tenant_id, share_name, table_name
                ),
                &token,
            );
            Operator::from_env(Scheme::Memory)
                .expect("must init")
                .layer(SharedLayer::new(signer))
        }
        None => Operator::from_env(Scheme::Memory)
            .expect("must init")
            .layer(DummySharedLayer {}),
    }
}

#[derive(Debug)]
struct SharedAccessor {
    signer: SharedSigner,
    client: HttpClient,
}

#[async_trait]
impl Accessor for SharedAccessor {
    fn metadata(&self) -> AccessorMetadata {
        let mut meta = AccessorMetadata::default();
        meta.set_scheme(Scheme::Custom("shared"))
            .set_capabilities(AccessorCapability::Read);
        meta
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let req: PresignedRequest = self
            .signer
            .fetch(path, Operation::Read)
            .await
            .map_err(|err| Error::new(ErrorKind::Other, err))?;

        let mut req: Request<AsyncBody> = req.into();
        req.headers_mut().insert(
            RANGE,
            BytesRange::new(args.offset(), args.size())
                .to_string()
                .parse()
                .map_err(|err| Error::new(ErrorKind::Other, err))?,
        );

        let resp = self.client.send_async(req).await?;
        Ok(resp.into_body().reader())
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<ObjectMetadata> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(ObjectMetadata::new(ObjectMode::DIR));
        }
        let req: PresignedRequest = self
            .signer
            .fetch(path, Operation::Stat)
            .await
            .map_err(|err| Error::new(ErrorKind::Other, err))?;
        let req = Request::head(req.uri());
        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Stat, path, e))?;
        let resp = self.client.send_async(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let mode = if path.ends_with('/') {
                    ObjectMode::DIR
                } else {
                    ObjectMode::FILE
                };
                let mut m = ObjectMetadata::new(mode);
                if let Some(v) = parse_content_length(resp.headers())
                    .map_err(|e| new_other_object_error(Operation::Stat, path, e))?
                {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_etag(resp.headers())
                    .map_err(|e| new_other_object_error(Operation::Stat, path, e))?
                {
                    m.set_etag(v);
                    m.set_content_md5(v.trim_matches('"'));
                }

                if let Some(v) = parse_last_modified(resp.headers())
                    .map_err(|e| new_other_object_error(Operation::Stat, path, e))?
                {
                    m.set_last_modified(v);
                }
                Ok(m)
            }
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(ObjectMetadata::new(ObjectMode::DIR))
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Stat, path, er);
                Err(err)
            }
        }
    }
}

#[derive(Error, Debug)]
#[error("object error: (op: {op}, path: {path}, source: {source})")]
pub struct ObjectError {
    op: Operation,
    path: String,
    source: anyhow::Error,
}

impl ObjectError {
    pub fn new(op: Operation, path: &str, source: impl Into<anyhow::Error>) -> Self {
        ObjectError {
            op,
            path: path.to_string(),
            source: source.into(),
        }
    }
}

pub fn parse_error(op: Operation, path: &str, er: ErrorResponse) -> Error {
    let kind = match er.status_code() {
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::FORBIDDEN => ErrorKind::PermissionDenied,
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => ErrorKind::Interrupted,
        _ => ErrorKind::Other,
    };

    Error::new(kind, ObjectError::new(op, path, anyhow!("{er}")))
}

pub fn new_other_object_error(
    op: Operation,
    path: &str,
    source: impl Into<anyhow::Error>,
) -> Error {
    io::Error::new(io::ErrorKind::Other, ObjectError::new(op, path, source))
}

pub struct DummySharedLayer {}
impl Layer for DummySharedLayer {
    fn layer(&self, _: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(DummySharedAccessor {})
    }
}

// A dummy Accessor which cannot do anything.
#[derive(Debug)]
struct DummySharedAccessor {}

#[async_trait]
impl Accessor for DummySharedAccessor {}
