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

use async_trait::async_trait;
use backon::ExponentialBackoff;
use common_auth::RefreshableToken;
use http::Request;
use http::StatusCode;
use opendal::layers::LoggingLayer;
use opendal::layers::MetricsLayer;
use opendal::layers::RetryLayer;
use opendal::layers::TracingLayer;
use opendal::raw::apply_wrapper;
use opendal::raw::new_request_build_error;
use opendal::raw::parse_content_length;
use opendal::raw::parse_error_response;
use opendal::raw::parse_etag;
use opendal::raw::parse_last_modified;
use opendal::raw::Accessor;
use opendal::raw::AccessorCapability;
use opendal::raw::AccessorMetadata;
use opendal::raw::AsyncBody;
use opendal::raw::BytesReader;
use opendal::raw::ErrorResponse;
use opendal::raw::HttpClient;
use opendal::raw::Operation;
use opendal::raw::PresignedRequest;
use opendal::raw::RpRead;
use opendal::raw::RpStat;
use opendal::Error;
use opendal::ErrorKind;
use opendal::ObjectMetadata;
use opendal::ObjectMode;
use opendal::OpRead;
use opendal::OpStat;
use opendal::Operator;
use opendal::Result;
use opendal::Scheme;
use reqwest::header::RANGE;

use crate::SharedSigner;

pub fn create_share_table_operator(
    share_endpoint_address: Option<String>,
    share_endpoint_token: RefreshableToken,
    share_tenant_id: &str,
    share_name: &str,
    table_name: &str,
) -> Operator {
    let op = match share_endpoint_address {
        Some(share_endpoint_address) => {
            let signer = SharedSigner::new(
                &format!(
                    "http://{}/tenant/{}/{}/table/{}/presign",
                    share_endpoint_address, share_tenant_id, share_name, table_name
                ),
                share_endpoint_token,
            );
            Operator::new(apply_wrapper(SharedAccessor {
                signer,
                client: HttpClient::new(),
            }))
        }
        None => Operator::new(DummySharedAccessor {}),
    };

    op
        // Add retry
        .layer(RetryLayer::new(ExponentialBackoff::default().with_jitter()))
        // Add metrics
        .layer(MetricsLayer)
        // Add logging
        .layer(LoggingLayer::default())
        // Add tracing
        .layer(TracingLayer)
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

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let req: PresignedRequest =
            self.signer
                .fetch(path, Operation::Read)
                .await
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "fetch presigned url failed").set_source(err)
                })?;

        let br = args.range();
        let mut req: Request<AsyncBody> = req.into();
        req.headers_mut().insert(
            RANGE,
            br.to_header().parse().map_err(|err| {
                Error::new(ErrorKind::Unexpected, "header value is invalid").set_source(err)
            })?,
        );

        let resp = self.client.send_async(req).await?;

        if resp.status().is_success() {
            let content_length = parse_content_length(resp.headers())
                .unwrap()
                .expect("content_length must be valid");
            Ok((RpRead::new(content_length), resp.into_body().reader()))
        } else {
            let er = parse_error_response(resp).await?;
            let err = parse_error(er);
            Err(err)
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)));
        }
        let req: PresignedRequest =
            self.signer
                .fetch(path, Operation::Stat)
                .await
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "fetch presigned url failed").set_source(err)
                })?;
        let req = Request::head(req.uri());
        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
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
                if let Some(v) = parse_content_length(resp.headers())? {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_etag(resp.headers())? {
                    m.set_etag(v);
                    m.set_content_md5(v.trim_matches('"'));
                }

                if let Some(v) = parse_last_modified(resp.headers())? {
                    m.set_last_modified(v);
                }
                Ok(RpStat::new(m))
            }
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)))
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(er);
                Err(err)
            }
        }
    }
}

pub fn parse_error(er: ErrorResponse) -> Error {
    let (kind, retryable) = match er.status_code() {
        StatusCode::NOT_FOUND => (ErrorKind::ObjectNotFound, false),
        StatusCode::FORBIDDEN => (ErrorKind::ObjectPermissionDenied, false),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let mut err = Error::new(kind, &er.to_string());

    if retryable {
        err = err.set_temporary();
    }

    err
}

// A dummy Accessor which cannot do anything.
#[derive(Debug)]
struct DummySharedAccessor {}

#[async_trait]
impl Accessor for DummySharedAccessor {
    fn metadata(&self) -> AccessorMetadata {
        let mut meta = AccessorMetadata::default();
        meta.set_scheme(Scheme::Custom("shared"));
        meta
    }
}
