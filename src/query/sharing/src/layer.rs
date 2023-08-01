// Copyright 2021 Datafuse Labs
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

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use common_auth::RefreshableToken;
use common_exception::ErrorCode;
use http::Request;
use http::Response;
use http::StatusCode;
use opendal::layers::LoggingLayer;
use opendal::layers::MetricsLayer;
use opendal::layers::RetryLayer;
use opendal::layers::TracingLayer;
use opendal::raw::new_request_build_error;
use opendal::raw::parse_content_length;
use opendal::raw::parse_etag;
use opendal::raw::parse_last_modified;
use opendal::raw::Accessor;
use opendal::raw::AccessorInfo;
use opendal::raw::AsyncBody;
use opendal::raw::HttpClient;
use opendal::raw::IncomingAsyncBody;
use opendal::raw::OpRead;
use opendal::raw::OpStat;
use opendal::raw::Operation;
use opendal::raw::PresignedRequest;
use opendal::raw::RpRead;
use opendal::raw::RpStat;
use opendal::Builder;
use opendal::Capability;
use opendal::EntryMode;
use opendal::Error;
use opendal::ErrorKind;
use opendal::Metadata;
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
) -> common_exception::Result<Operator> {
    let op = match share_endpoint_address {
        Some(share_endpoint_address) => {
            let signer = SharedSigner::new(
                &format!(
                    "http://{}/tenant/{}/{}/table/{}/presign",
                    share_endpoint_address, share_tenant_id, share_name, table_name
                ),
                share_endpoint_token,
                HttpClient::new()?,
            );
            let client = HttpClient::new()?;
            Operator::new(SharedBuilder {
                signer: Some(signer),
                client: Some(client),
            })?
            // Add retry
            .layer(RetryLayer::new().with_jitter())
            // Add metrics
            .layer(MetricsLayer)
            // Add logging
            .layer(LoggingLayer::default())
            // Add tracing
            .layer(TracingLayer)
            .finish()
        }
        None => {
            return Err(ErrorCode::EmptyShareEndpointConfig(format!(
                "Empty share config for creating operator of shared table {}.{}",
                share_name, table_name,
            )));
        }
    };

    Ok(op)
}

#[derive(Default)]
struct SharedBuilder {
    signer: Option<SharedSigner>,
    client: Option<HttpClient>,
}

impl Builder for SharedBuilder {
    const SCHEME: Scheme = Scheme::Custom("shared");

    type Accessor = SharedAccessor;

    fn from_map(_: HashMap<String, String>) -> Self {
        unreachable!("shared accessor doesn't build from map")
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        Ok(SharedAccessor {
            signer: self.signer.take().expect("must be valid"),
            client: self.client.take().expect("must be valid"),
        })
    }
}

#[derive(Debug)]
struct SharedAccessor {
    signer: SharedSigner,
    client: HttpClient,
}

#[async_trait]
impl Accessor for SharedAccessor {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Pager = ();
    type BlockingPager = ();
    type Appender = ();

    fn info(&self) -> AccessorInfo {
        let mut meta = AccessorInfo::default();
        meta.set_scheme(Scheme::Custom("shared"))
            .set_capability(Capability {
                read: true,
                read_with_range: true,

                stat: true,
                ..Default::default()
            });

        meta
    }

    #[async_backtrace::framed]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
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

        let resp = self.client.send(req).await?;

        if resp.status().is_success() {
            let content_length = parse_content_length(resp.headers())
                .unwrap()
                .expect("content_length must be valid");
            Ok((RpRead::new(content_length), resp.into_body()))
        } else {
            Err(parse_error(resp).await)
        }
    }

    #[async_backtrace::framed]
    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
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
        let resp = self.client.send(req).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let mode = if path.ends_with('/') {
                    EntryMode::DIR
                } else {
                    EntryMode::FILE
                };
                let mut m = Metadata::new(mode);
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
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await),
        }
    }
}

pub async fn parse_error(er: Response<IncomingAsyncBody>) -> Error {
    let (part, body) = er.into_parts();
    let message = body.bytes().await.unwrap_or_default();

    let (kind, retryable) = match part.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let mut err = Error::new(kind, &String::from_utf8_lossy(&message));

    if retryable {
        err = err.set_temporary();
    }

    err
}
