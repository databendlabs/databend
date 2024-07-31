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

use std::fmt::Debug;
use std::sync::Arc;

use databend_common_meta_app::schema::ShareDBParams;
use http::header::RANGE;
use http::Request;
use http::Response;
use http::StatusCode;
use opendal::layers::FastraceLayer;
use opendal::layers::LoggingLayer;
use opendal::layers::RetryLayer;
use opendal::raw::new_request_build_error;
use opendal::raw::parse_content_length;
use opendal::raw::parse_etag;
use opendal::raw::parse_last_modified;
use opendal::raw::Access;
use opendal::raw::AccessorInfo;
use opendal::raw::HttpBody;
use opendal::raw::HttpClient;
use opendal::raw::OpRead;
use opendal::raw::OpStat;
use opendal::raw::Operation;
use opendal::raw::PresignedRequest;
use opendal::raw::RpRead;
use opendal::raw::RpStat;
use opendal::Buffer;
use opendal::Builder;
use opendal::Capability;
use opendal::EntryMode;
use opendal::Error;
use opendal::ErrorKind;
use opendal::Metadata;
use opendal::Operator;
use opendal::Result;
use opendal::Scheme;

use crate::SharedSigner;

pub fn create_share_table_operator(
    share_params: &ShareDBParams,
    table_id: u64,
) -> databend_common_exception::Result<Operator> {
    let share_ident_raw = &share_params.share_ident;
    let signer = SharedSigner::new(
        &share_params.share_endpoint_url,
        &format!(
            "/{}/{}/{}/presign_files",
            share_ident_raw.tenant_name(),
            share_ident_raw.share_name(),
            table_id
        ),
        share_params.share_endpoint_credential.clone(),
    );
    let client = HttpClient::new()?;
    let op = Operator::new(SharedBuilder {
        signer: Some(signer),
        client: Some(client),
    })?
    // Add retry
    .layer(RetryLayer::new().with_jitter())
    // Add logging
    .layer(LoggingLayer::default())
    // Add tracing
    .layer(FastraceLayer)
    // TODO(liyz): add PrometheusClientLayer
    .finish();

    Ok(op)
}

#[derive(Default)]
struct SharedBuilder {
    signer: Option<SharedSigner>,
    client: Option<HttpClient>,
}

impl Builder for SharedBuilder {
    const SCHEME: Scheme = Scheme::Custom("shared");
    type Config = ();

    fn build(mut self) -> Result<impl Access> {
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

impl Access for SharedAccessor {
    type Reader = HttpBody;
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Lister = ();
    type BlockingLister = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut meta = AccessorInfo::default();
        meta.set_scheme(Scheme::Custom("shared"))
            .set_native_capability(Capability {
                read: true,

                stat: true,
                ..Default::default()
            });

        meta.into()
    }

    #[async_backtrace::framed]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let mut req: Request<Buffer> = self
            .signer
            .fetch(path, Operation::Read)
            .await
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "fetch presigned url failed").set_source(err)
            })?
            .into();

        req.headers_mut().insert(
            RANGE,
            args.range().to_header().parse().map_err(|err| {
                Error::new(ErrorKind::Unexpected, "header value is invalid").set_source(err)
            })?,
        );

        let resp = self.client.fetch(req).await?;

        let res = if resp.status().is_success() {
            resp.into_body()
        } else {
            let (part, mut body) = resp.into_parts();
            let buf = body.to_buffer().await?;
            return Err(parse_error(Response::from_parts(part, buf)).await);
        };

        Ok((RpRead::default(), res))
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
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;
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

pub async fn parse_error(er: Response<Buffer>) -> Error {
    let (part, body) = er.into_parts();
    let message = body.to_vec();

    let (kind, retryable) = match part.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let mut err = Error::new(kind, String::from_utf8_lossy(&message));

    if retryable {
        err = err.set_temporary();
    }

    err
}
