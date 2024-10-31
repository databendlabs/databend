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

use std::future;
use std::mem;
use std::str::FromStr;

use databend_common_base::http_client::GLOBAL_HTTP_CLIENT;
use futures::TryStreamExt;
use http::Request;
use http::Response;
use opendal::raw::parse_content_encoding;
use opendal::raw::parse_content_length;
use opendal::raw::HttpBody;
use opendal::raw::HttpFetch;
use opendal::Buffer;

pub struct StorageHttpClient {
    client: reqwest::Client,
}

impl StorageHttpClient {
    pub fn new() -> Self {
        Self {
            client: GLOBAL_HTTP_CLIENT.inner(),
        }
    }
}

impl HttpFetch for StorageHttpClient {
    async fn fetch(&self, req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        // Uri stores all string alike data in `Bytes` which means
        // the clone here is cheap.
        let uri = req.uri().clone();
        let is_head = req.method() == http::Method::HEAD;

        let (parts, body) = req.into_parts();

        let mut req_builder = self
            .client
            .request(
                parts.method,
                reqwest::Url::from_str(&uri.to_string()).expect("input request url must be valid"),
            )
            .headers(parts.headers);

        req_builder = req_builder.version(parts.version);
        // Don't set body if body is empty.
        if !body.is_empty() {
            req_builder = req_builder.body(reqwest::Body::wrap_stream(body))
        }

        let mut resp = req_builder
            .send()
            .await
            .map_err(|err| to_opendal_unexpected_error(err, &uri, "send http request"))?;

        // Get content length from header so that we can check it.
        //
        // - If the request method is HEAD, we will ignore content length.
        // - If response contains content_encoding, we should omit its content length.
        let content_length = if is_head || parse_content_encoding(resp.headers())?.is_some() {
            None
        } else {
            parse_content_length(resp.headers())?
        };

        let mut hr = Response::builder()
            .status(resp.status())
            // Insert uri into response extension so that we can fetch
            // it later.
            .extension(uri.clone());

        hr = hr.version(resp.version());

        // Swap headers directly instead of copy the entire map.
        mem::swap(hr.headers_mut().unwrap(), resp.headers_mut());

        let bs = HttpBody::new(
            resp.bytes_stream()
                .try_filter(|v| future::ready(!v.is_empty()))
                .map_ok(Buffer::from)
                .map_err(move |err| {
                    to_opendal_unexpected_error(err, &uri, "read data from http response")
                }),
            content_length,
        );

        let resp = hr.body(bs).expect("response must build succeed");
        Ok(resp)
    }
}

fn to_opendal_unexpected_error(err: reqwest::Error, uri: &http::Uri, desc: &str) -> opendal::Error {
    let mut oe = opendal::Error::new(opendal::ErrorKind::Unexpected, desc)
        .with_operation("http_util::Client::send")
        .with_context("url", uri.to_string());
    if is_temporary_error(&err) {
        oe = oe.set_temporary();
    }
    oe = oe.set_source(err);
    oe
}

#[inline]
fn is_temporary_error(err: &reqwest::Error) -> bool {
    // error sending request
    err.is_request()||
  // request or response body error
  err.is_body() ||
  // error decoding response body, for example, connection reset.
  err.is_decode()
}
