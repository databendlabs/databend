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
use std::fmt::Formatter;
use std::time;
use std::time::Duration;

use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use databend_common_auth::RefreshableToken;
use databend_common_config::GlobalConfig;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::Method;
use http::Request;
use log::info;
use moka::sync::Cache;
use opendal::raw::AsyncBody;
use opendal::raw::HttpClient;
use opendal::raw::Operation;
use opendal::raw::PresignedRequest;

pub(crate) const TENANT_HEADER: &str = "X-DATABEND-TENANT";

/// SharedSigner is used to track presign request, and it's response.
///
/// There is an internal cache about presign request. Getting an expired
/// request will get `None`. Please sign it again.
#[derive(Clone)]
pub struct SharedSigner {
    endpoint: String,
    cache: Cache<PresignRequest, PresignedRequest>,
    client: HttpClient,
    token: RefreshableToken,
}

impl Debug for SharedSigner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSigner")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl SharedSigner {
    /// Create a new SharedSigner.
    pub fn new(endpoint: &str, token: RefreshableToken, client: HttpClient) -> Self {
        let cache = Cache::builder()
            // Databend Cloud Presign will expire after 3600s (1 hour).
            // We will expire them 10 minutes before to avoid edge cases.
            .time_to_live(Duration::from_secs(3000))
            .build();

        Self {
            endpoint: endpoint.to_string(),
            cache,
            client,
            token,
        }
    }

    /// Get a presign request.
    pub fn get(&self, path: &str, op: Operation) -> Option<PresignedRequest> {
        self.cache.get(&PresignRequest {
            path: path.to_string(),
            op,
        })
    }

    /// Fetch a presigned request. If not found, build a new one by sign.
    #[async_backtrace::framed]
    pub async fn fetch(&self, path: &str, op: Operation) -> Result<PresignedRequest> {
        match self.get(path, op) {
            Some(v) => Ok(v),
            None => {
                self.sign(PresignRequest::new(path, op)).await?;
                self.get(path, op)
                    .ok_or_else(|| anyhow!("presigned request should exist, but not"))
            }
        }
    }

    /// Set a presigned request.
    ///
    /// This operation will update the expiry time about this request.
    pub fn set(&self, path: &str, op: Operation, signed: PresignedRequest) {
        self.cache.insert(PresignRequest::new(path, op), signed)
    }

    /// Sign a request.
    #[async_backtrace::framed]
    pub async fn sign(&self, req: PresignRequest) -> Result<()> {
        self.sign_inner(vec![req]).await
    }

    /// Batch sign multiple requests at once.
    #[async_backtrace::framed]
    pub async fn batch_sign(&self, reqs: Vec<PresignRequest>) -> Result<()> {
        self.sign_inner(reqs).await
    }

    /// Databend Cloud Presign API will be:
    ///
    /// ```shell
    /// curl -X POST \
    ///      https://endpoint/tenant/<tenant_id>/database/<db_id>/table/<table_id>/presign \
    ///      --header 'Authorization Bearer OIDC TOKEN' \
    ///      --data-raw '[{"path": "file_a", "method": "GET"}, {"path": "file_b", "method": "PUT"}'
    ///
    /// [
    ///     {
    ///        "path": "file_a",
    ///        "method": "GET",
    ///        "url": "https://example.com",
    ///        "headers": {
    ///           "host": "example.com"
    ///        },
    ///        "expires_in": "Sun, 06 Nov 1994 08:49:37 GMT"
    ///    },
    ///     {
    ///        "path": "file_b",
    ///        "method": "PUT",
    ///        "url": "https://example.com",
    ///        "headers": {
    ///           "host": "example.com"
    ///        },
    ///        "expires_in": "Sun, 06 Nov 1994 08:49:37 GMT"
    ///     }
    /// ]
    /// ```
    #[async_backtrace::framed]
    async fn sign_inner(&self, reqs: Vec<PresignRequest>) -> Result<()> {
        let now = time::Instant::now();
        info!("started sharing signing");

        let reqs: Vec<PresignRequestItem> = reqs
            .into_iter()
            .map(|v| PresignRequestItem {
                file_name: v.path,
                method: to_method(v.op),
            })
            .collect();
        let bs = Bytes::from(serde_json::to_vec(&reqs)?);
        let auth = self.token.to_header().await?;
        let requester = GlobalConfig::instance()
            .as_ref()
            .query
            .tenant_id
            .tenant_name()
            .to_string();
        let req = Request::builder()
            .method(Method::POST)
            .uri(&self.endpoint)
            .header(AUTHORIZATION, auth)
            .header(CONTENT_LENGTH, bs.len())
            .header(TENANT_HEADER, requester)
            .body(AsyncBody::Bytes(bs))?;
        let resp = self.client.send(req).await?;
        let bs = resp.into_body().bytes().await?;
        let items: Vec<PresignResponseItem> = serde_json::from_slice(&bs)?;

        for item in items {
            self.cache.insert(
                PresignRequest::new(&item.path, from_method(&item.method)),
                item.into(),
            );
        }

        info!(
            "finished sharing signing after {}ms",
            now.elapsed().as_millis()
        );
        Ok(())
    }
}

/// PresignRequest struct represent a request to be signed.
#[derive(Hash, Eq, PartialEq)]
pub struct PresignRequest {
    path: String,
    op: Operation,
}

impl PresignRequest {
    /// Create a new PresignRequest.
    pub fn new(path: &str, op: Operation) -> Self {
        Self {
            path: path.to_string(),
            op,
        }
    }
}

fn to_method(op: Operation) -> String {
    match op {
        Operation::Read => "GET".to_string(),
        Operation::Stat => "HEAD".to_string(),
        v => unimplemented!("not supported operation: {v}"),
    }
}

fn from_method(method: &str) -> Operation {
    match method {
        "GET" => Operation::Read,
        "HEAD" => Operation::Stat,
        v => unimplemented!("not supported operation: {v}"),
    }
}

#[derive(serde::Serialize)]
struct PresignRequestItem {
    file_name: String,
    method: String,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct PresignResponseItem {
    method: String,
    path: String,
    presigned_url: String,
    headers: HashMap<String, String>,
}

impl From<PresignResponseItem> for PresignedRequest {
    fn from(v: PresignResponseItem) -> Self {
        PresignedRequest::new(
            v.method.parse().expect("must be valid method"),
            v.presigned_url.parse().expect("must be valid uri"),
            v.headers
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.parse().expect("header name must be valid"),
                        v.parse().expect("header value must be valid"),
                    )
                })
                .collect(),
        )
    }
}
