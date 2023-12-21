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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::http::metrics_incr_http_request_count;
use databend_common_metrics::http::metrics_incr_http_response_panics_count;
use databend_common_metrics::http::metrics_incr_http_slow_request_count;
use databend_common_storages_fuse::TableContext;
use headers::authorization::Basic;
use headers::authorization::Bearer;
use headers::authorization::Credentials;
use http::header::AUTHORIZATION;
use http::HeaderMap;
use http::HeaderValue;
use log::error;
use log::warn;
use poem::error::Error as PoemError;
use poem::error::Result as PoemResult;
use poem::http::StatusCode;
use poem::Addr;
use poem::Body;
use poem::Endpoint;
use poem::IntoResponse;
use poem::Middleware;
use poem::Request;
use poem::Response;
use uuid::Uuid;

use super::v1::HttpQueryContext;
use crate::auth::AuthMgr;
use crate::auth::Credential;
use crate::servers::HttpHandlerKind;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

const DEDUPLICATE_LABEL: &str = "X-DATABEND-DEDUPLICATE-LABEL";
const USER_AGENT: &str = "User-Agent";
const QUERY_ID: &str = "X-DATABEND-QUERY-ID";

pub struct HTTPSessionMiddleware {
    pub kind: HttpHandlerKind,
    pub auth_manager: Arc<AuthMgr>,
}

impl HTTPSessionMiddleware {
    pub fn create(kind: HttpHandlerKind, auth_manager: Arc<AuthMgr>) -> HTTPSessionMiddleware {
        HTTPSessionMiddleware { kind, auth_manager }
    }
}

fn get_credential(req: &Request, kind: HttpHandlerKind) -> Result<Credential> {
    let std_auth_headers: Vec<_> = req.headers().get_all(AUTHORIZATION).iter().collect();
    if std_auth_headers.len() > 1 {
        let msg = &format!("Multiple {} headers detected", AUTHORIZATION);
        return Err(ErrorCode::AuthenticateFailure(msg));
    }
    let client_ip = match req.remote_addr().0 {
        Addr::SocketAddr(addr) => Some(addr.ip().to_string()),
        Addr::Custom(..) => Some("127.0.0.1".to_string()),
        _ => None,
    };
    if std_auth_headers.is_empty() {
        if matches!(kind, HttpHandlerKind::Clickhouse) {
            auth_clickhouse_name_password(req, client_ip)
        } else {
            Err(ErrorCode::AuthenticateFailure(
                "No authorization header detected",
            ))
        }
    } else {
        auth_by_header(&std_auth_headers, client_ip)
    }
}

fn auth_by_header(
    std_auth_headers: &[&HeaderValue],
    client_ip: Option<String>,
) -> Result<Credential> {
    let value = &std_auth_headers[0];
    if value.as_bytes().starts_with(b"Basic ") {
        match Basic::decode(value) {
            Some(basic) => {
                let name = basic.username().to_string();
                let password = basic.password().to_owned().as_bytes().to_vec();
                let password = (!password.is_empty()).then_some(password);
                let c = Credential::Password {
                    name,
                    password,
                    client_ip,
                };
                Ok(c)
            }
            None => Err(ErrorCode::AuthenticateFailure("bad Basic auth header")),
        }
    } else if value.as_bytes().starts_with(b"Bearer ") {
        match Bearer::decode(value) {
            Some(bearer) => Ok(Credential::Jwt {
                token: bearer.token().to_string(),
                client_ip,
            }),
            None => Err(ErrorCode::AuthenticateFailure("bad Bearer auth header")),
        }
    } else {
        Err(ErrorCode::AuthenticateFailure("bad auth header"))
    }
}

fn auth_clickhouse_name_password(req: &Request, client_ip: Option<String>) -> Result<Credential> {
    let (user, key) = (
        req.headers().get("X-CLICKHOUSE-USER"),
        req.headers().get("X-CLICKHOUSE-KEY"),
    );
    if let (Some(name), Some(password)) = (user, key) {
        let c = Credential::Password {
            name: String::from_utf8(name.as_bytes().to_vec()).unwrap(),
            password: Some(password.as_bytes().to_vec()),
            client_ip,
        };
        Ok(c)
    } else {
        let query_str = req.uri().query().unwrap_or_default();
        let query_params = serde_urlencoded::from_str::<HashMap<String, String>>(query_str)
            .map_err(|e| ErrorCode::BadArguments(format!("{}", e)))?;
        let (user, key) = (query_params.get("user"), query_params.get("password"));
        if let (Some(name), Some(password)) = (user, key) {
            Ok(Credential::Password {
                name: name.clone(),
                password: Some(password.as_bytes().to_vec()),
                client_ip,
            })
        } else {
            Err(ErrorCode::AuthenticateFailure(
                "No header or query parameters for authorization detected",
            ))
        }
    }
}

impl<E: Endpoint> Middleware<E> for HTTPSessionMiddleware {
    type Output = HTTPSessionEndpoint<E>;
    fn transform(&self, ep: E) -> Self::Output {
        HTTPSessionEndpoint {
            ep,
            kind: self.kind,
            auth_manager: self.auth_manager.clone(),
        }
    }
}

pub struct HTTPSessionEndpoint<E> {
    ep: E,
    pub kind: HttpHandlerKind,
    pub auth_manager: Arc<AuthMgr>,
}

impl<E> HTTPSessionEndpoint<E> {
    #[async_backtrace::framed]
    async fn auth(&self, req: &Request) -> Result<HttpQueryContext> {
        let credential = get_credential(req, self.kind)?;
        let session_manager = SessionManager::instance();
        let session = session_manager.create_session(SessionType::Dummy).await?;
        let ctx = session.create_query_context().await?;
        if let Some(tenant_id) = req.headers().get("X-DATABEND-TENANT") {
            let tenant_id = tenant_id.to_str().unwrap().to_string();
            session.set_current_tenant(tenant_id);
        }
        let node_id = ctx.get_cluster().local_id.clone();

        self.auth_manager
            .auth(ctx.get_current_session(), &credential)
            .await?;

        let deduplicate_label = req
            .headers()
            .get(DEDUPLICATE_LABEL)
            .map(|id| id.to_str().unwrap().to_string());

        let user_agent = req
            .headers()
            .get(USER_AGENT)
            .map(|id| id.to_str().unwrap().to_string());

        let query_id = req
            .headers()
            .get(QUERY_ID)
            .map(|id| id.to_str().unwrap().to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        Ok(HttpQueryContext::new(
            session,
            query_id,
            node_id,
            deduplicate_label,
            user_agent,
        ))
    }
}

#[poem::async_trait]
impl<E: Endpoint> Endpoint for HTTPSessionEndpoint<E> {
    type Output = Response;

    #[async_backtrace::framed]
    async fn call(&self, mut req: Request) -> PoemResult<Self::Output> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();

        let res = match self.auth(&req).await {
            Ok(ctx) => {
                req.extensions_mut().insert(ctx);
                self.ep.call(req).await
            }
            Err(err) => match err.code() {
                ErrorCode::AUTHENTICATE_FAILURE => {
                    warn!(
                        "http auth failure: {method} {uri}, headers={:?}, error={}",
                        sanitize_request_headers(&headers),
                        err
                    );
                    Err(PoemError::from_string(
                        err.message(),
                        StatusCode::UNAUTHORIZED,
                    ))
                }
                _ => {
                    error!(
                        "http request err: {method} {uri}, headers={:?}, error={}",
                        sanitize_request_headers(&headers),
                        err
                    );
                    Err(PoemError::from_string(
                        err.message(),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    ))
                }
            },
        };
        match res {
            Err(err) => {
                let body = Body::from_json(serde_json::json!({
                    "error": {
                        "code": err.status().as_str(),
                        "message": err.to_string(),
                    }
                }))
                .unwrap();
                Ok(Response::builder().status(err.status()).body(body))
            }
            Ok(res) => Ok(res.into_response()),
        }
    }
}

pub fn sanitize_request_headers(headers: &HeaderMap) -> HashMap<String, String> {
    let sensitive_headers = ["authorization", "x-clickhouse-key", "cookie"];
    headers
        .iter()
        .map(|(k, v)| {
            let k = k.as_str().to_lowercase();
            if sensitive_headers.contains(&k.as_str()) {
                (k, "******".to_string())
            } else {
                (k, v.to_str().unwrap_or_default().to_string())
            }
        })
        .collect()
}

pub struct MetricsMiddleware {
    api: String,
}

impl MetricsMiddleware {
    pub fn new(api: impl Into<String>) -> Self {
        Self { api: api.into() }
    }
}

impl<E: Endpoint> Middleware<E> for MetricsMiddleware {
    type Output = MetricsMiddlewareEndpoint<E>;

    fn transform(&self, ep: E) -> Self::Output {
        MetricsMiddlewareEndpoint {
            ep,
            api: self.api.clone(),
        }
    }
}

pub struct MetricsMiddlewareEndpoint<E> {
    api: String,
    ep: E,
}

#[poem::async_trait]
impl<E: Endpoint> Endpoint for MetricsMiddlewareEndpoint<E> {
    type Output = Response;

    async fn call(&self, req: Request) -> poem::error::Result<Self::Output> {
        let start_time = Instant::now();
        let method = req.method().to_string();
        let output = self.ep.call(req).await?;
        let resp = output.into_response();
        let status_code = resp.status().to_string();
        metrics_incr_http_request_count(method.clone(), self.api.clone(), status_code.clone());
        if start_time.elapsed().as_secs() > 20 {
            // TODO: replace this into histogram
            metrics_incr_http_slow_request_count(method, self.api.clone(), status_code);
        }
        Ok(resp)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PanicHandler {}

impl PanicHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl poem::middleware::PanicHandler for PanicHandler {
    type Response = (StatusCode, &'static str);

    fn get_response(&self, _err: Box<dyn Any + Send + 'static>) -> Self::Response {
        metrics_incr_http_response_panics_count();
        (StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }
}
