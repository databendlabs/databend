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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use headers::authorization::Basic;
use headers::authorization::Bearer;
use headers::authorization::Credentials;
use http::header::AUTHORIZATION;
use http::HeaderValue;
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
use tracing::error;
use tracing::info;

use super::v1::HttpQueryContext;
use crate::auth::AuthMgr;
use crate::auth::Credential;
use crate::servers::HttpHandlerKind;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
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
                    hostname: client_ip,
                };
                Ok(c)
            }
            None => Err(ErrorCode::AuthenticateFailure("bad Basic auth header")),
        }
    } else if value.as_bytes().starts_with(b"Bearer ") {
        match Bearer::decode(value) {
            Some(bearer) => Ok(Credential::Jwt {
                token: bearer.token().to_string(),
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
            hostname: client_ip,
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
                hostname: client_ip,
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

        self.auth_manager
            .auth(ctx.get_current_session(), &credential)
            .await?;

        Ok(HttpQueryContext::new(session))
    }
}
#[poem::async_trait]
impl<E: Endpoint> Endpoint for HTTPSessionEndpoint<E> {
    type Output = Response;

    #[async_backtrace::framed]
    async fn call(&self, mut req: Request) -> PoemResult<Self::Output> {
        // method, url, version, header
        info!("receive http handler request: {req:?},");
        let res = match self.auth(&req).await {
            Ok(ctx) => {
                req.extensions_mut().insert(ctx);
                self.ep.call(req).await
            }
            Err(err) => Err(PoemError::from_string(
                err.message(),
                StatusCode::UNAUTHORIZED,
            )),
        };
        match res {
            Err(err) => {
                error!("http request error: {}", err);
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
