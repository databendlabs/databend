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

// Logs from this module will show up as "[HTTP-SESSION] ...".
databend_common_tracing::register_module_tag!("[HTTP-SESSION]");

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_base::headers::HEADER_CLIENT_CAPABILITIES;
use databend_common_base::headers::HEADER_DEDUPLICATE_LABEL;
use databend_common_base::headers::HEADER_NODE_ID;
use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_STICKY;
use databend_common_base::headers::HEADER_TENANT;
use databend_common_base::headers::HEADER_VERSION;
use databend_common_base::headers::HEADER_WAREHOUSE;
use databend_common_base::runtime::ThreadTracker;
use databend_common_catalog::session_type::SessionType;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::user_token::TokenType;
use databend_common_meta_app::tenant::Tenant;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_meta_types::NodeInfo;
use fastrace::func_name;
use headers::authorization::Basic;
use headers::authorization::Bearer;
use headers::authorization::Credentials;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use http::header::AUTHORIZATION;
use log::error;
use log::info;
use log::warn;
use opentelemetry::baggage::BaggageExt;
use opentelemetry::propagation::Extractor;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::BaggagePropagator;
use poem::Addr;
use poem::Endpoint;
use poem::Error;
use poem::IntoResponse;
use poem::Middleware;
use poem::Request;
use poem::Response;
use poem::error::ResponseError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use uuid::Uuid;

use crate::auth::AuthMgr;
use crate::auth::Credential;
use crate::clusters::ClusterDiscovery;
use crate::servers::HttpHandlerKind;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::error::JsonErrorOnly;
use crate::servers::http::error::QueryError;
use crate::servers::http::middleware::ClientCapabilities;
use crate::servers::http::middleware::session_header::ClientSession;
use crate::servers::http::middleware::session_header::ClientSessionType;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::SessionClaim;
use crate::servers::login_history::LoginEventType;
use crate::servers::login_history::LoginHandler;
use crate::servers::login_history::LoginHistory;
use crate::sessions::SessionManager;
const USER_AGENT: &str = "User-Agent";
const TRACE_PARENT: &str = "traceparent";

#[derive(Debug, Copy, Clone)]
pub enum EndpointKind {
    Login,
    Logout,
    Refresh,
    HeartBeat,
    StartQuery,
    PollQuery,
    Clickhouse,
    NoAuth,
    Verify,
    UploadToStage,
    SystemInfo,
    Catalog,
    Metadata,
    StreamingLoad,
}

impl EndpointKind {
    /// avoid the cost of get user from meta
    pub fn need_user_info(&self) -> bool {
        !matches!(self, EndpointKind::NoAuth | EndpointKind::PollQuery)
    }
    pub fn may_need_sticky(&self) -> bool {
        matches!(
            self,
            EndpointKind::StartQuery
                | EndpointKind::PollQuery
                | EndpointKind::Logout
                | EndpointKind::HeartBeat
                | EndpointKind::Catalog
        )
    }
    pub fn require_databend_token_type(&self) -> Result<Option<TokenType>> {
        match self {
            EndpointKind::Verify | EndpointKind::NoAuth => Ok(None),
            EndpointKind::Refresh => Ok(Some(TokenType::Refresh)),
            EndpointKind::StartQuery
            | EndpointKind::PollQuery
            | EndpointKind::Logout
            | EndpointKind::SystemInfo
            | EndpointKind::HeartBeat
            | EndpointKind::StreamingLoad
            | EndpointKind::UploadToStage
            | EndpointKind::Metadata
            | EndpointKind::Catalog => {
                if GlobalConfig::instance().query.common.management_mode {
                    Ok(None)
                } else {
                    Ok(Some(TokenType::Session))
                }
            }
            EndpointKind::Login | EndpointKind::Clickhouse => Err(ErrorCode::AuthenticateFailure(
                format!("Invalid token usage: databend token cannot be used for {self:?}",),
            )),
        }
    }
}

pub struct HTTPSessionMiddleware {
    pub kind: HttpHandlerKind,
    pub endpoint_kind: EndpointKind,
    pub auth_manager: Arc<AuthMgr>,
}

impl HTTPSessionMiddleware {
    pub fn create(kind: HttpHandlerKind, endpoint_kind: EndpointKind) -> HTTPSessionMiddleware {
        let auth_manager = AuthMgr::instance();
        HTTPSessionMiddleware {
            kind,
            endpoint_kind,
            auth_manager,
        }
    }
}

pub struct HeaderExtractor<'a>(pub &'a http::HeaderMap);
impl Extractor for HeaderExtractor<'_> {
    /// Get a value for a key from the HeaderMap.  If the value is not valid ASCII, returns None.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    /// Collect all the keys from the HeaderMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|value| value.as_str())
            .collect::<Vec<_>>()
    }
}

fn extract_baggage_from_headers(headers: &HeaderMap) -> Option<Vec<(String, String)>> {
    headers.get("baggage")?;
    let propagator = BaggagePropagator::new();
    let extractor = HeaderExtractor(headers);
    let result: Vec<(String, String)> = {
        let context = propagator.extract(&extractor);
        let baggage = context.baggage();
        baggage
            .iter()
            .map(|(key, (value, _metadata))| (key.to_string(), value.to_string()))
            .collect()
    };
    Some(result)
}

fn get_credential(
    req: &Request,
    kind: HttpHandlerKind,
    endpoint_kind: EndpointKind,
) -> Result<Credential> {
    if matches!(endpoint_kind, EndpointKind::NoAuth) {
        return Ok(Credential::NoNeed);
    }
    let std_auth_headers: Vec<_> = req.headers().get_all(AUTHORIZATION).iter().collect();
    if std_auth_headers.len() > 1 {
        let msg = &format!(
            "Authentication error: multiple {} headers detected",
            AUTHORIZATION
        );
        return Err(ErrorCode::AuthenticateFailure(msg));
    }
    let client_ip = get_client_ip(req);
    if std_auth_headers.is_empty() {
        if matches!(kind, HttpHandlerKind::Clickhouse) {
            get_clickhouse_name_password(req, client_ip)
        } else {
            Err(ErrorCode::AuthenticateFailure(
                "Authentication error: no authorization header provided",
            ))
        }
    } else {
        get_credential_from_header(&std_auth_headers, client_ip, endpoint_kind)
    }
}

/// this function tries to get the client IP address from the headers. if the ip in header
/// not found, fallback to the remote address, which might be local proxy's ip address.
/// please note that when it comes with network policy, we need make sure the incoming
/// traffic comes from a trustworthy proxy instance.
fn get_client_ip(req: &Request) -> Option<String> {
    let headers = ["X-Real-IP", "X-Forwarded-For", "CF-Connecting-IP"];
    for &header in headers.iter() {
        if let Some(value) = req.headers().get(header) {
            if let Ok(mut ip_str) = value.to_str() {
                if header == "X-Forwarded-For" {
                    ip_str = ip_str.split(',').next().unwrap_or("");
                }
                return Some(ip_str.to_string());
            }
        }
    }

    // fallback to the connection's remote address, take care
    let client_ip = match req.remote_addr().0 {
        Addr::SocketAddr(addr) => Some(addr.ip().to_string()),
        Addr::Custom(..) => Some("127.0.0.1".to_string()),
        _ => None,
    };

    client_ip
}

fn get_credential_from_header(
    std_auth_headers: &[&HeaderValue],
    client_ip: Option<String>,
    endpoint_kind: EndpointKind,
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
            None => Err(ErrorCode::AuthenticateFailure(
                "Authentication error: invalid Basic auth header format",
            )),
        }
    } else if value.as_bytes().starts_with(b"Bearer ") {
        match Bearer::decode(value) {
            Some(bearer) => {
                let token = bearer.token().to_string();
                if SessionClaim::is_databend_token(&token) {
                    if let Some(t) = endpoint_kind.require_databend_token_type()? {
                        if t != SessionClaim::get_type(&token)? {
                            return Err(ErrorCode::AuthenticateFailure(
                                "Authentication error: incorrect token type for this endpoint",
                            ));
                        }
                    }
                    Ok(Credential::DatabendToken { token })
                } else {
                    Ok(Credential::Jwt { token, client_ip })
                }
            }
            None => Err(ErrorCode::AuthenticateFailure(
                "Authentication error: invalid Bearer auth header format",
            )),
        }
    } else {
        Err(ErrorCode::AuthenticateFailure(
            "Authentication error: unsupported authorization header format",
        ))
    }
}

fn get_clickhouse_name_password(req: &Request, client_ip: Option<String>) -> Result<Credential> {
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
            .map_err(|e| {
                ErrorCode::BadArguments(format!("Failed to parse query parameters: {}", e))
            })?;
        let (user, key) = (query_params.get("user"), query_params.get("password"));
        if let (Some(name), Some(password)) = (user, key) {
            Ok(Credential::Password {
                name: name.clone(),
                password: Some(password.as_bytes().to_vec()),
                client_ip,
            })
        } else {
            Err(ErrorCode::AuthenticateFailure(
                "Authentication error: no credentials found in headers or query parameters",
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
            endpoint_kind: self.endpoint_kind,
            auth_manager: self.auth_manager.clone(),
        }
    }
}

pub struct HTTPSessionEndpoint<E> {
    ep: E,
    pub kind: HttpHandlerKind,
    pub endpoint_kind: EndpointKind,
    pub auth_manager: Arc<AuthMgr>,
}

impl<E> HTTPSessionEndpoint<E> {
    #[async_backtrace::framed]
    async fn auth(
        &self,
        req: &Request,
        query_id: String,
        is_query_id_from_client: bool,
        login_history: &mut LoginHistory,
        is_sticky_node: bool,
    ) -> Result<HttpQueryContext> {
        let client_host = get_client_ip(req);
        let node_id = GlobalConfig::instance().query.node_id.clone();
        login_history.client_ip = client_host.clone().unwrap_or_default();
        login_history.node_id = node_id.clone();
        let user_agent = req
            .headers()
            .get(USER_AGENT)
            .map(|id| id.to_str().unwrap().to_string());

        let mut client_caps = req
            .headers()
            .get(HEADER_CLIENT_CAPABILITIES)
            .map(|caps| caps.to_str().unwrap().to_string())
            .map(|caps| ClientCapabilities::parse(&caps))
            .unwrap_or_default();

        let is_worksheet = user_agent
            .as_ref()
            .map(|ua_str| {
                [
                    // only worksheet client run in browser.
                    // most browser ua contain multi of them
                    "Mozilla",
                    "Chrome",
                    "Firefox",
                    "Safari",
                    "Edge",
                    // worksheet start query with ua like DatabendCloud/worksheet=4703;
                    "worksheet",
                ]
                .iter()
                .any(|kw| ua_str.contains(kw))
            })
            .unwrap_or(false);

        login_history.user_agent = user_agent.clone().unwrap_or_default();

        let credential = get_credential(req, self.kind, self.endpoint_kind)?;
        login_history.auth_type = credential.type_name();

        // Extract and record username from credential before authenticate
        // This ensures we log the username even if authentication failed
        if let Some(user_name) = credential.user_name() {
            login_history.user_name = user_name.clone();
        }
        let session_manager = SessionManager::instance();

        let mut session = session_manager.create_session(SessionType::Dummy).await?;

        if let Some(tenant_id) = req.headers().get(HEADER_TENANT) {
            let tenant_id = tenant_id.to_str().unwrap().to_string();
            let tenant = Tenant::new_or_err(tenant_id.clone(), func_name!())?;
            session.set_current_tenant(tenant);
        }
        let (user_name, authed_client_session_id) = self
            .auth_manager
            .auth(
                &mut session,
                &credential,
                self.endpoint_kind.need_user_info(),
            )
            .await?;
        login_history.user_name = user_name.clone();

        let mut client_session = if is_worksheet {
            ClientSession::try_decode_for_worksheet(req)
        } else {
            ClientSession::try_decode(req, &mut client_caps)?
        };
        if client_session.is_none() && !matches!(self.endpoint_kind, EndpointKind::PollQuery) {
            info!(
                "got request without session, url={}, headers={:?}",
                req.uri(),
                &req.headers()
            );
        }

        if let (Some(id1), Some(c)) = (&authed_client_session_id, &client_session) {
            if *id1 != c.header.id {
                return Err(ErrorCode::AuthenticateFailure(format!(
                    "Session ID mismatch: token session ID '{}' does not match header session ID '{}'",
                    id1, c.header.id
                )));
            }
        }
        login_history.disable_write = false;
        if let Some(s) = &mut client_session {
            let sid = s.header.id.clone();
            session.set_client_session_id(sid.clone());
            login_history.session_id = sid.clone();
            if !s.is_new_session || is_worksheet {
                // if session enabled by client:
                //     log for the first request of the session.
                // else:
                //     log every request, which can be distinguished by `session_id = ''`
                login_history.disable_write = true;
            }
            if ClientSessionType::IDOnly != s.typ {
                s.try_refresh_state(session.get_current_tenant(), &user_name, req.cookie())
                    .await?;
            }
        }

        let session = session_manager.register_session(session)?;

        let deduplicate_label = req
            .headers()
            .get(HEADER_DEDUPLICATE_LABEL)
            .map(|id| id.to_str().unwrap().to_string());

        let expected_node_id = req
            .headers()
            .get(HEADER_NODE_ID)
            .map(|id| id.to_str().unwrap().to_string());

        let trace_parent = req
            .headers()
            .get(TRACE_PARENT)
            .map(|id| id.to_str().unwrap().to_string());
        let opentelemetry_baggage = extract_baggage_from_headers(req.headers());

        let version = GlobalConfig::version();
        Ok(HttpQueryContext {
            session,
            query_id,
            is_query_id_from_client,
            node_id,
            credential,
            expected_node_id,
            deduplicate_label,
            user_agent,
            trace_parent,
            opentelemetry_baggage,
            http_method: req.method().to_string(),
            uri: req.uri().to_string(),
            client_host,
            client_session_id: client_session.as_ref().map(|s| s.header.id.clone()),
            user_name,
            is_sticky_node,
            client_session,
            fixed_coordinator_node: is_worksheet,
            client_caps,
            version,
        })
    }
}

async fn forward_request(mut req: Request, node: Arc<NodeInfo>) -> PoemResult<Response> {
    let body = req.take_body().into_bytes().await?;
    let mut headers = req.headers().clone();
    headers.remove(http::header::HOST);
    forward_request_with_body(
        node,
        &req.uri().to_string(),
        body,
        req.method().to_owned(),
        headers,
    )
    .await
}

pub async fn forward_request_with_body<T: Into<reqwest::Body>>(
    node: Arc<NodeInfo>,
    uri: &str,
    body: T,
    method: Method,
    headers: HeaderMap,
) -> PoemResult<Response> {
    let addr = node.http_address.clone();
    let config = GlobalConfig::instance();
    let scheme = if config.query.common.http_handler_tls_server_key.is_empty()
        || config.query.common.http_handler_tls_server_cert.is_empty()
    {
        "http"
    } else {
        "https"
    };
    let url = format!("{scheme}://{addr}/v1{}", uri);

    let client = reqwest::Client::new();
    let reqwest_request = client
        .request(method, &url)
        .headers(headers)
        .body(body)
        .build()
        .map_err(|e| {
            HttpErrorCode::bad_request(ErrorCode::BadArguments(format!(
                "fail to build forward request: {e}"
            )))
        })?;

    let response = client.execute(reqwest_request).await.map_err(|e| {
        HttpErrorCode::server_error(ErrorCode::Internal(format!(
            "fail to send forward request: {e}",
        )))
    })?;

    let status = StatusCode::from_u16(response.status().as_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let headers = response.headers().clone();
    let body = response.bytes().await.map_err(|e| {
        HttpErrorCode::server_error(ErrorCode::Internal(format!(
            "fail to send forward request: {e}",
        )))
    })?;
    let mut poem_resp = Response::builder().status(status).body(body);
    let headers_ref = poem_resp.headers_mut();
    for (key, value) in headers.iter() {
        headers_ref.insert(key, value.to_owned());
    }
    Ok(poem_resp)
}

fn get_request_info(mut req: Request) -> String {
    req.headers_mut().remove(AUTHORIZATION);
    format!("{req:?}")
}

impl<E: Endpoint> Endpoint for HTTPSessionEndpoint<E> {
    type Output = Response;

    #[async_backtrace::framed]
    async fn call(&self, mut req: Request) -> PoemResult<Self::Output> {
        let headers = req.headers().clone();

        let mut is_sticky_node = false;

        if self.endpoint_kind.may_need_sticky() {
            if let Some(sticky_node_id) = headers.get(HEADER_STICKY) {
                let sticky_node_id = sticky_node_id
                    .to_str()
                    .map_err(|e| {
                        HttpErrorCode::bad_request(ErrorCode::BadArguments(format!(
                            "Invalid Header ({HEADER_STICKY}: {sticky_node_id:?}): {e}"
                        )))
                    })?
                    .to_string();
                is_sticky_node = true;
                let local_id = GlobalConfig::instance().query.node_id.clone();
                if local_id != sticky_node_id {
                    return if let Some(node) = ClusterDiscovery::instance()
                        .find_node_by_id(&sticky_node_id)
                        .await
                        .map_err(HttpErrorCode::server_error)?
                    {
                        log::info!(
                            "forwarding /v1{} from {local_id} to {sticky_node_id}",
                            req.uri()
                        );
                        forward_request(req, node).await
                    } else {
                        let msg = format!(
                            "Sticky session state lost: node '{sticky_node_id}' not found in cluster, request={}",
                            get_request_info(req)
                        );
                        warn!("{}", msg);
                        Err(Error::from(HttpErrorCode::bad_request(
                            ErrorCode::BadArguments(msg),
                        )))
                    };
                }
            } else if let Some(warehouse) = headers.get(HEADER_WAREHOUSE) {
                let resources_management = GlobalInstance::get::<Arc<dyn ResourcesManagement>>();
                if resources_management.support_forward_warehouse_request() {
                    req.headers_mut().remove(HEADER_WAREHOUSE);

                    let warehouse = warehouse
                        .to_str()
                        .map_err(|e| {
                            HttpErrorCode::bad_request(ErrorCode::BadArguments(format!(
                                "Invalid value for header ({HEADER_WAREHOUSE}: {warehouse:?}): {e}"
                            )))
                        })?
                        .to_string();

                    let cluster_discovery = ClusterDiscovery::instance();

                    let forward_node = cluster_discovery.find_node_by_warehouse(&warehouse).await;

                    match forward_node {
                        Err(error) => {
                            return Err(HttpErrorCode::server_error(
                                error.add_message_back("(while in warehouse request forward)"),
                            )
                            .into());
                        }
                        Ok(None) => {
                            let msg = format!(
                                "Not find the '{}' warehouse; it is possible that all nodes of the warehouse have gone offline. Please exit the client and reconnect, or use `use warehouse <new_warehouse>`. request = {}.",
                                warehouse,
                                get_request_info(req)
                            );
                            warn!("{}", msg);
                            return Err(Error::from(HttpErrorCode::bad_request(
                                ErrorCode::UnknownWarehouse(msg),
                            )));
                        }
                        Ok(Some(node)) => {
                            let local_id = GlobalConfig::instance().query.node_id.clone();
                            if node.id != local_id {
                                log::info!(
                                    "forwarding /v1{} from {} to warehouse {}({})",
                                    req.uri(),
                                    local_id,
                                    warehouse,
                                    node.id
                                );
                                return forward_request(req, node).await;
                            }
                        }
                    }
                }

                log::warn!("Ignoring warehouse header: {HEADER_WAREHOUSE}={warehouse:?}");
            }
        };

        let method = req.method().clone();
        let uri = req.uri().clone();

        let (query_id, is_query_id_from_client) =
            if let Some(v) = req.headers().get(HEADER_QUERY_ID) {
                (v.to_str().unwrap().to_string(), true)
            } else {
                (Uuid::now_v7().simple().to_string(), false)
            };

        let mut login_history = LoginHistory::new();
        login_history.handler = LoginHandler::HTTP;
        login_history.connection_uri = uri.to_string();

        ThreadTracker::tracking_future(async move {
            match self
                .auth(
                    &req,
                    query_id,
                    is_query_id_from_client,
                    &mut login_history,
                    is_sticky_node,
                )
                .await
            {
                Ok(ctx) => {
                    login_history.event_type = LoginEventType::LoginSuccess;
                    login_history.write_to_log();
                    let client_session = ctx.client_session.clone();
                    req.extensions_mut().insert(ctx);
                    self.ep.call(req).await.map(|v| {
                        let mut r = v.into_response();
                        if let Some(s) = client_session {
                            s.on_response(&mut r);
                        }
                        r
                    })
                }
                Err(err) => {
                    login_history.event_type = LoginEventType::LoginFailed;
                    login_history.error_message = err.to_string();
                    login_history.write_to_log();
                    let err = HttpErrorCode::error_code(err);
                    if err.status() == StatusCode::UNAUTHORIZED {
                        warn!(
                            "Authentication failure: {method} {uri}, headers={:?}, error={}",
                            sanitize_request_headers(&headers),
                            err
                        );
                    } else {
                        error!(
                            "Request error: {method} {uri}, headers={:?}, error={}",
                            sanitize_request_headers(&headers),
                            err
                        );
                    }
                    Ok(err.as_response())
                }
            }
        })
        .await
    }
}

pub fn sanitize_request_headers(headers: &poem::http::HeaderMap) -> HashMap<String, String> {
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

pub async fn json_response<E: Endpoint>(next: E, req: Request) -> PoemResult<Response> {
    let mut resp = match next.call(req).await {
        Ok(resp) => resp.into_response(),
        Err(err) => (
            err.status(),
            Json(JsonErrorOnly {
                error: QueryError {
                    code: err.status().as_u16(),
                    message: err.to_string(),
                    detail: None,
                },
            }),
        )
            .into_response(),
    };
    resp.headers_mut().insert(
        HEADER_VERSION,
        GlobalConfig::version()
            .semantic
            .to_string()
            .parse()
            .unwrap(),
    );
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use crate::servers::http::middleware::session::get_client_ip;

    #[test]
    fn test_parse_ip() {
        let req = poem::Request::builder()
            .header("X-Forwarded-For", "1.2.3.4")
            .finish();
        let ip = get_client_ip(&req);
        assert_eq!(ip, Some("1.2.3.4".to_string()));
    }
}
