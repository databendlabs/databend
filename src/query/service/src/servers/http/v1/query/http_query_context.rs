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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use http::StatusCode;
use log::warn;
use poem::FromRequest;
use poem::Request;
use poem::RequestBody;

use crate::auth::Credential;
use crate::servers::http::middleware::ClientCapabilities;
use crate::servers::http::middleware::session_header::ClientSession;
use crate::servers::http::middleware::session_header::ClientSessionType;
use crate::servers::http::v1::ClientSessionManager;
use crate::servers::http::v1::HttpQueryManager;
use crate::servers::http::v1::HttpSessionConf;
use crate::sessions::BuildInfoRef;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::SessionManager;

#[derive(Clone)]
pub struct HttpQueryContext {
    pub session: Arc<Session>,
    pub credential: Credential,
    pub query_id: String,
    pub node_id: String,
    pub expected_node_id: Option<String>,
    pub deduplicate_label: Option<String>,
    pub user_agent: Option<String>,
    pub trace_parent: Option<String>,
    pub opentelemetry_baggage: Option<Vec<(String, String)>>,
    pub http_method: String,
    pub uri: String,
    pub client_host: Option<String>,
    pub client_session_id: Option<String>,
    pub user_name: String,
    pub is_sticky_node: bool,
    pub client_session: Option<ClientSession>,
    // for now only used for worksheet
    pub fixed_coordinator_node: bool,
    pub client_caps: ClientCapabilities,
    pub version: BuildInfoRef,
}

impl HttpQueryContext {
    pub fn upgrade_session(&self, session_type: SessionType) -> Result<Arc<Session>, poem::Error> {
        SessionManager::instance()
            .try_upgrade_session(self.session.clone(), session_type.clone())
            .map_err(|err| {
                poem::Error::from_string(err.message(), poem::http::StatusCode::TOO_MANY_REQUESTS)
            })?;
        Ok(self.session.clone())
    }

    pub fn to_fastrace_properties(&self) -> BTreeMap<String, String> {
        let mut result = BTreeMap::new();
        let properties = self.session.to_fastrace_properties();
        result.extend(properties);
        result.extend([
            ("query_id".to_string(), self.query_id.clone()),
            ("node_id".to_string(), self.node_id.clone()),
            (
                "deduplicate_label".to_string(),
                self.deduplicate_label.clone().unwrap_or_default(),
            ),
            (
                "user_agent".to_string(),
                self.user_agent.clone().unwrap_or_default(),
            ),
            ("http_method".to_string(), self.http_method.clone()),
            ("uri".to_string(), self.uri.clone()),
        ]);
        if let Some(baggage) = self.opentelemetry_baggage.clone() {
            result.extend(baggage);
        }
        result
    }

    pub fn set_fail(&self) {
        self.session.txn_mgr().lock().set_fail();
    }

    pub fn check_node_id(&self, query_id: &str) -> poem::Result<()> {
        if let Some(expected_node_id) = self.expected_node_id.as_ref() {
            if expected_node_id != &self.node_id {
                let manager = HttpQueryManager::instance();
                let start_time = manager.server_info.start_time.clone();
                let uptime = (Instant::now() - manager.start_instant).as_secs_f32();
                let msg = format!(
                    "Routing error: query {query_id} should be on server {expected_node_id}, but current server is {}, which started at {start_time} ({uptime} secs ago)",
                    self.node_id
                );
                warn!("{}", msg);
                return Err(poem::Error::from_string(msg, StatusCode::NOT_FOUND));
            }
        }
        Ok(())
    }

    pub async fn try_refresh_worksheet_session(&self) -> databend_common_exception::Result<()> {
        if let Some(client_session) = self.client_session.as_ref() {
            if client_session.typ == ClientSessionType::IDOnly {
                ClientSessionManager::instance()
                    .try_refresh_state(
                        self.session.get_current_tenant(),
                        &client_session.header.id,
                        &self.user_name,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn try_set_worksheet_session(
        &self,
        http_session_conf: &Option<HttpSessionConf>,
    ) -> databend_common_exception::Result<()> {
        if let Some(client_session) = self.client_session.as_ref() {
            if client_session.typ == ClientSessionType::IDOnly {
                if let Some(conf) = http_session_conf.as_ref() {
                    match &conf.internal {
                        Some(internal) => {
                            if internal.has_temp_table {
                                ClientSessionManager::instance()
                                    .try_refresh_state(
                                        self.session.get_current_tenant(),
                                        &client_session.header.id,
                                        &self.user_name,
                                    )
                                    .await?;
                            }
                        }
                        None => {
                            ClientSessionManager::instance()
                                .drop_client_session_state(
                                    &self.session.get_current_tenant(),
                                    &self.user_name,
                                    &client_session.header.id,
                                )
                                .await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn create_session(
        &self,
        http_session_conf: &Option<HttpSessionConf>,
        session_type: SessionType,
    ) -> databend_common_exception::Result<(Arc<Session>, Arc<QueryContext>)> {
        let session = self
            .upgrade_session(session_type)
            .map_err(|err| ErrorCode::Internal(format!("Failed to upgrade session: {err}")))?;

        if let Some(cid) = session.get_client_session_id() {
            ClientSessionManager::instance().on_query_start(&cid, &self.user_name, &session);
        };
        if let Some(session_conf) = http_session_conf {
            session_conf.restore(&session, self).await?;
        };
        let user_agent = &self.user_agent;
        session.set_client_host(self.client_host.clone());

        let ctx = session.create_query_context(self.version).await?;

        // Deduplicate label is used on the DML queries which may be retried by the client.
        // It can be used to avoid the duplicated execution of the DML queries.
        let deduplicate_label = &self.deduplicate_label;
        if let Some(label) = deduplicate_label {
            unsafe {
                ctx.get_settings().set_deduplicate_label(label.clone())?;
            }
        }
        if let Some(ua) = user_agent {
            ctx.set_ua(ua.clone());
        }
        ctx.update_init_query_id(self.query_id.clone());
        Ok((session, ctx))
    }
}

impl<'a> FromRequest<'a> for &'a HttpQueryContext {
    #[async_backtrace::framed]
    async fn from_request(req: &'a Request, _body: &mut RequestBody) -> poem::Result<Self> {
        Ok(req.extensions().get::<HttpQueryContext>().expect(
            "To use the `HttpQueryContext` extractor, the `HTTPSessionMiddleware` is required",
        ))
    }
}
