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

//! SQL KILL identifiers, authorization, and compatibility.
//!
//! # Identifiers and lookup
//!
//! Server-generated internal session ids use `p-<UUID>` (`p` for the SQL process,
//! not an OS PID), as exposed by `connection_id()` and `system.processes.id`.
//! HTTP login/client-session ids and MySQL numeric protocol ids are unchanged;
//! temporary-table keys and logs treat internal session ids as opaque strings.
//! Client query ids may use any prefix, including `p-`; there is no reserved
//! query-id namespace. Hex, UUID and UUID-plus-sequence ids remain supported.
//!
//! `KillTarget` resolves a node-local MySQL `u32` connection id first. For
//! KILL QUERY, `p-` ids prefer a session match and fall back to query lookup;
//! other strings prefer a query match and fall back to a legacy session id.
//! The prefix is only a lookup-order hint. If both kinds exist on a node, the
//! preferred match wins. Only absence triggers the alternate identifier lookup:
//! denial or a failed conditional cancellation never tries the other kind.
//! Numeric query ids that parse as `u32` retain connection-id semantics.
//! KILL CONNECTION looks up sessions regardless of prefix. Authorized query-only
//! matches return `BadArguments` rather than disconnecting their sessions.
//!
//! Lookup is scoped to the requester's tenant, including before identifier
//! fallback and query-only CONNECTION errors. Other tenants' sessions and queries
//! count as absent, so their ids cannot hide a match in the requester's tenant.
//! Lookup includes queued queries but excludes FlightRPC fragment sessions,
//! which are not registered as active client sessions. A local in-tenant match
//! stops the search; otherwise warehouse peers are searched until one matches.
//! Numeric MySQL connection ids are never forwarded.
//!
//! # Authorization and cancellation
//!
//! Requester identity requires a nonempty matching tenant, even for SUPER.
//! Within that tenant, ownership compares username and hostname; other targets,
//! including userless sessions, require global SUPER (including role grants).
//! Duplicate query IDs are resolved among in-tenant candidates by choosing an
//! authorized session. If none is authorized, the query match still denies KILL;
//! it does not fall back to a sid or a peer.
//! Authorization precedes queue removal and cancellation. Same-tenant denial
//! returns `PermissionDenied` (1063); missing and cross-tenant targets both return
//! `UnknownSession` (1053). This exposes existence only within the same tenant,
//! including beyond rows visible to the caller in `system.processes`.
//! Ordinary client sessions use the configured tenant; runtime tenant overrides
//! are restricted to management/sandbox modes and internal Local sessions.
//!
//! Query-id cancellation checks and pins the matching context under the session's
//! context-slot lock. Queue entries and semaphore acquirers use an immutable UUID
//! from that shared context, not the client query ID. Removal after releasing the
//! lock cannot touch another request reusing that ID, even on the same session.
//! Only that pinned context is canceled, never the connection's next statement.
//!
//! # Flight and rolling upgrades
//!
//! SUPER retains the legacy action, with upgraded senders appending optional
//! requester access fields to the unchanged top-level plan fields. Upgraded
//! receivers check that tenant; old receivers ignore the extra fields. Trusted
//! old requests without access fields are limited to the receiver's configured
//! tenant, not a wildcard. Their actual requester tenant cannot be recovered:
//! identity-less old senders and old receivers remain an isolation limitation.
//!
//! Non-SUPER uses only `/actions/kill_query_v2`, carrying tenant, user identity
//! and SUPER status. Unsupported v2 is treated as not found, with no legacy
//! fallback. Old coordinators cannot resolve query-id KILL; use their actual
//! session ids subject to the old node's privilege requirements.
//!
//! # HTTP query lifecycle
//!
//! SQL KILL does not call or change the HTTP query-kill endpoint. After stopping,
//! `/page` and `/final` return normal HTTP 200 envelopes with `Failed` and
//! `AbortedQuery` (1043). Query/session/MySQL-id KILL QUERY and explicit queue
//! removal use `Query canceled by KILL`. KILL CONNECTION also closes a queued
//! session after removal; other connection-kill/shutdown errors retain their semantics.
//! Buffered rows do not make failure succeed; the query-log event is `Aborted`.
//! Clients still finalize results: finalization
//! decrements the active count, and HttpQueryManager removes retained records
//! after its existing timeout. The HTTP `/kill` closed-query response is unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_sql::plans::KillPlan;
use serde::Deserialize;
use serde::Serialize;

use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::actions::KILL_QUERY;
use crate::servers::flight::v1::actions::KILL_QUERY_V2;
use crate::servers::flight::v1::actions::KillQueryRequest;
use crate::servers::flight::v1::actions::LegacyKillQueryRequest;
use crate::sessions::INTERNAL_SESSION_ID_PREFIX;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;

/// Authorization captured on the requesting node and carried over authenticated Flight RPC.
#[derive(Clone, Serialize, Deserialize)]
pub struct KillQueryAccess {
    pub tenant: String,
    pub user: UserIdentity,
    pub is_super: bool,
}

impl KillQueryAccess {
    pub fn can_kill(&self, target_tenant: &str, target_user: Option<&UserIdentity>) -> bool {
        !self.tenant.is_empty()
            && self.tenant == target_tenant
            && (self.is_super || target_user == Some(&self.user))
    }
}

/// The internal session prefix selects lookup order, not a reserved namespace.
/// Numeric ids retain MySQL semantics and are never resolved as query ids.
enum KillTarget<'a> {
    MySqlConnection(u32),
    SessionOrQuery(&'a str),
    QueryOrSession(&'a str),
}

impl<'a> KillTarget<'a> {
    fn parse(id: &'a str) -> Self {
        if let Ok(id) = id.parse::<u32>() {
            Self::MySqlConnection(id)
        } else if id.starts_with(INTERNAL_SESSION_ID_PREFIX) {
            Self::SessionOrQuery(id)
        } else {
            Self::QueryOrSession(id)
        }
    }
}

pub struct KillInterpreter {
    ctx: Arc<QueryContext>,
    plan: KillPlan,
    proxy_to_warehouse: bool,
    access: Option<KillQueryAccess>,
}

impl KillInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: KillPlan) -> Result<Self> {
        Ok(KillInterpreter {
            ctx,
            plan,
            proxy_to_warehouse: true,
            access: None,
        })
    }

    pub fn from_flight(
        ctx: Arc<QueryContext>,
        plan: KillPlan,
        access: KillQueryAccess,
    ) -> Result<Self> {
        Ok(KillInterpreter {
            ctx,
            plan,
            proxy_to_warehouse: false,
            access: Some(access),
        })
    }

    #[async_backtrace::framed]
    async fn kill_warehouse_query(&self, access: &KillQueryAccess) -> Result<PipelineBuildResult> {
        let settings = self.ctx.get_settings();
        let warehouse = self.ctx.get_warehouse_cluster().await?;

        let flight_params = FlightParams {
            timeout: settings.get_flight_client_timeout()?,
            retry_times: settings.get_flight_max_retry_times()?,
            retry_interval: settings.get_flight_retry_interval()?,
            keep_alive: settings.get_flight_keep_alive_params()?,
            reuse_control_channel: false,
        };

        for node_info in &warehouse.nodes {
            if node_info.id == warehouse.local_id {
                continue;
            }

            let result = if access.is_super {
                // Keep the legacy action for old receivers, which ignore the extra access
                // field. Upgraded receivers enforce the sender's tenant for SUPER too.
                let request = LegacyKillQueryRequest {
                    plan: self.plan.clone(),
                    access: Some(access.clone()),
                };
                let message = HashMap::from([(node_info.id.clone(), request)]);
                warehouse
                    .do_action::<_, bool>(KILL_QUERY, message, flight_params)
                    .await
            } else {
                let request = KillQueryRequest {
                    plan: self.plan.clone(),
                    access: access.clone(),
                };
                let message = HashMap::from([(node_info.id.clone(), request)]);
                warehouse
                    .do_action::<_, bool>(KILL_QUERY_V2, message, flight_params)
                    .await
            };

            match result {
                Ok(res) if res.values().any(|found| *found) => {
                    return Ok(PipelineBuildResult::create());
                }
                // During rolling upgrades an old node cannot authorize this request. Never
                // fall back to the legacy action. Continue looking on other (upgraded) nodes.
                Err(cause) if !access.is_super && cause.code() == ErrorCode::UNIMPLEMENTED => {}
                Err(cause) => return Err(cause),
                Ok(_) => {}
            }
        }

        Err(self.unknown_session())
    }

    #[async_backtrace::framed]
    async fn execute_kill(&self) -> Result<PipelineBuildResult> {
        let access = match &self.access {
            Some(access) => access.clone(),
            None => {
                let session = self.ctx.get_current_session();
                KillQueryAccess {
                    tenant: session.get_current_tenant().tenant_name().to_string(),
                    user: session.get_current_user()?.identity(),
                    is_super: session
                        .has_privilege(&GrantObject::Global, UserPrivilegeType::Super, false)
                        .await?,
                }
            }
        };

        if access.tenant.is_empty() {
            return Err(self.unknown_session());
        }

        let manager = SessionManager::instance();
        let session_in_tenant = |id: &str| {
            manager
                .get_session_by_id(id)
                .filter(|session| session.get_current_tenant().tenant_name() == access.tenant)
        };
        let query_in_tenant = |id: &str| {
            let mut denied = None;
            for session in manager.get_sessions_by_query_id(&access.tenant, id) {
                let user = session.get_current_user().ok().map(|user| user.identity());
                if access.can_kill(session.get_current_tenant().tenant_name(), user.as_ref()) {
                    return Some(session);
                }
                // Keep a denied match so it cannot turn into sid/peer fallback.
                denied.get_or_insert(session);
            }
            denied
        };
        let (target, by_query_id) = match KillTarget::parse(&self.plan.id) {
            KillTarget::MySqlConnection(id) => {
                // MySQL connection ids are node-local. Do not broadcast them to peers.
                let session_id = manager
                    .get_id_by_mysql_conn_id(&Some(id))
                    .ok_or_else(|| self.unknown_session())?;
                // The mapping can outlive the session briefly during concurrent
                // cleanup. A stale numeric id must not fall through to peer lookup.
                let session =
                    session_in_tenant(&session_id).ok_or_else(|| self.unknown_session())?;
                (Some(session), false)
            }
            KillTarget::SessionOrQuery(id) | KillTarget::QueryOrSession(id)
                if self.plan.kill_connection =>
            {
                // CONNECTION accepts sessions only. A query-only match is used just
                // to return a clear error; it must never disconnect that session.
                match session_in_tenant(id) {
                    Some(session) => (Some(session), false),
                    None => (query_in_tenant(id), true),
                }
            }
            KillTarget::SessionOrQuery(id) => match session_in_tenant(id) {
                Some(session) => (Some(session), false),
                None => (query_in_tenant(id), true),
            },
            KillTarget::QueryOrSession(id) => match query_in_tenant(id) {
                Some(session) => (Some(session), true),
                None => (session_in_tenant(id), false),
            },
        };

        let Some(kill_session) = target else {
            return match self.proxy_to_warehouse {
                true => self.kill_warehouse_query(&access).await,
                false => Err(self.unknown_session()),
            };
        };
        self.check_access(&access, &kill_session)?;
        if by_query_id && self.plan.kill_connection {
            return Err(ErrorCode::BadArguments(
                "KILL CONNECTION requires a connection id or session id, not a query id",
            ));
        }

        let cause = ErrorCode::AbortedQuery("Query canceled by KILL");
        if by_query_id {
            return match kill_session.force_kill_query_if_id(&self.plan.id, cause) {
                true => Ok(PipelineBuildResult::create()),
                false => Err(self.unknown_session()),
            };
        }

        if let Some(key) = kill_session.get_current_query_queue_key() {
            if QueriesQueueManager::instance().remove(key) {
                if self.plan.kill_connection {
                    // Close queued connections too. Use the removal cause for the
                    // context so either the queue or its abort watcher reports it.
                    kill_session.force_kill_query(cause);
                    kill_session.kill();
                }
                return Ok(PipelineBuildResult::create());
            }
        }

        if self.plan.kill_connection {
            kill_session.force_kill_session();
        } else {
            kill_session.force_kill_query(cause);
        }
        Ok(PipelineBuildResult::create())
    }

    fn check_access(&self, access: &KillQueryAccess, kill_session: &Session) -> Result<()> {
        let target_tenant = kill_session.get_current_tenant();
        if access.tenant.is_empty() || access.tenant != target_tenant.tenant_name() {
            return Err(self.unknown_session());
        }
        let target_user = kill_session
            .get_current_user()
            .ok()
            .map(|user| user.identity());
        if !access.can_kill(target_tenant.tenant_name(), target_user.as_ref()) {
            return Err(ErrorCode::PermissionDenied(format!(
                "Permission denied: not owner of target {}; global SUPER privilege is required",
                self.plan.id
            )));
        }
        Ok(())
    }

    fn unknown_session(&self) -> ErrorCode {
        let message = match self.plan.id.parse::<u32>() {
            Ok(id) => format!("MySQL connection id {} not found session id", id),
            Err(_) => format!("Not found query or session id {}", self.plan.id),
        };
        ErrorCode::UnknownSession(message)
    }
}

#[async_trait::async_trait]
impl Interpreter for KillInterpreter {
    fn name(&self) -> &str {
        "KillInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(self.execute_kill())
    }
}
