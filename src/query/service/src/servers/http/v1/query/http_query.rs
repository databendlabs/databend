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
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::short_sql;
use databend_common_base::base::tokio::sync::Mutex as TokioMutex;
use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::GlobalQueryRuntime;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table_context::StageAttachment;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_expression::Scalar;
use databend_common_metrics::http::metrics_incr_http_response_errors_count;
use databend_common_settings::ScopeLevel;
use databend_storages_common_session::TempTblMgrRef;
use databend_storages_common_session::TxnState;
use fastrace::future::FutureExt;
use http::StatusCode;
use log::error;
use log::info;
use log::warn;
use parking_lot::Mutex;
use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::execute_state::ExecutionError;
use super::HttpQueryContext;
use super::RemoveReason;
use crate::servers::http::error::QueryError;
use crate::servers::http::v1::http_query_handlers::QueryResponseField;
use crate::servers::http::v1::query::blocks_serializer::BlocksSerializer;
use crate::servers::http::v1::query::execute_state::ExecuteStarting;
use crate::servers::http::v1::query::execute_state::ExecuteStopped;
use crate::servers::http::v1::query::execute_state::ExecutorSessionState;
use crate::servers::http::v1::query::execute_state::Progresses;
use crate::servers::http::v1::query::ExecuteState;
use crate::servers::http::v1::query::ExecuteStateKind;
use crate::servers::http::v1::query::Executor;
use crate::servers::http::v1::query::PageManager;
use crate::servers::http::v1::query::ResponseData;
use crate::servers::http::v1::query::Wait;
use crate::servers::http::v1::ClientSessionManager;
use crate::servers::http::v1::HttpQueryManager;
use crate::servers::http::v1::QueryResponse;
use crate::servers::http::v1::QueryStats;
use crate::sessions::QueryAffect;
use crate::sessions::Session;
use crate::sessions::TableContext;

fn default_as_true() -> bool {
    true
}

#[derive(Deserialize, Clone)]
pub struct HttpQueryRequest {
    pub session_id: Option<String>,
    pub session: Option<HttpSessionConf>,
    pub sql: String,
    #[serde(default)]
    pub pagination: PaginationConf,
    #[serde(default = "default_as_true")]
    pub string_fields: bool,
    pub stage_attachment: Option<StageAttachmentConf>,
}

impl HttpQueryRequest {
    pub(crate) fn fail_to_start_sql(&self, err: ErrorCode) -> impl IntoResponse {
        metrics_incr_http_response_errors_count(err.name(), err.code());
        let session = self.session.as_ref().map(|s| {
            let txn_state = if matches!(s.txn_state, Some(TxnState::Active)) {
                Some(TxnState::Fail)
            } else {
                s.txn_state.clone()
            };
            let need_sticky = match &s.internal {
                Some(internal) => internal.has_temp_table,
                None => false,
            };
            HttpSessionConf {
                txn_state,
                need_sticky,
                need_keep_alive: need_sticky,
                ..s.clone()
            }
        });
        Json(QueryResponse {
            id: "".to_string(),
            stats: QueryStats::default(),
            state: ExecuteStateKind::Failed,
            affect: None,
            data: Arc::new(BlocksSerializer::empty()),
            schema: vec![],
            session_id: None,
            warnings: vec![],
            node_id: "".to_string(),
            session,
            next_uri: None,
            stats_uri: None,
            final_uri: None,
            kill_uri: None,
            error: Some(QueryError::from_error_code(err)),
            has_result_set: None,
            result_timeout_secs: None,
        })
    }

    pub fn set_settings(&mut self, settings: BTreeMap<String, String>) {
        match self.session.as_mut() {
            None => {
                self.session = Some(HttpSessionConf {
                    settings: Some(settings),
                    ..Default::default()
                });
            }
            Some(session) => {
                session.set_settings(settings);
            }
        }
    }
}

impl Debug for HttpQueryRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("HttpQueryRequest")
            .field("session_id", &self.session_id)
            .field("session", &self.session)
            .field("sql", &short_sql(self.sql.clone(), 1000))
            .field("pagination", &self.pagination)
            .field("string_fields", &self.string_fields)
            .field("stage_attachment", &self.stage_attachment)
            .finish()
    }
}

const DEFAULT_MAX_ROWS_IN_BUFFER: usize = 5 * 1000 * 1000;
const DEFAULT_MAX_ROWS_PER_PAGE: usize = 10000;
const DEFAULT_WAIT_TIME_SECS: u32 = 10;

fn default_max_rows_in_buffer() -> usize {
    DEFAULT_MAX_ROWS_IN_BUFFER
}

fn default_max_rows_per_page() -> usize {
    DEFAULT_MAX_ROWS_PER_PAGE
}

fn default_wait_time_secs() -> u32 {
    DEFAULT_WAIT_TIME_SECS
}

#[derive(Deserialize, Debug, Clone)]
pub struct PaginationConf {
    #[serde(default = "default_wait_time_secs")]
    pub(crate) wait_time_secs: u32,
    #[serde(default = "default_max_rows_in_buffer")]
    pub(crate) max_rows_in_buffer: usize,
    #[serde(default = "default_max_rows_per_page")]
    pub(crate) max_rows_per_page: usize,
}

impl Default for PaginationConf {
    fn default() -> Self {
        PaginationConf {
            wait_time_secs: DEFAULT_WAIT_TIME_SECS,
            max_rows_in_buffer: DEFAULT_MAX_ROWS_IN_BUFFER,
            max_rows_per_page: DEFAULT_MAX_ROWS_PER_PAGE,
        }
    }
}

impl PaginationConf {
    pub(crate) fn get_wait_type(&self) -> Wait {
        let t = self.wait_time_secs;
        if t > 0 {
            Wait::Deadline(Instant::now() + Duration::from_secs(t as u64))
        } else {
            Wait::Async
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct ServerInfo {
    pub id: String,
    pub start_time: String,
}

fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct HttpSessionStateInternal {
    /// value is JSON of Scalar
    #[serde(default)]
    #[serde(skip_serializing_if = "is_default")]
    pub variables: Vec<(String, String)>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_default")]
    pub last_query_result_cache_key: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_default")]
    pub has_temp_table: bool,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_node_id: Option<String>,
    /// last_query_ids[0] is the last query id, last_query_ids[1] is the second last query id, etc.
    #[serde(default)]
    #[serde(skip_serializing_if = "is_default")]
    pub last_query_ids: Vec<String>,
}

impl HttpSessionStateInternal {
    fn new(
        variables: &HashMap<String, Scalar>,
        last_query_result_cache_key: String,
        has_temp_table: bool,
        last_node_id: Option<String>,
        last_query_ids: Vec<String>,
    ) -> Self {
        let variables = variables
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::to_string(&v).expect("fail to serialize Scalar"),
                )
            })
            .collect();
        Self {
            variables,
            last_query_result_cache_key,
            has_temp_table,
            last_node_id,
            last_query_ids,
        }
    }

    pub fn get_variables(&self) -> Result<HashMap<String, Scalar>> {
        let mut vars = HashMap::with_capacity(self.variables.len());
        for (k, v) in self.variables.iter() {
            match serde_json::from_str::<Scalar>(v) {
                Ok(s) => {
                    vars.insert(k.to_string(), s);
                }
                Err(e) => {
                    return Err(ErrorCode::BadBytes(format!(
                        "fail decode scalar from string '{v}', error: {e}"
                    )));
                }
            }
        }
        Ok(vars)
    }
}

fn serialize_as_json_string<S>(
    value: &Option<HttpSessionStateInternal>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(complex_value) => {
            let json_string =
                serde_json::to_string(complex_value).map_err(serde::ser::Error::custom)?;
            serializer.serialize_some(&json_string)
        }
        None => serializer.serialize_none(),
    }
}

fn deserialize_from_json_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<HttpSessionStateInternal>, D::Error>
where D: Deserializer<'de> {
    let json_string: Option<String> = Option::deserialize(deserializer)?;
    match json_string {
        Some(s) => {
            if s.is_empty() {
                return Ok(None);
            }
            let complex_value = serde_json::from_str(&s).map_err(serde::de::Error::custom)?;
            Ok(Some(complex_value))
        }
        None => Ok(None),
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct HttpSessionConf {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary_roles: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_state: Option<TxnState>,
    #[serde(default)]
    pub need_sticky: bool,
    #[serde(default)]
    pub need_keep_alive: bool,
    /// hide state not useful to clients
    /// so client only need to know there is a String field `internal`,
    /// which need to carry with session/conn
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(
        serialize_with = "serialize_as_json_string",
        deserialize_with = "deserialize_from_json_string"
    )]
    pub internal: Option<HttpSessionStateInternal>,
}

impl HttpSessionConf {
    pub fn set_settings(&mut self, values: BTreeMap<String, String>) {
        let Some(settings) = self.settings.as_mut() else {
            self.settings = Some(values);
            return;
        };

        settings.extend(values)
    }

    // Read the session variables in the request, and set them to the current session.
    // the session variables includes:
    // - the current catalog
    // - the current database
    // - the current role
    // - the session-level settings, like max_threads, http_handler_result_timeout_secs, etc.
    // - the session-level query cache.
    pub async fn restore(&self, session: &Arc<Session>, http_ctx: &HttpQueryContext) -> Result<()> {
        let query_id = &http_ctx.query_id;
        let http_query_manager = HttpQueryManager::instance();
        if let Some(catalog) = &self.catalog {
            if !catalog.is_empty() {
                session.set_current_catalog(catalog.clone());
            }
        }
        if let Some(db) = &self.database {
            if !db.is_empty() {
                session.set_current_database(db.clone());
            }
        }
        if let Some(role) = &self.role {
            if !role.is_empty() {
                session.set_current_role_checked(role).await?;
            }
        }
        // if the secondary_roles are None (which is the common case), it will not send any rpc on validation.
        session
            .set_secondary_roles_checked(self.secondary_roles.clone())
            .await?;
        // TODO(liyz): pass secondary roles here
        if let Some(conf_settings) = &self.settings {
            let settings = session.get_settings();
            for (k, v) in conf_settings {
                settings
                    .set_setting(k.to_string(), v.to_string())
                    .or_else(|e| {
                        if e.code() == ErrorCode::UNKNOWN_VARIABLE {
                            warn!("[HTTP-QUERY] Unknown session setting ignored: {}", k);
                            Ok(())
                        } else {
                            Err(e)
                        }
                    })?;
            }
        }
        if let Some(state) = &self.internal {
            if !state.variables.is_empty() {
                session.set_all_variables(state.get_variables()?)
            }
            if let Some(id) = state.last_query_ids.first() {
                if let Some(last_query) = http_query_manager.queries.get(id) {
                    let state = *last_query.state.lock();
                    if !matches!(
                        state,
                        HttpQueryState::Removed(RemoveReason::Finished | RemoveReason::Canceled)
                    ) {
                        warn!(
                            "[HTTP-QUERY] Last query id not finished yet, id = {}, state = {:?}",
                            id, state
                        );
                    }
                }
                if !id.is_empty() && !state.last_query_result_cache_key.is_empty() {
                    session.update_query_ids_results(
                        id.to_owned(),
                        state.last_query_result_cache_key.to_owned(),
                    );
                }
            }
            let has_temp_table = !session.temp_tbl_mgr().lock().is_empty();
            if state.has_temp_table {
                if let Some(id) = &state.last_node_id {
                    if http_query_manager.server_info.id != *id {
                        if http_ctx.fixed_coordinator_node {
                            return Err(ErrorCode::SessionLost(
                                "Temp table lost due to server restart.",
                            ));
                        } else {
                            return Err(ErrorCode::SessionLost(format!(
                                "Temp table lost due to server restart (at {}) or route error: node_id={} (expected {}); session_id={}, query_id={}, is_sticky_node={}",
                                http_query_manager.server_info.start_time,
                                http_query_manager.server_info.id, id, http_ctx.client_session_id.as_deref().unwrap_or("None"),
                                query_id,
                                http_ctx.is_sticky_node
                            )));
                        }
                    }
                } else {
                    return Err(ErrorCode::InvalidSessionState(
                        "contains temporary tables but missing last_server_info field".to_string(),
                    ));
                }
                if !has_temp_table {
                    match &http_ctx.client_session {
                        None => {
                            return Err(ErrorCode::InvalidSessionState(
                                "contains temporary tables but missing session info".to_string(),
                            ));
                        }
                        Some(s) => {
                            return    Err(ErrorCode::SessionTimeout(format!(
                                "temporary tables in session {} expired after idle for more than {} seconds, when starting query {}",
                                s.header.id, ClientSessionManager::instance().max_idle_time.as_secs(), query_id,
                            )));
                        }
                    }
                }
            } else if has_temp_table {
                warn!("[TEMP-TABLE] Found unexpected Temp table.");
            }
        }
        try_set_txn(
            &http_ctx.query_id,
            session,
            &self.txn_state,
            &self.internal.clone().unwrap_or_default(),
            &http_query_manager,
        )?;
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct StageAttachmentConf {
    /// location of the stage
    /// for example: @stage_name/path/to/file, @~/path/to/file
    pub(crate) location: String,
    pub(crate) file_format_options: Option<BTreeMap<String, String>>,
    pub(crate) copy_options: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone)]
pub struct ResponseState {
    pub has_result_set: Option<bool>,
    pub schema: Vec<QueryResponseField>,
    pub running_time_ms: i64,
    pub progresses: Progresses,
    pub state: ExecuteStateKind,
    pub affect: Option<QueryAffect>,
    pub error: Option<ErrorCode<ExecutionError>>,
    pub warnings: Vec<String>,
}

pub struct HttpQueryResponseInternal {
    pub data: Option<ResponseData>,
    pub session_id: String,
    pub session: Option<HttpSessionConf>,
    pub state: ResponseState,
    pub node_id: String,
    pub result_timeout_secs: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum HttpQueryState {
    Working,
    WaitForFinal,
    ExpireAt(Instant),
    Removed(RemoveReason),
}

#[derive(Debug)]
pub enum ExpireResult {
    Expired,
    Sleep(Duration),
    Removed,
}

pub struct HttpQuery {
    pub(crate) id: String,
    pub(crate) user_name: String,
    pub(crate) client_session_id: Option<String>,
    pub(crate) node_id: String,
    request: HttpQueryRequest,
    executor: Arc<Mutex<Executor>>,
    page_manager: Arc<TokioMutex<PageManager>>,
    state: Arc<Mutex<HttpQueryState>>,
    /// The timeout for the query result polling. In the normal case, the client driver
    /// should fetch the paginated result in a timely manner, and the interval should not
    /// exceed this result_timeout_secs.
    pub(crate) result_timeout_secs: u64,

    pub(crate) is_txn_mgr_saved: AtomicBool,

    pub(crate) temp_tbl_mgr: TempTblMgrRef,
    pub(crate) query_mem_stat: Option<Arc<MemStat>>,
    last_session_conf: Arc<Mutex<Option<HttpSessionConf>>>,
}

fn try_set_txn(
    query_id: &str,
    session: &Arc<Session>,
    txn_state: &Option<TxnState>,
    internal_state: &HttpSessionStateInternal,
    http_query_manager: &Arc<HttpQueryManager>,
) -> Result<()> {
    match txn_state {
        Some(TxnState::Active) => {
            http_query_manager.check_sticky_for_txn(&internal_state.last_node_id)?;
            let last_query_id = internal_state.last_query_ids.first().ok_or_else(|| {
                ErrorCode::InvalidSessionState(
                    "[HTTP-QUERY] Invalid transaction state: transaction is active but last_query_ids is empty".to_string(),
                )
            })?;
            if let Some(txn_mgr) = http_query_manager.get_txn(last_query_id) {
                session.set_txn_mgr(txn_mgr);
                info!(
                    "[HTTP-QUERY] Query {} continuing transaction from previous query {}",
                    query_id, last_query_id
                );
            } else {
                // the returned TxnState should be Fail
                return Err(ErrorCode::TransactionTimeout(format!(
                    "[HTTP-QUERY] Transaction timeout: last_query_id {} not found on this server",
                    last_query_id
                )));
            }
        }
        Some(TxnState::Fail) => session.txn_mgr().lock().force_set_fail(),
        _ => {}
    }
    Ok(())
}

impl HttpQuery {
    #[async_backtrace::framed]
    #[fastrace::trace(name = "HttpQuery::try_create")]
    pub async fn try_create(
        http_ctx: &HttpQueryContext,
        req: HttpQueryRequest,
    ) -> Result<HttpQuery> {
        let (session, ctx) = http_ctx
            .create_session(&req.session, SessionType::HTTPQuery)
            .await?;
        let query_id = http_ctx.query_id.clone();
        let client_session_id = http_ctx.client_session_id.as_deref().unwrap_or("None");
        let node_id = ctx.get_cluster().local_id.clone();
        let sql = &req.sql;
        info!(query_id = query_id, session_id = client_session_id, node_id = node_id, sql = sql; "[HTTP-QUERY] Creating new query");

        // Stage attachment is used to carry the data payload to the INSERT/REPLACE statements.
        // When stage attachment is specified, the query may look like `INSERT INTO mytbl VALUES;`,
        // and the data in the stage attachment (which is mostly a s3 path) will be inserted into
        // the table.
        if let Some(attachment) = &req.stage_attachment {
            ctx.attach_stage(StageAttachment {
                location: attachment.location.clone(),
                file_format_options: attachment.file_format_options.as_ref().map(|v| {
                    v.iter()
                        .map(|(k, v)| (k.to_lowercase(), v.to_owned()))
                        .collect::<BTreeMap<_, _>>()
                }),
                copy_options: attachment.copy_options.clone(),
            })
        };

        let (page_manager, sender) = PageManager::create(&req.pagination);
        let executor = Arc::new(Mutex::new(Executor {
            query_id: query_id.clone(),
            state: ExecuteState::Starting(ExecuteStarting {
                ctx: ctx.clone(),
                sender: Some(sender),
            }),
        }));

        let settings = session.get_settings();
        let result_timeout_secs = settings.get_http_handler_result_timeout_secs()?;

        Ok(HttpQuery {
            id: query_id,
            user_name: http_ctx.user_name.clone(),
            client_session_id: http_ctx.client_session_id.clone(),
            node_id,
            request: req,
            executor,
            page_manager: Arc::new(TokioMutex::new(page_manager)),
            result_timeout_secs,

            state: Arc::new(Mutex::new(HttpQueryState::Working)),
            temp_tbl_mgr: session.temp_tbl_mgr().clone(),
            query_mem_stat: ctx.get_query_memory_tracking(),
            is_txn_mgr_saved: Default::default(),
            last_session_conf: Default::default(),
        })
    }

    #[async_backtrace::framed]
    #[fastrace::trace(name = "HttpQuery::get_response_page",properties = {"page_no": "{page_no}"})]
    pub async fn get_response_page(&self, page_no: usize) -> Result<HttpQueryResponseInternal> {
        let data = Some(self.get_page(page_no).await?);
        let state = self.get_state();
        let session = self.get_response_session().await?;
        let session = {
            let mut last = self.last_session_conf.lock();
            if Some(&session) == last.as_ref() {
                None
            } else {
                *last = Some(session.clone());
                Some(session)
            }
        };

        Ok(HttpQueryResponseInternal {
            data,
            state,
            session,
            node_id: self.node_id.clone(),
            session_id: self.client_session_id.clone().unwrap_or_default(),
            result_timeout_secs: self.result_timeout_secs,
        })
    }

    pub fn get_response_state_only(&self) -> Result<HttpQueryResponseInternal> {
        let state = self.get_state();

        Ok(HttpQueryResponseInternal {
            data: None,
            session_id: self.client_session_id.clone().unwrap_or_default(),
            node_id: self.node_id.clone(),
            state,
            session: None,
            result_timeout_secs: self.result_timeout_secs,
        })
    }

    fn get_state(&self) -> ResponseState {
        let state = self.executor.lock();
        state.get_response_state()
    }

    #[async_backtrace::framed]
    async fn get_response_session(&self) -> Result<HttpSessionConf> {
        // reply the updated session state, includes:
        // - current_database: updated by USE XXX;
        // - role: updated by SET ROLE;
        // - secondary_roles: updated by SET SECONDARY ROLES ALL|NONE;
        // - settings: updated by SET XXX = YYY;
        // - query cache: last_query_id and result_scan

        let (session_state, is_stopped) = {
            let executor = self.executor.lock();

            let session_state = executor.get_session_state();
            let is_stopped = matches!(executor.state, ExecuteState::Stopped(_));

            (session_state, is_stopped)
        };

        let settings = session_state
            .settings
            .as_ref()
            .into_iter()
            .filter(|item| matches!(item.level, ScopeLevel::Session))
            .map(|item| (item.name.to_string(), item.user_value.as_string()))
            .collect::<BTreeMap<_, _>>();
        let catalog = session_state.current_catalog.clone();
        let database = session_state.current_database.clone();
        let role = session_state.current_role.clone();
        let secondary_roles = session_state.secondary_roles.clone();
        let txn_state = session_state.txn_manager.lock().state();
        let has_temp_table = !self.temp_tbl_mgr.lock().is_empty();

        let internal = HttpSessionStateInternal::new(
            &session_state.variables,
            session_state.last_query_result_cache_key,
            has_temp_table,
            Some(HttpQueryManager::instance().server_info.id.clone()),
            if session_state.last_query_ids.is_empty() {
                vec![self.id.clone()]
            } else {
                session_state.last_query_ids
            },
        );

        if is_stopped
            && txn_state == TxnState::Active
            && !self.is_txn_mgr_saved.load(Ordering::Relaxed)
            && self
                .is_txn_mgr_saved
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
        {
            let timeout = session_state
                .settings
                .get_idle_transaction_timeout_secs()
                .unwrap();
            HttpQueryManager::instance()
                .add_txn(self.id.clone(), session_state.txn_manager.clone(), timeout)
                .await;
        }

        let need_sticky = txn_state == TxnState::Active || has_temp_table;
        let need_keep_alive = need_sticky;

        Ok(HttpSessionConf {
            catalog: Some(catalog),
            database: Some(database),
            role,
            secondary_roles,
            settings: Some(settings),
            txn_state: Some(txn_state),
            need_sticky,
            need_keep_alive,

            internal: Some(internal),
        })
    }

    #[async_backtrace::framed]
    async fn get_page(&self, page_no: usize) -> Result<ResponseData> {
        let mut page_manager = self.page_manager.lock().await;
        let page = page_manager
            .get_a_page(page_no, &self.request.pagination.get_wait_type())
            .await?;
        let response = ResponseData {
            page,
            next_page_no: page_manager.next_page_no(),
        };
        Ok(response)
    }

    pub async fn start_query(&mut self, sql: String) -> Result<()> {
        let span = fastrace::Span::enter_with_local_parent("HttpQuery::start_query");
        let (block_sender, query_context) = {
            let state = &mut self.executor.lock().state;
            let ExecuteState::Starting(state) = state else {
                return Err(ErrorCode::Internal(
                    "[HTTP-QUERY] Invalid query state: expected Starting state",
                ));
            };

            (state.sender.take().unwrap(), state.ctx.clone())
        };

        let query_session = query_context.get_current_session();

        let query_state = self.executor.clone();

        GlobalQueryRuntime::instance().runtime().try_spawn(
            async move {
                let block_sender_closer = block_sender.closer();
                if let Err(e) = CatchUnwindFuture::create(ExecuteState::try_start_query(
                    query_state.clone(),
                    sql,
                    query_session,
                    query_context.clone(),
                    block_sender,
                ))
                .await
                .with_context(|| "failed to start query")
                .flatten()
                {
                    crate::interpreters::hook_clear_m_cte_temp_table(&query_context)
                        .inspect_err(|e| warn!("clear_m_cte_temp_table fail: {e}"))
                        .ok();
                    let state = ExecuteStopped {
                        stats: Progresses::default(),
                        schema: vec![],
                        has_result_set: None,
                        reason: Err(e.clone()),
                        session_state: ExecutorSessionState::new(
                            query_context.get_current_session(),
                        ),
                        query_duration_ms: query_context.get_query_duration_ms(),
                        affect: query_context.get_affect(),
                        warnings: query_context.pop_warnings(),
                    };

                    error!(
                        "[HTTP-QUERY] Query state changed to Stopped, failed to start: {:?}",
                        e
                    );
                    Executor::start_to_stop(&query_state, ExecuteState::Stopped(Box::new(state)));
                    block_sender_closer.abort();
                }
            }
            .in_span(span),
            None,
        )?;

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn kill(&self, reason: ErrorCode) {
        // the query will be removed from the query manager before the session is dropped.
        self.page_manager.lock().await.detach().await;

        Executor::stop(&self.executor, Err(reason));
    }

    #[async_backtrace::framed]
    pub async fn update_expire_time(&self, before_wait: bool) {
        let duration = Duration::from_secs(self.result_timeout_secs)
            + if before_wait {
                Duration::from_secs(self.request.pagination.wait_time_secs as u64)
            } else {
                Duration::new(0, 0)
            };
        let deadline = Instant::now() + duration;
        let mut t = self.state.lock();
        *t = HttpQueryState::ExpireAt(deadline);
    }

    pub fn mark_removed(&self, remove_reason: RemoveReason) -> bool {
        let mut t = self.state.lock();
        if !matches!(*t, HttpQueryState::Removed(_)) {
            *t = HttpQueryState::Removed(remove_reason);
            true
        } else {
            false
        }
    }

    pub fn wait_for_final(&self) {
        let mut t = self.state.lock();
        *t = HttpQueryState::WaitForFinal;
    }

    pub fn check_removed(&self) -> Option<RemoveReason> {
        let t = self.state.lock();
        if let HttpQueryState::Removed(r) = *t {
            Some(r)
        } else {
            None
        }
    }

    // return Duration to sleep
    #[async_backtrace::framed]
    pub async fn check_expire(&self) -> ExpireResult {
        let expire_state = self.state.lock();
        match *expire_state {
            HttpQueryState::ExpireAt(expire_at) => {
                let now = Instant::now();
                if now >= expire_at {
                    ExpireResult::Expired
                } else {
                    ExpireResult::Sleep(expire_at - now)
                }
            }
            HttpQueryState::Removed(_) => ExpireResult::Removed,
            _ => ExpireResult::Sleep(Duration::from_secs(self.result_timeout_secs)),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace(name = "HttpQuery::on_heartbeat")]
    pub fn on_heartbeat(&self) -> bool {
        let mut expire_state = self.state.lock();
        match *expire_state {
            HttpQueryState::ExpireAt(_) => {
                let duration = Duration::from_secs(self.result_timeout_secs);
                let deadline = Instant::now() + duration;
                *expire_state = HttpQueryState::ExpireAt(deadline);
                true
            }
            HttpQueryState::Removed(_) => false,
            HttpQueryState::Working | HttpQueryState::WaitForFinal => true,
        }
    }

    pub fn check_client_session_id(&self, id: &Option<String>) -> poem::error::Result<()> {
        if *id != self.client_session_id {
            return Err(poem::error::Error::from_string(
                format!(
                    "[HTTP-QUERY] Authentication error: wrong client_session_id, expected {:?}, got {id:?}",
                    &self.client_session_id
                ),
                StatusCode::UNAUTHORIZED,
            ));
        }
        Ok(())
    }
}
