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
use std::time::Duration;
use std::time::Instant;

use common_base::base::tokio;
use common_base::base::tokio::sync::Mutex as TokioMutex;
use common_base::base::tokio::sync::RwLock;
use common_base::runtime::GlobalQueryRuntime;
use common_base::runtime::TrySpawn;
use common_catalog::table_context::StageAttachment;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use super::HttpQueryContext;
use crate::interpreters::InterpreterQueryLog;
use crate::servers::http::v1::query::execute_state::ExecuteStarting;
use crate::servers::http::v1::query::execute_state::ExecuteStopped;
use crate::servers::http::v1::query::execute_state::Progresses;
use crate::servers::http::v1::query::expirable::Expirable;
use crate::servers::http::v1::query::expirable::ExpiringState;
use crate::servers::http::v1::query::http_query_manager::HttpQueryConfig;
use crate::servers::http::v1::query::sized_spsc::sized_spsc;
use crate::servers::http::v1::query::ExecuteState;
use crate::servers::http::v1::query::ExecuteStateKind;
use crate::servers::http::v1::query::Executor;
use crate::servers::http::v1::query::PageManager;
use crate::servers::http::v1::query::ResponseData;
use crate::servers::http::v1::query::Wait;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::QueryAffect;
use crate::sessions::SessionType;
use crate::sessions::TableContext;

fn default_as_true() -> bool {
    true
}

#[derive(Deserialize, Debug)]
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

const DEFAULT_MAX_ROWS_IN_BUFFER: usize = 5 * 1000 * 1000;
const DEFAULT_MAX_ROWS_PER_PAGE: usize = 10000;
const DEFAULT_WAIT_TIME_SECS: u32 = 1;

fn default_max_rows_in_buffer() -> usize {
    DEFAULT_MAX_ROWS_IN_BUFFER
}

fn default_max_rows_per_page() -> usize {
    DEFAULT_MAX_ROWS_PER_PAGE
}

fn default_wait_time_secs() -> u32 {
    DEFAULT_WAIT_TIME_SECS
}

#[derive(Deserialize, Debug)]
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
            wait_time_secs: 1,
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

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct HttpSessionConf {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_server_session_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<BTreeMap<String, String>>,
}

impl HttpSessionConf {
    fn apply_affect(&self, affect: &QueryAffect) -> HttpSessionConf {
        let mut ret = self.clone();
        match affect {
            QueryAffect::UseDB { name } => {
                ret.database = Some(name.to_string());
            }
            QueryAffect::ChangeSettings {
                keys,
                values,
                is_globals: _,
            } => {
                let settings = ret.settings.get_or_insert_default();
                for (key, value) in keys.iter().zip(values) {
                    settings.insert(key.to_string(), value.to_string());
                }
            }
            _ => {}
        }
        ret
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
    pub running_time_ms: f64,
    pub progresses: Progresses,
    pub state: ExecuteStateKind,
    pub affect: Option<QueryAffect>,
    pub error: Option<ErrorCode>,
}

pub struct HttpQueryResponseInternal {
    pub data: Option<ResponseData>,
    pub session_id: String,
    pub session: Option<HttpSessionConf>,
    pub state: ResponseState,
}

pub enum ExpireState {
    Working,
    ExpireAt(Instant),
    Removed,
}

pub enum ExpireResult {
    Expired,
    Sleep(Duration),
    Removed,
}

pub struct HttpQuery {
    pub(crate) id: String,
    pub(crate) session_id: String,
    request: HttpQueryRequest,
    state: Arc<RwLock<Executor>>,
    page_manager: Arc<TokioMutex<PageManager>>,
    config: HttpQueryConfig,
    expire_state: Arc<TokioMutex<ExpireState>>,
}

impl HttpQuery {
    #[async_backtrace::framed]
    pub(crate) async fn try_create(
        ctx: &HttpQueryContext,
        request: HttpQueryRequest,
        config: HttpQueryConfig,
    ) -> Result<Arc<HttpQuery>> {
        let http_query_manager = HttpQueryManager::instance();

        let session = if let Some(id) = &request.session_id {
            let session = http_query_manager.get_session(id).await.ok_or_else(|| {
                ErrorCode::UnknownSession(format!("unknown session-id {}, maybe expired", id))
            })?;
            let mut n = 1;
            while let ExpiringState::InUse(query_id) = session.expire_state() {
                if let Some(last_query) = &http_query_manager.get_query(&query_id).await {
                    if last_query.get_state().await.state == ExecuteStateKind::Running {
                        return Err(ErrorCode::BadArguments(
                            "last query on the session not finished",
                        ));
                    } else {
                        http_query_manager.remove_query(&query_id).await;
                    }
                }
                // wait for Arc<QueryContextShared> to drop and detach itself from session
                // should not take too long
                tokio::time::sleep(Duration::from_millis(1)).await;
                n += 1;
                if n > 10 {
                    return Err(ErrorCode::Internal("last query stop but not released"));
                }
            }
            session
        } else {
            ctx.get_session(SessionType::HTTPQuery)
        };

        if let Some(session_conf) = &request.session {
            if let Some(db) = &session_conf.database {
                session.set_current_database(db.clone());
            }
            if let Some(conf_settings) = &session_conf.settings {
                let settings = session.get_settings();
                for (k, v) in conf_settings {
                    settings.set_setting(k.to_string(), v.to_string())?;
                }
            }
            if let Some(secs) = session_conf.keep_server_session_secs {
                if secs > 0 && request.session_id.is_none() {
                    http_query_manager
                        .add_session(session.clone(), Duration::from_secs(secs))
                        .await;
                }
            }
        };

        let session_id = session.get_id().clone();

        let ctx = session.create_query_context().await?;
        let id = ctx.get_id();
        let sql = &request.sql;
        tracing::info!("run query_id={id} in session_id={session_id}, sql='{sql}'");

        match &request.stage_attachment {
            Some(attachment) => ctx.attach_stage(StageAttachment {
                location: attachment.location.clone(),
                file_format_options: attachment.file_format_options.clone(),
                copy_options: attachment.copy_options.clone(),
                values_str: "".to_string(),
            }),
            None => {}
        };

        let (block_sender, block_receiver) = sized_spsc(request.pagination.max_rows_in_buffer);
        let start_time = Instant::now();
        let state = Arc::new(RwLock::new(Executor {
            query_id: id.clone(),
            start_time,
            state: ExecuteState::Starting(ExecuteStarting { ctx: ctx.clone() }),
        }));
        let block_sender_closer = block_sender.closer();
        let state_clone = state.clone();
        let ctx_clone = ctx.clone();
        let ctx_clone2 = ctx.clone();
        let sql = request.sql.clone();
        let query_id = id.clone();
        let query_id_clone = id.clone();

        let schema = ExecuteState::get_schema(&sql, ctx.clone()).await?;
        let http_query_runtime_instance = GlobalQueryRuntime::instance();
        http_query_runtime_instance
            .runtime()
            .try_spawn(async move {
                let state = state_clone.clone();
                if let Err(e) = ExecuteState::try_start_query(
                    state,
                    &sql,
                    session,
                    ctx_clone.clone(),
                    block_sender,
                )
                .await
                {
                    InterpreterQueryLog::fail_to_start(ctx_clone.clone(), e.clone());
                    let state = ExecuteStopped {
                        stats: Progresses::default(),
                        reason: Err(e.clone()),
                        stop_time: Instant::now(),
                        affect: ctx_clone.get_affect(),
                    };
                    tracing::info!(
                        "http query {}, change state to Stopped, fail to start {:?}",
                        &query_id,
                        e
                    );
                    Executor::start_to_stop(&state_clone, ExecuteState::Stopped(Box::new(state)))
                        .await;
                    block_sender_closer.close();
                }
            })?;

        let format_settings = ctx.get_format_settings()?;
        let data = Arc::new(TokioMutex::new(PageManager::new(
            query_id_clone,
            request.pagination.max_rows_per_page,
            block_receiver,
            schema,
            format_settings,
            ctx_clone2,
        )));
        let query = HttpQuery {
            id,
            session_id,
            request,
            state,
            page_manager: data,
            config,
            expire_state: Arc::new(TokioMutex::new(ExpireState::Working)),
        };

        Ok(Arc::new(query))
    }

    #[async_backtrace::framed]
    pub async fn get_response_page(&self, page_no: usize) -> Result<HttpQueryResponseInternal> {
        let data = Some(self.get_page(page_no).await?);
        let state = self.get_state().await;
        let session_conf = self.request.session.clone().unwrap_or_default();
        let session_conf = if let Some(affect) = &state.affect {
            Some(session_conf.apply_affect(affect))
        } else {
            Some(session_conf)
        };

        Ok(HttpQueryResponseInternal {
            data,
            state,
            session: session_conf,
            session_id: self.session_id.clone(),
        })
    }

    #[async_backtrace::framed]
    pub async fn get_response_state_only(&self) -> HttpQueryResponseInternal {
        HttpQueryResponseInternal {
            data: None,
            session_id: self.session_id.clone(),
            state: self.get_state().await,
            session: None,
        }
    }

    #[async_backtrace::framed]
    async fn get_state(&self) -> ResponseState {
        let state = self.state.read().await;
        let (exe_state, err) = state.state.extract();
        ResponseState {
            running_time_ms: state.elapsed().as_secs_f64() * 1000.0,
            progresses: state.get_progress(),
            state: exe_state,
            error: err,
            affect: state.get_affect(),
        }
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

    #[async_backtrace::framed]
    pub async fn kill(&self) {
        Executor::stop(
            &self.state,
            Err(ErrorCode::AbortedQuery("killed by http")),
            true,
        )
        .await;
    }

    #[async_backtrace::framed]
    pub async fn detach(&self) {
        let data = self.page_manager.lock().await;
        data.detach().await
    }

    #[async_backtrace::framed]
    pub async fn update_expire_time(&self, before_wait: bool) {
        let duration = Duration::from_secs(self.config.result_timeout_secs)
            + if before_wait {
                Duration::from_secs(self.request.pagination.wait_time_secs as u64)
            } else {
                Duration::new(0, 0)
            };
        let deadline = Instant::now() + duration;
        let mut t = self.expire_state.lock().await;
        *t = ExpireState::ExpireAt(deadline);
    }

    #[async_backtrace::framed]
    pub async fn mark_removed(&self) {
        let mut t = self.expire_state.lock().await;
        *t = ExpireState::Removed;
    }

    // return Duration to sleep
    #[async_backtrace::framed]
    pub async fn check_expire(&self) -> ExpireResult {
        let expire_state = self.expire_state.lock().await;
        match *expire_state {
            ExpireState::ExpireAt(expire_at) => {
                let now = Instant::now();
                if now >= expire_at {
                    ExpireResult::Expired
                } else {
                    ExpireResult::Sleep(expire_at - now)
                }
            }
            ExpireState::Removed => ExpireResult::Removed,
            ExpireState::Working => {
                ExpireResult::Sleep(Duration::from_secs(self.config.result_timeout_secs))
            }
        }
    }
}
