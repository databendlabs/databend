// Copyright 2021 Datafuse Labs.
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
use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use super::HttpQueryContext;
use crate::interpreters::InterpreterQueryLog;
use crate::servers::http::v1::query::execute_state::Progresses;
use crate::servers::http::v1::query::expirable::Expirable;
use crate::servers::http::v1::query::expirable::ExpiringState;
use crate::servers::http::v1::query::http_query_manager::HttpQueryConfig;
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
use crate::storages::result::block_buffer::BlockBuffer;

#[derive(Deserialize, Debug)]
pub struct HttpQueryRequest {
    pub session_id: Option<String>,
    pub session: Option<HttpSessionConf>,
    pub sql: String,
    #[serde(default)]
    pub pagination: PaginationConf,
    #[serde(default)]
    pub string_fields: bool,
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
            QueryAffect::ChangeSetting {
                key,
                value,
                is_global: _,
            } => {
                let settings = ret.settings.get_or_insert_default();
                settings.insert(key.to_string(), value.to_string());
            }
            _ => {}
        }
        ret
    }
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

pub struct HttpQuery {
    pub(crate) id: String,
    pub(crate) session_id: String,
    request: HttpQueryRequest,
    state: Arc<RwLock<Executor>>,
    data: Arc<TokioMutex<PageManager>>,
    config: HttpQueryConfig,
    expire_at: Arc<TokioMutex<Option<Instant>>>,
}

impl HttpQuery {
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
                    return Err(ErrorCode::UnexpectedError(
                        "last query stop but not released",
                    ));
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
                    settings.set_settings(k.to_string(), v.to_string(), false)?;
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

        let block_buffer = BlockBuffer::new(request.pagination.max_rows_in_buffer);
        let state =
            ExecuteState::try_create(&request, session, ctx.clone(), block_buffer.clone()).await;
        match state {
            Ok(state) => {
                let format_settings = ctx.get_format_settings()?;
                let data = Arc::new(TokioMutex::new(PageManager::new(
                    request.pagination.max_rows_per_page,
                    block_buffer,
                    request.string_fields,
                    format_settings,
                )));
                let query = HttpQuery {
                    id,
                    session_id,
                    request,
                    state,
                    data,
                    config,
                    expire_at: Arc::new(TokioMutex::new(None)),
                };
                let query = Arc::new(query);
                Ok(query)
            }
            Err(e) => {
                InterpreterQueryLog::fail_to_start(ctx, e.clone()).await;
                Err(e)
            }
        }
    }

    pub fn is_async(&self) -> bool {
        self.request.pagination.wait_time_secs == 0
    }

    pub async fn get_response_page(&self, page_no: usize) -> Result<HttpQueryResponseInternal> {
        let data = Some(self.get_page(page_no).await?);
        let state = self.get_state().await;
        let session_conf = if let Some(conf) = &self.request.session {
            if let Some(affect) = &state.affect {
                Some(conf.clone().apply_affect(affect))
            } else {
                Some(conf.clone())
            }
        } else {
            None
        };
        Ok(HttpQueryResponseInternal {
            data,
            state,
            session: session_conf,
            session_id: self.session_id.clone(),
        })
    }

    pub async fn get_response_state_only(&self) -> HttpQueryResponseInternal {
        HttpQueryResponseInternal {
            data: None,
            session_id: self.session_id.clone(),
            state: self.get_state().await,
            session: None,
        }
    }

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

    async fn get_page(&self, page_no: usize) -> Result<ResponseData> {
        let mut data = self.data.lock().await;
        let page = data
            .get_a_page(page_no, &self.request.pagination.get_wait_type())
            .await?;
        let response = ResponseData {
            page,
            next_page_no: data.next_page_no(),
        };
        Ok(response)
    }

    pub async fn kill(&self) {
        Executor::stop(
            &self.state,
            Err(ErrorCode::AbortedQuery("killed by http")),
            true,
        )
        .await;
    }

    pub async fn detach(&self) {
        let data = self.data.lock().await;
        data.detach().await
    }

    pub async fn clear_expire_time(&self) {
        let mut t = self.expire_at.lock().await;
        *t = None;
    }

    pub async fn update_expire_time(&self) {
        let mut t = self.expire_at.lock().await;
        *t = Some(Instant::now() + Duration::from_millis(self.config.result_timeout_millis));
    }

    pub async fn check_expire(&self) -> Option<Duration> {
        let expire_at = self.expire_at.lock().await;
        if let Some(expire_at) = *expire_at {
            let now = Instant::now();
            if now >= expire_at {
                None
            } else {
                Some(expire_at - now)
            }
        } else {
            Some(Duration::from_millis(self.config.result_timeout_millis))
        }
    }
}
