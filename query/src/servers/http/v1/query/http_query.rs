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

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::Mutex as TokioMutex;
use common_base::tokio::sync::RwLock;
use common_base::ProgressValues;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserInfo;
use serde::Deserialize;

use crate::servers::http::v1::query::http_query_manager::HttpQueryConfig;
use crate::servers::http::v1::query::ExecuteState;
use crate::servers::http::v1::query::ExecuteStateName;
use crate::servers::http::v1::query::Executor;
use crate::servers::http::v1::query::ResponseData;
use crate::servers::http::v1::query::ResultDataManager;
use crate::servers::http::v1::query::Wait;
use crate::sessions::SessionManager;

#[derive(Deserialize, Debug)]
pub struct HttpQueryRequest {
    #[serde(default)]
    pub session: HttpSessionConf,
    pub sql: String,
    #[serde(default)]
    pub pagination: PaginationConf,
}

#[derive(Deserialize, Debug)]
pub struct PaginationConf {
    pub(crate) wait_time_secs: i32,
}

impl Default for PaginationConf {
    fn default() -> Self {
        PaginationConf { wait_time_secs: 1 }
    }
}

impl PaginationConf {
    pub(crate) fn get_wait_type(&self) -> Wait {
        let t = self.wait_time_secs;
        match t.cmp(&0) {
            Ordering::Greater => Wait::Deadline(Instant::now() + Duration::from_secs(t as u64)),
            Ordering::Equal => Wait::Async,
            Ordering::Less => Wait::Sync,
        }
    }
}

#[derive(Deserialize, Debug, Default)]
pub struct HttpSessionConf {
    pub database: Option<String>,
}

pub struct ResponseInitialState {
    pub schema: Option<DataSchemaRef>,
}

pub struct ResponseState {
    pub wall_time_ms: u128,
    pub progress: Option<ProgressValues>,
    pub state: ExecuteStateName,
    pub error: Option<ErrorCode>,
}

pub struct HttpQueryResponseInternal {
    pub data: Option<ResponseData>,
    pub initial_state: Option<ResponseInitialState>,
    pub state: ResponseState,
}

pub struct HttpQuery {
    pub(crate) id: String,
    #[allow(dead_code)]
    request: HttpQueryRequest,
    state: Arc<RwLock<Executor>>,
    data: Arc<TokioMutex<ResultDataManager>>,
    config: HttpQueryConfig,
    expire_at: Arc<TokioMutex<Option<Instant>>>,
}

impl HttpQuery {
    pub(crate) async fn try_create(
        id: &str,
        request: HttpQueryRequest,
        session_manager: &Arc<SessionManager>,
        user_info: &UserInfo,
        config: HttpQueryConfig,
    ) -> Result<Arc<HttpQuery>> {
        //TODO(youngsofun): support config/set channel size
        let (block_tx, block_rx) = mpsc::channel(10);

        let (state, schema) =
            ExecuteState::try_create(&request, session_manager, user_info, block_tx).await?;
        let data = Arc::new(TokioMutex::new(ResultDataManager::new(schema, block_rx)));
        let query = HttpQuery {
            id: id.to_string(),
            request,
            state,
            data,
            config,
            expire_at: Arc::new(TokioMutex::new(None)),
        };
        let query = Arc::new(query);
        Ok(query)
    }

    pub fn is_async(&self) -> bool {
        self.request.pagination.wait_time_secs == 0
    }

    pub async fn get_response_page(
        &self,
        page_no: usize,
        init: bool,
    ) -> Result<HttpQueryResponseInternal> {
        Ok(HttpQueryResponseInternal {
            data: Some(self.get_page(page_no).await?),
            initial_state: if init {
                Some(self.get_initial_state().await)
            } else {
                None
            },
            state: self.get_state().await,
        })
    }

    pub async fn get_response_state_only(&self) -> HttpQueryResponseInternal {
        HttpQueryResponseInternal {
            data: None,
            initial_state: None,
            state: self.get_state().await,
        }
    }

    pub async fn get_initial_state(&self) -> ResponseInitialState {
        let data = self.data.lock().await;
        ResponseInitialState {
            schema: Some(data.schema.clone()),
        }
    }

    async fn get_state(&self) -> ResponseState {
        let state = self.state.read().await;
        let (exe_state, err) = state.state.extract();
        let wall_time_ms = state.elapsed().as_millis();
        ResponseState {
            wall_time_ms,
            progress: state.get_progress(),
            state: exe_state,
            error: err,
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
        self.data.lock().await.block_rx.close();
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
