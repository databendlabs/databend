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

use std::sync::Arc;

use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::Mutex as TokioMutex;
use common_base::ProgressValues;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::servers::http::v1::query::execute_state::ExecuteState;
use crate::servers::http::v1::query::execute_state::ExecuteStateName;
use crate::servers::http::v1::query::execute_state::ExecuteStateRef;
use crate::servers::http::v1::query::execute_state::HttpQueryRequest;
use crate::servers::http::v1::query::result_data_manager::ResponseData;
use crate::servers::http::v1::query::result_data_manager::ResultDataManager;
use crate::servers::http::v1::query::result_data_manager::Wait;
use crate::sessions::SessionManagerRef;

pub struct ResponseInitialState {
    pub schema: Option<DataSchemaRef>,
}

pub struct ResponseState {
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
    state: ExecuteStateRef,
    data: Arc<TokioMutex<ResultDataManager>>,
}

pub type HttpQueryRef = Arc<HttpQuery>;

impl HttpQuery {
    pub(crate) async fn try_create(
        id: String,
        request: HttpQueryRequest,
        session_manager: &SessionManagerRef,
    ) -> Result<HttpQueryRef> {
        //TODO(youngsofun): support config/set channel size
        let (block_tx, block_rx) = mpsc::channel(10);

        let (state, schema) = ExecuteState::try_create(&request, session_manager, block_tx).await?;
        let data = Arc::new(TokioMutex::new(ResultDataManager::new(schema, block_rx)));
        let query = HttpQuery {
            id,
            request,
            state,
            data,
        };
        let query = Arc::new(query);
        Ok(query)
    }

    pub async fn get_response_page(
        &self,
        page_no: usize,
        wait: &Wait,
        init: bool,
    ) -> Result<HttpQueryResponseInternal> {
        Ok(HttpQueryResponseInternal {
            data: Some(self.get_page(page_no, wait).await?),
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

    pub async fn get_state(&self) -> ResponseState {
        let state = self.state.read().await;
        let (exe_state, err) = state.state.extract();
        ResponseState {
            progress: state.get_progress(),
            state: exe_state,
            error: err,
        }
    }

    pub async fn get_page(&self, page_no: usize, tp: &Wait) -> Result<ResponseData> {
        let mut data = self.data.lock().await;
        let page = data.get_a_page(page_no, tp).await?;
        let response = ResponseData {
            page,
            next_page_no: data.next_page_no(),
        };
        Ok(response)
    }

    pub async fn kill(&self) {
        ExecuteState::stop(
            &self.state,
            Err(ErrorCode::AbortedQuery("killed by http")),
            true,
        )
        .await;
        self.data.lock().await.block_rx.close();
    }
}
