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

use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::RwLock;
use common_base::ProgressValues;
use common_base::TrySpawn;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use futures::StreamExt;
use serde::Deserialize;

use crate::interpreters::InterpreterFactory;
use crate::sessions::DatabendQueryContextRef;
use crate::sessions::SessionManagerRef;
use crate::sessions::SessionRef;
use crate::sql::PlanParser;

#[derive(Deserialize, Debug)]
pub struct HttpQueryRequest {
    pub sql: String,
}

pub(crate) enum ExecuteState {
    Running(ExecuteRunning),
    Stopped(ExecuteStopped),
}

use ExecuteState::*;

pub(crate) type ExecuteStateRef = Arc<RwLock<ExecuteStateWrapper>>;

pub(crate) struct ExecuteStopped {
    progress: Option<ProgressValues>,
    #[allow(dead_code)]
    reason: Result<()>,
}

pub(crate) struct ExecuteStateWrapper {
    pub(crate) state: ExecuteState,
}

pub const STATE_RUNNING: &str = "running";
pub const STATE_STOPPED: &str = "stopped";

impl ExecuteStateWrapper {
    pub(crate) fn get_state(&self) -> &str {
        match &self.state {
            Running(_) => STATE_RUNNING,
            Stopped(_) => STATE_STOPPED,
        }
    }

    pub(crate) fn get_schema(&self) -> Result<DataSchemaRef> {
        match &self.state {
            Running(r) => Ok(r.plan.schema()),
            Stopped(_) => Err(ErrorCode::LogicalError(
                "Query is in Stopped state, can't call get_schema",
            )),
        }
    }

    pub(crate) fn get_progress(&self) -> Option<ProgressValues> {
        match &self.state {
            Running(r) => Some(r.context.get_progress_value()),
            Stopped(f) => f.progress.clone(),
        }
    }
}

pub struct HttpQueryHandle {
    pub abort_sender: mpsc::Sender<()>,
}

impl HttpQueryHandle {
    pub fn abort(&self) {
        let sender = self.abort_sender.clone();
        tokio::spawn(async move {
            sender.send(()).await.ok();
        });
    }
}

pub(crate) struct ExecuteRunning {
    // used to kill query
    session: SessionRef,
    // mainly used to get progress for now
    context: DatabendQueryContextRef,
    plan: PlanNode,
}

impl ExecuteState {
    pub(crate) async fn try_create(
        request: &HttpQueryRequest,
        session_manager: &SessionManagerRef,
        block_tx: mpsc::Sender<DataBlock>,
    ) -> Result<ExecuteStateRef> {
        let sql = &request.sql;
        let session = session_manager.create_session("http-statement")?;
        let context = session.create_context().await?;
        context.attach_query_str(sql);

        let plan = PlanParser::create(context.clone()).build_from_sql(sql)?;

        let interpreter = InterpreterFactory::get(context.clone(), plan.clone())?;
        let data_stream = interpreter.execute().await?;
        let mut data_stream = context.try_create_abortable(data_stream)?;

        let (abort_tx, mut abort_rx) = mpsc::channel(2);
        context.attach_http_query(HttpQueryHandle {
            abort_sender: abort_tx,
        });

        let running_state = ExecuteRunning {
            session,
            context: context.clone(),
            plan,
        };
        let state = Arc::new(RwLock::new(ExecuteStateWrapper {
            state: Running(running_state),
        }));
        let state_clone = state.clone();

        context
            .try_spawn(async move {
                loop {
                    if let Some(block_r) = data_stream.next().await {
                        match block_r {
                            Ok(block) => tokio::select! {
                                _ = block_tx.send(block) => { },
                                _ = abort_rx.recv() => {
                                    ExecuteState::stop(&state, Err(ErrorCode::AbortedQuery("query aborted"))).await;
                                    break;
                                },
                            },
                            Err(err) => {
                                ExecuteState::stop(&state, Err(err)).await;
                                break
                            }
                        };
                    } else {
                        ExecuteState::stop(&state, Ok(())).await;
                        break;
                    }
                }
                log::debug!("drop block sender!");
            })?;
        Ok(state_clone)
    }

    pub(crate) async fn stop(this: &ExecuteStateRef, reason: Result<()>) {
        let mut guard = this.write().await;
        if let Running(r) = &guard.state {
            // release session
            let progress = Some(r.context.get_progress_value());
            r.session.force_kill_session();
            guard.state = Stopped(ExecuteStopped { progress, reason });
        };
    }
}
