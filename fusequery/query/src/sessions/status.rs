// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::Shutdown;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use futures::future::AbortHandle;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::datasources::DataSource;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::Settings;
use common_infallible::Mutex;

#[derive(PartialEq, Clone)]
pub enum State {
    Init,
    Idle,
    Progress,
    Aborting,
    Aborted,
}

struct SessionStatusInner {
    state: State,
    current_database: String,
    session_settings: Arc<Settings>,

    stream: Option<TcpStream>,
    #[allow(unused)]
    execute_instant: Option<Instant>,
    #[allow(unused)]
    executing_query: Option<String>,
    #[allow(unused)]
    executing_query_plan: Option<PlanNode>,
    abort_handler: Option<AbortHandle>,
}

#[derive(Clone)]
pub struct SessionStatus {
    inner: Arc<Mutex<SessionStatusInner>>,
}

impl SessionStatus {
    pub fn try_create() -> Result<SessionStatus> {
        Ok(SessionStatus {
            inner: Arc::new(Mutex::new(SessionStatusInner {
                state: State::Init,
                current_database: String::from("default"),
                session_settings: Settings::try_create()?,
                stream: None,
                execute_instant: None,
                executing_query: None,
                executing_query_plan: None,
                abort_handler: None,
            }))
        })
    }

    pub fn is_aborting(&self) -> bool {
        match self.inner.lock().state {
            State::Aborting | State::Aborted => true,
            _ => false
        }
    }

    pub fn is_aborted(&self) -> bool {
        self.inner.lock().state == State::Aborted
    }

    pub fn enter_init(&self, stream: std::net::TcpStream) {
        let mut inner = self.inner.lock();
        inner.state = State::Idle;
        inner.stream = Some(stream);
    }

    pub fn aborted(&self) {
        let mut inner = self.inner.lock();
        inner.state = State::Aborted;
        inner.abort_handler = None;
        if let Some(stream) = inner.stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    pub fn try_create_context(
        &self,
        conf: Config,
        cluster: ClusterRef,
        datasource: Arc<DataSource>,
    ) -> Result<FuseQueryContextRef> {
        let mut inner = self.inner.lock();

        FuseQueryContext::from_settings(
            conf,
            inner.session_settings.clone(),
            inner.current_database.clone(),
            datasource,
        )
        .and_then(|context| context.with_cluster(cluster))
    }

    pub fn enter_query(&self, query: &str) {
        let mut inner = self.inner.lock();
        inner.state = State::Progress;
        inner.execute_instant = Some(Instant::now());
        inner.executing_query = Some(query.to_string());
    }

    pub fn exit_query(&self) -> Result<()> {
        let mut inner = self.inner.lock();

        match inner.state {
            State::Init => {
                return Err(ErrorCode::LogicalError(
                    "Logical error: exit_query with Init state",
                ))
            }
            State::Idle => {
                return Err(ErrorCode::LogicalError(
                    "Logical error: exit_query with Idle state",
                ))
            }
            State::Progress => inner.state = State::Idle,
            State::Aborting | State::Aborted => {
                return Err(ErrorCode::AbortedSession(
                    "Aborting this connection. because we are try aborting server.",
                ))
            }
        };

        Ok(())
    }

    pub fn enter_interpreter(&self, query_plan: &PlanNode) {
        let mut inner = self.inner.lock();
        inner.executing_query_plan = Some(query_plan.clone());
    }

    pub fn enter_pipeline_executor(&mut self, abort_handle: AbortHandle) {
        let mut inner = self.inner.lock();
        inner.abort_handler = Some(abort_handle);
    }

    pub fn update_database(&self, database_name: String) {
        let mut inner = self.inner.lock();
        inner.current_database = database_name;
    }

    pub fn abort_session(&self, force: bool) -> Result<()> {
        let mut inner = self.inner.lock();
        match inner.state {
            State::Aborted => {}
            State::Init | State::Idle => {
                inner.state = State::Aborting;
                if let Some(stream) = inner.stream.take() {
                    stream.shutdown(Shutdown::Both)?;
                }
            }
            _ if !force => inner.state = State::Aborting,
            State::Progress | State::Aborting => {
                inner.state = State::Aborting;
                if let Some(abort_handle) = inner.abort_handler.take() {
                    abort_handle.abort();
                }
            }
        };

        Ok(())
    }
}
