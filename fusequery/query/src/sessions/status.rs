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

use crate::configs::Config;
use crate::datasources::DataSource;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::Settings;

#[derive(PartialEq, Clone)]
pub enum State {
    Init,
    Idle,
    Progress,
    Aborting,
    Aborted,
}

pub struct SessionStatus {
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

impl SessionStatus {
    pub fn try_create() -> Result<SessionStatus> {
        Ok(SessionStatus {
            state: State::Init,
            current_database: String::from("default"),
            session_settings: Settings::try_create()?,
            stream: None,
            execute_instant: None,
            executing_query: None,
            executing_query_plan: None,
            abort_handler: None,
        })
    }

    pub fn is_aborting(&self) -> bool {
        matches!(self.state, State::Aborting | State::Aborted)
    }

    pub fn is_aborted(&self) -> bool {
        self.state == State::Aborted
    }

    pub fn enter_init(&mut self, stream: std::net::TcpStream) {
        self.state = State::Idle;
        self.stream = Some(stream);
    }

    pub fn enter_aborted(&mut self) {
        self.state = State::Aborted;
        self.abort_handler = None;
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    pub fn try_create_context(
        &mut self,
        conf: Config,
        datasource: Arc<DataSource>,
    ) -> Result<FuseQueryContextRef> {
        FuseQueryContext::from_settings(
            conf,
            self.session_settings.clone(),
            self.current_database.clone(),
            datasource,
        )
    }

    pub fn enter_query(&mut self, query: &str) {
        self.state = State::Progress;
        self.execute_instant = Some(Instant::now());
        self.executing_query = Some(query.to_string());
    }

    pub fn exit_query(&mut self) -> Result<()> {
        match self.state {
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
            State::Progress => self.state = State::Idle,
            State::Aborting | State::Aborted => {
                return Err(ErrorCode::AbortedSession(
                    "Aborting this connection. because we are try aborting server.",
                ))
            }
        };

        Ok(())
    }

    pub fn enter_interpreter(&mut self, query_plan: &PlanNode) {
        self.executing_query_plan = Some(query_plan.clone());
    }

    pub fn enter_pipeline_executor(&mut self, abort_handle: AbortHandle) {
        self.abort_handler = Some(abort_handle);
    }

    pub fn update_database(&mut self, database_name: String) {
        self.current_database = database_name;
    }

    pub fn abort_session(&mut self, force: bool) -> Result<()> {
        match self.state {
            State::Aborted => {}
            State::Init | State::Idle => {
                self.state = State::Aborting;
                if let Some(stream) = self.stream.take() {
                    stream.shutdown(Shutdown::Both)?;
                }
            }
            _ if !force => self.state = State::Aborting,
            State::Progress | State::Aborting => {
                self.state = State::Aborting;
                if let Some(abort_handle) = self.abort_handler.take() {
                    abort_handle.abort();
                }
            }
        };

        Ok(())
    }
}
