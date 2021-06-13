use crate::sessions::{FuseQueryContextRef, FuseQueryContext, Settings};
use futures::future::{AbortHandle, Aborted};
use common_exception::{Result, ErrorCode};
use common_planners::PlanNode;
use crate::clusters::ClusterRef;
use std::time::Instant;
use std::net::{TcpStream, Shutdown};
use std::sync::Arc;
use crate::datasources::IDataSource;

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
    session_settings: Settings,

    stream: Option<TcpStream>,
    execute_instant: Option<Instant>,
    executing_query: Option<String>,
    executing_query_plan: Option<PlanNode>,
    abort_handler: Option<AbortHandle>,
}

impl SessionStatus {
    pub fn try_create() -> Result<SessionStatus> {
        let cpus = num_cpus::get();
        let session_settings = Settings::create();
        session_settings.try_set_u64("max_threads", cpus as u64, "The maximum number of threads to execute the request. By default, it is determined automatically.".to_string())?;

        Ok(SessionStatus {
            state: State::Init,
            current_database: String::from("default"),
            session_settings: session_settings,
            stream: None,
            execute_instant: None,
            executing_query: None,
            executing_query_plan: None,
            abort_handler: None,
        })
    }

    pub fn is_aborting(&self) -> bool {
        match self.state {
            State::Aborting | State::Aborted => true,
            _ => false
        }
    }

    pub fn is_aborted(&self) -> bool {
        self.state == State::Aborted
    }

    pub fn enter_init(&mut self, stream: std::net::TcpStream) {
        self.state = State::Idle;
        self.stream = Some(stream);
    }

    pub fn try_create_context(
        &mut self,
        cluster: ClusterRef,
        datasource: Arc<dyn IDataSource>,
    ) -> Result<FuseQueryContextRef> {
        Ok(FuseQueryContext::from_settings(
            self.session_settings.clone(),
            self.current_database.clone(), datasource)?
            .with_cluster(cluster)?
        )
    }

    pub fn enter_query(&mut self, query: &str) {
        self.state = State::Progress;
        self.execute_instant = Some(Instant::now());
        self.executing_query = Some(query.to_string());
    }

    pub fn exit_query(&mut self) -> Result<()> {
        match self.state {
            State::Init => return Err(ErrorCode::LogicalError("Logical error: exit_query with Init state")),
            State::Idle => return Err(ErrorCode::LogicalError("Logical error: exit_query with Idle state")),
            State::Progress => self.state = State::Idle,
            State::Aborting => self.state = State::Aborted,
            State::Aborted => self.state = State::Aborted,
        };

        Ok(())
    }

    pub fn enter_interpreter(&mut self, query_plan: &PlanNode) {
        self.executing_query_plan = Some(query_plan.clone());
    }

    pub fn enter_fetch_data(&mut self, abort_handle: AbortHandle) {
        self.abort_handler = Some(abort_handle);
    }

    pub fn abort_session(&mut self, force: bool) -> Result<()> {
        match self.state {
            State::Aborted => {},
            _ if !force => self.state = State::Aborting,
            State::Init | State::Idle => {
                self.state = State::Aborted;
                if let Some(stream) = self.stream.take() {
                    stream.shutdown(Shutdown::Both)?;
                }
            },
            State::Progress | State::Aborting => {
                self.state = State::Aborted;
                if let Some(abort_handle) = self.abort_handler.take() {
                    abort_handle.abort();
                }
            },
        };

        Ok(())
    }
}



