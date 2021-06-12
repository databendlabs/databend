use crate::sessions::{FuseQueryContextRef, FuseQueryContext, Settings};
use futures::future::{AbortHandle, Aborted};
use common_exception::Result;
use common_planners::PlanNode;

pub enum State {
    Idle,
    Progress,
    Aborting,
    Aborted,
}

pub struct SessionStatus {
    state: State,
    current_database: String,
    session_settings: Settings,

    executing_query: Option<String>,
    executing_query_plan: Option<PlanNode>,
    abort_handler: Option<AbortHandle>,
}

impl SessionStatus {
    pub fn create() -> SessionStatus {
        SessionStatus {
            state: State::Idle,
            current_database: String::from("default"),
            session_settings: Settings::create(),
            executing_query: None,
            executing_query_plan: None,
            abort_handler: None,
        }
    }

    pub fn is_aborted(&self) -> bool {
        match self.state {
            State::Aborting | State::Aborted => true,
            _ => false
        }
    }

    pub fn init_context(&mut self, context: FuseQueryContextRef) {
        self.state = State::Progress;

    }

    pub fn enter_parser(&mut self, query: &str) {
        self.executing_query = Some(query.to_string())
    }

    pub fn enter_interpreter(&mut self, query_plan: &PlanNode) {
        self.executing_query_plan = Some(query_plan.clone())
    }

    pub fn enter_fetch_data(&mut self, abort_handle: AbortHandle) {
        self.abort_handler = Some(abort_handle)
    }

    pub fn enter_abort(&mut self, force: bool) {
        match self.state {
            State::Aborted => {},
            _ if !force => self.state = State::Aborting,
            _ => {
                self.state = State::Aborted;
                if let Some(abort_handle) = self.abort_handler.take() {
                    abort_handle.abort()
                }
            },
        }
    }
}



