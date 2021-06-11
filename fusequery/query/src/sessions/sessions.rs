// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Partitions;
use metrics::counter;

use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;
use crate::servers::{RunnableService, Elapsed};
use std::time::Duration;
use crate::configs::Config;
use crate::clusters::{ClusterRef, Cluster};
use crate::sessions::session::{ISession, SessionCreator};

pub struct SessionManager {
    sessions: RwLock<HashMap<String, Arc<Box<dyn ISession>>>>,
    queries_context: RwLock<HashMap<String, FuseQueryContextRef>>,
    max_mysql_sessions: u64,
}

pub type SessionManagerRef = Arc<SessionManager>;

impl SessionManager {
    pub fn create(max_mysql_sessions: u64) -> SessionManagerRef {
        Arc::new(SessionManager {
            sessions: RwLock::new(HashMap::with_capacity(max_mysql_sessions as usize)),
            queries_context: RwLock::new(HashMap::with_capacity(max_mysql_sessions as usize)),
            max_mysql_sessions,
        })
    }

    pub fn from_conf(conf: Config/*, cluster: ClusterRef*/) -> SessionManagerRef {
        Self::create(conf.mysql_handler_thread_num)
    }

    pub fn get_cluster(self: &Arc<Self>) -> ClusterRef {
        Cluster::empty()
    }

    pub fn create_session<S: SessionCreator>(self: &Arc<Self>) -> Result<Arc<Box<dyn ISession>>> {
        counter!(super::metrics::METRIC_SESSION_CONNECT_NUMBERS, 1);

        let mut sessions = self.sessions.write();
        match sessions.len() == (self.max_mysql_sessions as usize) {
            true => Err(ErrorCode::TooManyUserConnections("The current accept connection has exceeded mysql_handler_thread_num config")),
            false => {
                let id = uuid::Uuid::new_v4().to_string();
                let session = S::create(id.clone(), self.clone());
                sessions.insert(id, session.clone());
                Ok(session)
            }
        }
    }

    pub fn get_session(self: &Arc<Self>, id: &str) -> Result<Arc<Box<dyn ISession>>> {
        let mut sessions = self.sessions.read();
        match sessions.get(id) {
            None => Err(ErrorCode::NotFoundSession("")),
            Some(sessions) => Ok(sessions.clone())
        }
    }

    pub fn destroy_session<S: ISession>(self: &Arc<Self>, session: &S) {
        counter!(super::metrics::METRIC_SESSION_CLOSE_NUMBERS, 1);

        self.sessions.write().remove(&session.get_id());
    }

    pub fn try_create_context(&self) -> Result<FuseQueryContextRef> {
        let ctx = FuseQueryContext::try_create()?;
        self.queries_context.write().insert(ctx.get_id(), ctx.clone());
        Ok(ctx)
    }

    pub fn try_remove_context(&self, ctx: FuseQueryContextRef) -> Result<()> {
        self.queries_context.write().remove(&*ctx.get_id());
        Ok(())
    }

    /// Fetch nums partitions from session manager by context id.
    pub fn try_fetch_partitions(&self, ctx_id: String, nums: usize) -> Result<Partitions> {
        let session_map = self.queries_context.read();
        let ctx = session_map.get(&*ctx_id).ok_or_else(|| {
            ErrorCode::UnknownContextID(format!("Unsupported context id: {}", ctx_id))
        })?;
        ctx.try_get_partitions(nums)
    }
}

#[async_trait::async_trait]
impl RunnableService<(), ()> for SessionManager {
    fn abort(&self, force: bool) {
        let sessions = self.sessions.read();
        sessions.iter().for_each(|(_, session)| session.abort(force));
    }

    async fn start(&self, _: ()) -> Result<()> {
        Err(ErrorCode::LogicalError("Logical error: start session manager."))
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        Err(ErrorCode::UnImplement(""))
    }
}

