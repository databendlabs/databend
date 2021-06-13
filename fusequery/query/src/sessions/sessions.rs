// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::{ErrorCode, ToErrorCode};
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Partitions;
use metrics::counter;

use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;
use crate::servers::{AbortableService, Elapsed};
use std::time::{Duration, Instant};
use crate::configs::Config;
use crate::clusters::{ClusterRef, Cluster};
use crate::sessions::session::{ISession, SessionCreator};
use std::ops::Sub;
use crate::datasources::{IDataSource, DataSource};

pub struct SessionManager {
    cluster: ClusterRef,
    datasource: Arc<dyn IDataSource>,

    max_mysql_sessions: usize,
    sessions: RwLock<HashMap<String, Arc<Box<dyn ISession>>>>,
    // TODO: remove queries_context.
    queries_context: RwLock<HashMap<String, FuseQueryContextRef>>,

    aborted_notify: Arc<tokio::sync::Notify>,

}

pub type SessionManagerRef = Arc<SessionManager>;

impl SessionManager {
    pub fn try_create(max_mysql_sessions: u64) -> Result<SessionManagerRef> {
        Ok(Arc::new(SessionManager {
            cluster: Cluster::empty(),
            datasource: Arc::new(DataSource::try_create()?),

            max_mysql_sessions: max_mysql_sessions as usize,
            sessions: RwLock::new(HashMap::with_capacity(max_mysql_sessions as usize)),
            queries_context: RwLock::new(HashMap::with_capacity(max_mysql_sessions as usize)),

            aborted_notify: Arc::new(tokio::sync::Notify::new()),
        }))
    }

    pub fn from_conf(conf: Config, cluster: ClusterRef) -> Result<SessionManagerRef> {
        let max_mysql_sessions = conf.mysql_handler_thread_num as usize;
        Ok(Arc::new(SessionManager {
            cluster,
            datasource: Arc::new(DataSource::try_create()?),

            max_mysql_sessions,
            sessions: RwLock::new(HashMap::with_capacity(max_mysql_sessions)),
            queries_context: RwLock::new(HashMap::with_capacity(max_mysql_sessions)),

            aborted_notify: Arc::new(tokio::sync::Notify::new()),
        }))
    }

    pub fn get_cluster(self: &Arc<Self>) -> ClusterRef {
        self.cluster.clone()
    }

    pub fn create_session<S: SessionCreator>(self: &Arc<Self>) -> Result<Arc<Box<dyn ISession>>> {
        counter!(super::metrics::METRIC_SESSION_CONNECT_NUMBERS, 1);

        let mut sessions = self.sessions.write();
        match sessions.len() == self.max_mysql_sessions {
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
            Some(sessions) => Ok(sessions.clone()),
            None => Err(ErrorCode::NotFoundSession(format!("Not found session: {}", id))),
        }
    }

    pub fn destroy_session(self: &Arc<Self>, session_id: String) {
        counter!(super::metrics::METRIC_SESSION_CLOSE_NUMBERS, 1);

        self.sessions.write().remove(&session_id);
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
impl AbortableService<(), ()> for SessionManager {
    fn abort(&self, force: bool) -> Result<()> {
        let sessions = self.sessions.write();
        sessions.iter().map(|(_, session)| session.abort(force)).collect::<Result<Vec<_>>>()?;
        self.aborted_notify.notify_waiters();
        Ok(())
    }

    async fn start(&self, _: ()) -> Result<()> {
        Err(ErrorCode::LogicalError("Logical error: start session manager."))
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        let instant = Instant::now();

        let active_sessions_snapshot = || {
            let locked_active_sessions = self.sessions.write();
            (&*locked_active_sessions).iter().map(|(_, session)| session.clone())
                .collect::<Vec<_>>()
        };

        match duration {
            None => {
                self.aborted_notify.notified().await;
                for active_session in active_sessions_snapshot() {
                    active_session.wait_terminal(None).await?;
                }
            }
            Some(duration) => {
                tokio::time::timeout(duration, self.aborted_notify.notified())
                    .await
                    .map_err_to_code(ErrorCode::Timeout, || "")?;

                let mut duration = duration.sub(instant.elapsed());
                for active_session in active_sessions_snapshot() {
                    if duration.is_zero() {
                        return Err(ErrorCode::Timeout(""));
                    }

                    let elapsed = active_session.wait_terminal(Some(duration.clone())).await?;
                    duration = duration.sub(elapsed);
                }
            }
        };

        Ok(instant.elapsed())
    }
}

