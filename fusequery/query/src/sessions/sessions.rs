// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::ops::Sub;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Partitions;
use common_runtime::tokio;
use metrics::counter;

use crate::configs::Config;
use crate::datasources::DataSource;
use crate::servers::AbortableService;
use crate::servers::Elapsed;
use crate::sessions::session::ISession;
use crate::sessions::session::SessionCreator;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;

pub struct SessionMgr {
    conf: Config,
    datasource: Arc<DataSource>,

    max_mysql_sessions: usize,
    sessions: RwLock<HashMap<String, Arc<Box<dyn ISession>>>>,
    // TODO: remove queries_context.
    queries_context: RwLock<HashMap<String, FuseQueryContextRef>>,

    notified: Arc<AtomicBool>,
    aborted_notify: Arc<tokio::sync::Notify>,
}

pub type SessionMgrRef = Arc<SessionMgr>;

impl SessionMgr {
    pub fn try_create(max_mysql_sessions: u64) -> Result<SessionMgrRef> {
        Ok(Arc::new(SessionMgr {
            conf: Config::default(),
            datasource: Arc::new(DataSource::try_create()?),

            max_mysql_sessions: max_mysql_sessions as usize,
            sessions: RwLock::new(HashMap::with_capacity(max_mysql_sessions as usize)),
            queries_context: RwLock::new(HashMap::with_capacity(max_mysql_sessions as usize)),

            notified: Arc::new(AtomicBool::new(false)),
            aborted_notify: Arc::new(tokio::sync::Notify::new()),
        }))
    }

    pub fn from_conf(conf: Config) -> Result<SessionMgrRef> {
        let max_mysql_sessions = conf.mysql_handler_thread_num as usize;
        Ok(Arc::new(SessionMgr {
            conf,
            datasource: Arc::new(DataSource::try_create()?),

            max_mysql_sessions,
            sessions: RwLock::new(HashMap::with_capacity(max_mysql_sessions)),
            queries_context: RwLock::new(HashMap::with_capacity(max_mysql_sessions)),

            notified: Arc::new(AtomicBool::new(false)),
            aborted_notify: Arc::new(tokio::sync::Notify::new()),
        }))
    }

    pub fn get_datasource(self: &Arc<Self>) -> Arc<DataSource> {
        self.datasource.clone()
    }

    pub fn create_session<S: SessionCreator>(self: &Arc<Self>) -> Result<Arc<Box<dyn ISession>>> {
        counter!(super::metrics::METRIC_SESSION_CONNECT_NUMBERS, 1);

        let mut sessions = self.sessions.write();
        match sessions.len() == self.max_mysql_sessions {
            true => Err(ErrorCode::TooManyUserConnections(
                "The current accept connection has exceeded mysql_handler_thread_num config",
            )),
            false => {
                let id = uuid::Uuid::new_v4().to_string();
                let session = S::create(self.conf.clone(), id.clone(), self.clone())?;
                sessions.insert(id, session.clone());
                Ok(session)
            }
        }
    }

    pub fn get_session(self: &Arc<Self>, id: &str) -> Result<Arc<Box<dyn ISession>>> {
        let sessions = self.sessions.read();
        match sessions.get(id) {
            Some(sessions) => Ok(sessions.clone()),
            None => Err(ErrorCode::NotFoundSession(format!(
                "Not found session: {}",
                id
            ))),
        }
    }

    pub fn destroy_session(self: &Arc<Self>, session_id: String) {
        counter!(super::metrics::METRIC_SESSION_CLOSE_NUMBERS, 1);

        self.sessions.write().remove(&session_id);
    }

    pub fn try_create_context(&self) -> Result<FuseQueryContextRef> {
        counter!(super::metrics::METRIC_SESSION_CONNECT_NUMBERS, 1);

        let ctx = FuseQueryContext::try_create(self.conf.clone())?;
        self.queries_context
            .write()
            .insert(ctx.get_id(), ctx.clone());
        Ok(ctx)
    }

    pub fn try_remove_context(&self, ctx: FuseQueryContextRef) -> Result<()> {
        counter!(super::metrics::METRIC_SESSION_CLOSE_NUMBERS, 1);

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
impl AbortableService<(), ()> for SessionMgr {
    fn abort(&self, force: bool) -> Result<()> {
        self.sessions
            .write()
            .iter()
            .map(|(_, session)| session.abort(force))
            .collect::<Result<Vec<_>>>()?;

        if !self.notified.load(Ordering::Relaxed) {
            self.aborted_notify.notify_waiters();
            self.notified.store(true, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn start(&self, _: ()) -> Result<()> {
        Err(ErrorCode::LogicalError(
            "Logical error: start session manager.",
        ))
    }

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed> {
        let instant = Instant::now();

        let active_sessions_snapshot = || {
            let locked_active_sessions = self.sessions.write();
            (&*locked_active_sessions)
                .iter()
                .map(|(_, session)| session.clone())
                .collect::<Vec<_>>()
        };

        match duration {
            None => {
                if !self.notified.load(Ordering::Relaxed) {
                    self.aborted_notify.notified().await;
                }

                for active_session in active_sessions_snapshot() {
                    active_session.wait_terminal(None).await?;
                }
            }
            Some(duration) => {
                let mut duration = duration;

                if !self.notified.load(Ordering::Relaxed) {
                    tokio::time::timeout(duration, self.aborted_notify.notified())
                        .await
                        .map_err(|_| {
                            ErrorCode::Timeout(format!(
                                "SessionManager did not shutdown in {:?}",
                                duration
                            ))
                        })?;

                    duration = duration.sub(std::cmp::min(instant.elapsed(), duration));
                }

                for active_session in active_sessions_snapshot() {
                    if duration.is_zero() {
                        return Err(ErrorCode::Timeout(format!(
                            "SessionManager did not shutdown in {:?}",
                            duration
                        )));
                    }

                    let elapsed = active_session.wait_terminal(Some(duration)).await?;
                    duration = duration.sub(std::cmp::min(elapsed, duration));
                }
            }
        };

        Ok(instant.elapsed())
    }
}
