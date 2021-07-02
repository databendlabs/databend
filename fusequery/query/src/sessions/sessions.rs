// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::Entry::Occupied;
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use metrics::counter;

use crate::clusters::Cluster;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::datasources::DataSource;
use crate::servers::AbortableService;
use crate::servers::Elapsed;
use crate::sessions::session::Session;
use crate::sessions::session_ref::SessionRef;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;

pub struct SessionManager {
    conf: Config,
    cluster: ClusterRef,
    datasource: Arc<DataSource>,

    max_sessions: usize,
    active_sessions: RwLock<HashMap<String, Arc<Session>>>,
}

pub type SessionManagerRef = Arc<SessionManager>;

impl SessionManager {
    pub fn try_create(max_mysql_sessions: u64) -> Result<SessionManagerRef> {
        Ok(Arc::new(SessionManager {
            conf: Config::default(),
            cluster: Cluster::empty(),
            datasource: Arc::new(DataSource::try_create()?),

            max_sessions: max_mysql_sessions as usize,
            active_sessions: RwLock::new(HashMap::with_capacity(max_mysql_sessions as usize)),
        }))
    }

    pub fn from_conf(conf: Config, cluster: ClusterRef) -> Result<SessionManagerRef> {
        let max_mysql_sessions = conf.mysql_handler_thread_num as usize;
        Ok(Arc::new(SessionManager {
            conf,
            cluster,
            datasource: Arc::new(DataSource::try_create()?),

            max_sessions: max_mysql_sessions,
            active_sessions: RwLock::new(HashMap::with_capacity(max_mysql_sessions)),
        }))
    }

    pub fn get_cluster(self: &Arc<Self>) -> ClusterRef {
        self.cluster.clone()
    }

    pub fn get_datasource(self: &Arc<Self>) -> Arc<DataSource> {
        self.datasource.clone()
    }

    pub fn create_session(self: &Arc<Self>, typ: impl Into<String>) -> Result<SessionRef> {
        counter!(super::metrics::METRIC_SESSION_CONNECT_NUMBERS, 1);

        let mut sessions = self.active_sessions.write();
        match sessions.len() == self.max_sessions {
            true => Err(ErrorCode::TooManyUserConnections(
                "The current accept connection has exceeded mysql_handler_thread_num config",
            )),
            false => {
                let session = Session::try_create(
                    self.conf.clone(),
                    uuid::Uuid::new_v4().to_string(),
                    self.clone(),
                )?;

                sessions.insert(session.get_id(), session.clone());
                Ok(SessionRef::create(typ.into(), session, self.clone()))
            }
        }
    }

    pub fn create_rpc_session(self: &Arc<Self>, id: String, aborted: bool) -> Result<SessionRef> {
        counter!(super::metrics::METRIC_SESSION_CONNECT_NUMBERS, 1);

        let mut sessions = self.active_sessions.write();

        let session = match sessions.entry(id) {
            Occupied(entry) => entry.get().clone(),
            Vacant(_) if aborted => return Err(ErrorCode::AbortedSession("Aborting server.")),
            Vacant(entry) => {
                let session =
                    Session::try_create(self.conf.clone(), entry.key().clone(), self.clone())?;

                entry.insert(session).clone()
            }
        };

        Ok(SessionRef::create(
            String::from("RpcSession"),
            session,
            self.clone(),
        ))
    }

    pub fn destroy_session(self: &Arc<Self>, session_id: &String) {
        counter!(super::metrics::METRIC_SESSION_CLOSE_NUMBERS, 1);

        self.active_sessions.write().remove(session_id);
    }
}

#[async_trait::async_trait]
impl AbortableService<(), ()> for SessionManager {
    fn abort(&self, _force: bool) -> Result<()> {
        // self.sessions
        //     .write()
        //     .iter()
        //     .map(|(_, session)| session.abort(force))
        //     .collect::<Result<Vec<_>>>()?;
        //
        // if !self.notifyed.load(Ordering::Relaxed) {
        //     self.aborted_notify.notify_waiters();
        //     self.notifyed.store(true, Ordering::Relaxed);
        // }
        //
        Ok(())
    }

    async fn start(&self, _: ()) -> Result<()> {
        Err(ErrorCode::LogicalError(
            "Logical error: start session manager.",
        ))
    }

    async fn wait_terminal(&self, _duration: Option<Duration>) -> Result<Elapsed> {
        let instant = Instant::now();

        // let active_sessions_snapshot = || {
        //     let locked_active_sessions = self.sessions.write();
        //     (&*locked_active_sessions)
        //         .iter()
        //         .map(|(_, session)| session.clone())
        //         .collect::<Vec<_>>()
        // };
        //
        // match duration {
        //     None => {
        //         if !self.notifyed.load(Ordering::Relaxed) {
        //             self.aborted_notify.notified().await;
        //         }
        //
        //         for active_session in active_sessions_snapshot() {
        //             active_session.wait_terminal(None).await?;
        //         }
        //     }
        //     Some(duration) => {
        //         let mut duration = duration;
        //
        //         if !self.notifyed.load(Ordering::Relaxed) {
        //             tokio::time::timeout(duration, self.aborted_notify.notified())
        //                 .await
        //                 .map_err(|_| {
        //                     ErrorCode::Timeout(format!(
        //                         "SessionManager did not shutdown in {:?}",
        //                         duration
        //                     ))
        //                 })?;
        //
        //             duration = duration.sub(std::cmp::min(instant.elapsed(), duration));
        //         }
        //
        //         for active_session in active_sessions_snapshot() {
        //             if duration.is_zero() {
        //                 return Err(ErrorCode::Timeout(format!(
        //                     "SessionManager did not shutdown in {:?}",
        //                     duration
        //                 )));
        //             }
        //
        //             let elapsed = active_session.wait_terminal(Some(duration)).await?;
        //             duration = duration.sub(std::cmp::min(elapsed, duration));
        //         }
        //     }
        // };

        Ok(instant.elapsed())
    }
}
