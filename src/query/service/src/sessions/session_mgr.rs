// Copyright 2021 Datafuse Labs
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

use std::collections::HashMap;
use std::future::Future;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::base::SignalStream;
use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table_context::ProcessInfoState;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use databend_common_metrics::session::*;
use databend_common_pipeline::core::PlanProfile;
use databend_common_settings::Settings;
use futures::StreamExt;
use futures::future::Either;
use log::info;
use parking_lot::RwLock;

use crate::sessions::ProcessInfo;
use crate::sessions::SessionContext;
use crate::sessions::SessionManagerStatus;
use crate::sessions::session::Session;
use crate::sessions::session_mgr_metrics::SessionManagerMetricsCollector;

pub struct SessionManager {
    pub(in crate::sessions) max_sessions: usize,
    pub(in crate::sessions) active_sessions: Arc<RwLock<HashMap<String, Weak<Session>>>>,
    pub status: Arc<RwLock<SessionManagerStatus>>,
    pub metrics_collector: SessionManagerMetricsCollector,

    // When typ is MySQL, insert into this map, key is id, val is MySQL connection id.
    pub(crate) mysql_conn_map: Arc<RwLock<HashMap<Option<u32>, String>>>,
    pub(in crate::sessions) mysql_basic_conn_id: AtomicU32,
}

impl SessionManager {
    pub fn init(conf: &InnerConfig) -> Result<()> {
        let global_instance = Self::create(conf);
        GlobalInstance::set(global_instance.clone());
        GLOBAL_METRICS_REGISTRY
            .register_collector(Box::new(global_instance.metrics_collector.clone()));

        Ok(())
    }

    pub fn create(conf: &InnerConfig) -> Arc<SessionManager> {
        let max_sessions = conf.query.common.max_active_sessions as usize;
        let mgr = Arc::new(SessionManager {
            max_sessions,
            mysql_basic_conn_id: AtomicU32::new(9_u32.to_le()),
            status: Arc::new(RwLock::new(SessionManagerStatus::default())),
            mysql_conn_map: Arc::new(RwLock::new(HashMap::with_capacity(max_sessions))),
            active_sessions: Arc::new(RwLock::new(HashMap::with_capacity(max_sessions))),
            metrics_collector: SessionManagerMetricsCollector::new(),
        });
        mgr.metrics_collector.attach_session_manager(mgr.clone());
        mgr
    }

    pub fn instance() -> Arc<SessionManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    pub async fn create_session(&self, typ: SessionType) -> Result<Session> {
        self.create_session_with_conn_id(typ, None).await
    }

    pub async fn create_mysql_session_with_conn_id(&self, conn_id: u32) -> Result<Session> {
        self.create_session_with_conn_id(SessionType::MySQL, Some(conn_id))
            .await
    }

    async fn create_session_with_conn_id(
        &self,
        typ: SessionType,
        mysql_conn_id: Option<u32>,
    ) -> Result<Session> {
        if !matches!(typ, SessionType::Dummy | SessionType::FlightRPC) {
            let sessions = self.active_sessions.read();
            self.validate_max_active_sessions(sessions.len(), "active sessions")?;
        }

        if matches!(typ, SessionType::MySQL) {
            let mysql_conn_map = self.mysql_conn_map.read();
            self.validate_max_active_sessions(mysql_conn_map.len(), "mysql conns")?;
        }

        let tenant = GlobalConfig::instance().query.tenant_id.clone();
        let settings = Settings::create(tenant);
        settings.load_changes().await?;

        let session = self.create_with_settings_with_conn_id(typ, settings, None, mysql_conn_id)?;

        Ok(session)
    }

    pub fn try_upgrade_session(&self, session: Arc<Session>, typ_to: SessionType) -> Result<()> {
        let typ_from = session.get_type();
        if typ_from != SessionType::Dummy {
            return Err(ErrorCode::Internal("bug: can only upgrade Dummy session"));
        }
        session.set_type(typ_to.clone());
        self.try_add_session(session, typ_to)
    }

    pub fn try_add_session(&self, session: Arc<Session>, typ: SessionType) -> Result<()> {
        let mut sessions = self.active_sessions.write();
        if !matches!(typ, SessionType::Dummy | SessionType::FlightRPC) {
            self.validate_max_active_sessions(sessions.len(), "active sessions")?;
            sessions.insert(session.get_id(), Arc::downgrade(&session));
            set_session_active_connections(sessions.len());
        }
        incr_session_connect_numbers();
        Ok(())
    }

    pub fn create_with_settings(
        &self,
        typ: SessionType,
        settings: Arc<Settings>,
        user: Option<UserInfo>,
    ) -> Result<Session> {
        self.create_with_settings_with_conn_id(typ, settings, user, None)
    }

    fn create_with_settings_with_conn_id(
        &self,
        typ: SessionType,
        settings: Arc<Settings>,
        user: Option<UserInfo>,
        mysql_conn_id: Option<u32>,
    ) -> Result<Session> {
        let id = uuid::Uuid::new_v4().to_string();
        let mysql_conn_id = match typ {
            SessionType::MySQL => Some(
                mysql_conn_id
                    .unwrap_or_else(|| self.mysql_basic_conn_id.fetch_add(1, Ordering::Relaxed)),
            ),
            _ => None,
        };

        let session_ctx = SessionContext::try_create(settings, typ.clone(), user)?;
        let session = Session::try_create(
            id.clone(),
            typ.clone(),
            Box::new(session_ctx),
            mysql_conn_id,
        )?;

        Ok(session)
    }

    pub fn alloc_mysql_conn_id(&self) -> u32 {
        self.mysql_basic_conn_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Add session to the session manager.
    ///
    /// Return a shared reference to the session.
    pub fn register_session(&self, session: Session) -> Result<Arc<Session>> {
        let id = session.get_id();
        let typ = session.get_type();
        let mysql_conn_id = session.get_mysql_conn_id();

        let session = Arc::new(session);
        self.try_add_session(session.clone(), typ.clone())?;

        if let SessionType::MySQL = typ {
            let mut mysql_conn_map = self.mysql_conn_map.write();
            self.validate_max_active_sessions(mysql_conn_map.len(), "mysql conns")?;

            mysql_conn_map.insert(mysql_conn_id, id);
        }

        Ok(session)
    }

    pub fn get_session_by_id(&self, id: &str) -> Option<Arc<Session>> {
        let sessions = self.active_sessions.read();
        sessions.get(id).and_then(|weak_ptr| weak_ptr.upgrade())
    }

    pub fn get_id_by_mysql_conn_id(&self, mysql_conn_id: &Option<u32>) -> Option<String> {
        let sessions = self.mysql_conn_map.read();
        sessions.get(mysql_conn_id).cloned()
    }

    pub fn destroy_session(&self, session_id: &String) {
        let _guard = LimitMemGuard::enter_unlimited();
        // NOTE: order and scope of lock are very important. It's will cause deadlock

        // stop tracking session
        {
            // Make sure this write lock has been released before dropping.
            // Because dropping session could re-enter `destroy_session`.
            let weak_session = { self.active_sessions.write().remove(session_id) };
            drop(weak_session);
        }

        // also need remove mysql_conn_map
        {
            let mut mysql_conns_map = self.mysql_conn_map.write();
            for (k, v) in mysql_conns_map.deref_mut().clone() {
                if &v == session_id {
                    mysql_conns_map.remove(&k);
                }
            }
        }

        {
            let sessions_count = { self.active_sessions.read().len() };

            incr_session_close_numbers();
            set_session_active_connections(sessions_count);
        }
    }

    pub fn graceful_shutdown(
        &self,
        mut signal: SignalStream,
        timeout: Option<Duration>,
    ) -> impl Future<Output = ()> {
        let active_sessions = self.active_sessions.clone();
        async move {
            if let Some(mut timeout) = timeout {
                info!(
                    "Waiting {:?} for connections to close. You can press Ctrl + C again to force shutdown.",
                    timeout
                );

                let mut signal = Box::pin(signal.next());

                while !timeout.is_zero() {
                    if SessionManager::destroy_idle_sessions(&active_sessions) {
                        return;
                    }

                    let interval = Duration::from_secs(1);
                    let sleep = Box::pin(tokio::time::sleep(interval));
                    match futures::future::select(sleep, signal).await {
                        Either::Right((_, _)) => break,
                        Either::Left((_, reserve_signal)) => signal = reserve_signal,
                    };

                    timeout = match timeout > Duration::from_secs(1) {
                        true => timeout - Duration::from_secs(1),
                        false => Duration::from_secs(0),
                    };
                }
            }

            info!("Will shutdown forcefully.");

            // During the destroy session, we need to get active_sessions write locks,
            // so we can only get active_sessions snapshots.
            let active_sessions = active_sessions.read().values().cloned().collect::<Vec<_>>();
            for weak_ptr in &active_sessions {
                if let Some(active_session) = weak_ptr.upgrade() {
                    active_session.force_kill_session();
                }
            }
        }
    }

    pub fn processes_info(&self) -> Vec<ProcessInfo> {
        self.active_sessions_snapshot()
            .into_iter()
            .filter_map(|weak_ptr| weak_ptr.upgrade().map(|session| session.process_info()))
            .collect::<Vec<_>>()
    }

    fn destroy_idle_sessions(sessions: &Arc<RwLock<HashMap<String, Weak<Session>>>>) -> bool {
        // Read lock does not support reentrant
        // https://github.com/Amanieu/parking_lot::/blob/lock_api-0.4.4/lock_api/src/rwlock.rs#L422
        let mut active_sessions_read_guard = sessions.write();

        // First try to kill the idle session
        active_sessions_read_guard.retain(|_id, weak_ptr| -> bool {
            weak_ptr.upgrade().is_some_and(|session| {
                session.kill();
                true
            })
        });

        // active_sessions_read_guard.values().for_each(Session::kill);
        let active_sessions = active_sessions_read_guard.len();

        match active_sessions {
            0 => true,
            _ => {
                info!("Waiting for {} connections to close.", active_sessions);
                false
            }
        }
    }

    fn validate_max_active_sessions(&self, count: usize, reason: &str) -> Result<()> {
        if count >= self.max_sessions {
            return Err(ErrorCode::TooManyUserConnections(format!(
                "Current {} ({}) has exceeded the max_active_sessions limit ({})",
                reason, count, self.max_sessions
            )));
        }
        Ok(())
    }

    pub fn get_current_session_status(&self) -> SessionManagerStatus {
        let mut status_t = self.status.read().clone();

        let mut running_queries_count = 0;
        let mut active_sessions_count = 0;
        let mut max_running_query_executed_secs = 0;

        for session in self.active_sessions_snapshot() {
            if let Some(session_ref) = session.upgrade() {
                if !session_ref.get_type().is_user_session() {
                    continue;
                }
                active_sessions_count += 1;
                let process_info = session_ref.process_info();
                if process_info.state == ProcessInfoState::Query {
                    running_queries_count += 1;

                    let query_executed_secs = process_info
                        .created_time
                        .elapsed()
                        .map(|x| x.as_secs())
                        .unwrap_or(0);
                    max_running_query_executed_secs =
                        std::cmp::max(max_running_query_executed_secs, query_executed_secs);
                }
            }
        }

        status_t.running_queries_count = running_queries_count;
        status_t.active_sessions_count = active_sessions_count;
        status_t.max_running_query_executed_secs = max_running_query_executed_secs;

        status_t
    }

    pub fn get_queries_profiles(&self) -> HashMap<String, Vec<PlanProfile>> {
        let mut queries_profiles = HashMap::new();
        for weak_ptr in self.active_sessions_snapshot() {
            let Some(arc_session) = weak_ptr.upgrade() else {
                continue;
            };

            let session_ctx = arc_session.session_ctx.as_ref();

            if let Some(context_shared) = session_ctx.get_query_context_shared() {
                queries_profiles.insert(
                    context_shared.init_query_id.as_ref().read().clone(),
                    context_shared.get_query_profiles(),
                );
            }
        }

        queries_profiles
    }

    pub fn get_query_profiles(&self, query_id: &str) -> Result<Vec<PlanProfile>> {
        for weak_ptr in self.active_sessions_snapshot() {
            let Some(arc_session) = weak_ptr.upgrade() else {
                continue;
            };

            let session_ctx = arc_session.session_ctx.as_ref();

            if let Some(context_shared) = session_ctx.get_query_context_shared() {
                if query_id == *context_shared.init_query_id.as_ref().read() {
                    return Ok(context_shared.get_query_profiles());
                }
            }
        }

        Err(ErrorCode::UnknownQuery(format!(
            "Unknown query {}",
            query_id
        )))
    }

    pub fn get_running_graph_dump(&self, query_id: &str) -> Result<String> {
        for weak_ptr in self.active_sessions_snapshot() {
            let Some(arc_session) = weak_ptr.upgrade() else {
                continue;
            };

            let session_ctx = arc_session.session_ctx.as_ref();

            if let Some(context_shared) = session_ctx.get_query_context_shared() {
                if query_id == *context_shared.init_query_id.as_ref().read() {
                    return Ok(context_shared.get_executor_graph_dump());
                }
            }
        }

        Err(ErrorCode::UnknownQuery(format!(
            "Unknown query {}",
            query_id
        )))
    }

    pub fn get_query_execution_stats(&self) -> Vec<(String, ExecutorStatsSnapshot)> {
        let mut res = Vec::new();
        for weak_ptr in self.active_sessions_snapshot() {
            let Some(arc_session) = weak_ptr.upgrade() else {
                continue;
            };

            let session_ctx = arc_session.session_ctx.as_ref();

            if let Some(context_shared) = session_ctx.get_query_context_shared() {
                let query_id = context_shared.init_query_id.as_ref().read().clone();
                let stats = context_shared.get_query_execution_stats();
                if let Some(stats) = stats {
                    res.push((query_id, stats));
                }
            }
        }
        res
    }

    fn active_sessions_snapshot(&self) -> Vec<Weak<Session>> {
        // Here the situation is the same of method `graceful_shutdown`:
        //
        // We should drop the read lock before
        // - acquiring upgraded session reference: the Arc<Session>,
        // - extracting the ProcessInfo from it
        // - and then drop the Arc<Session>
        // Since there are chances that we are the last one that holding the reference, and the
        // destruction of session need to acquire the write lock of `active_sessions`, which leads
        // to dead lock.

        let active_sessions_guard = self.active_sessions.read();
        active_sessions_guard.values().cloned().collect::<Vec<_>>()
    }

    pub fn kill_by_query_id(&self, query_id: &String) -> bool {
        for session in self.active_sessions_snapshot() {
            let Some(session) = session.upgrade() else {
                continue;
            };

            let Some(session_query_id) = session.get_current_query_id() else {
                continue;
            };

            if query_id == &session_query_id {
                session.force_kill_query(ErrorCode::AbortedQuery(
                    "Query interrupted due to memory contention.",
                ));
                return true;
            }
        }

        false
    }
    pub fn get_all_temp_tables(
        &self,
    ) -> Result<
        Vec<(
            String,
            SessionType,
            databend_common_meta_app::schema::TableInfo,
        )>,
    > {
        let mut all_temp_tables = Vec::new();
        let sessions = self.active_sessions_snapshot();
        for session in sessions {
            if let Some(session) = session.upgrade() {
                let temp_tables = {
                    let mgr_ref = session.temp_tbl_mgr();
                    let vals = mgr_ref.lock().list_tables();
                    vals
                }?;
                for table in temp_tables {
                    all_temp_tables.push((
                        format!(
                            "{}/{}",
                            session.get_current_user()?.name.clone(),
                            session.id.clone()
                        ),
                        session.typ.read().clone(),
                        table,
                    ));
                }
            }
        }
        Ok(all_temp_tables)
    }
}
