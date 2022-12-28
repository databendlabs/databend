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

use std::collections::HashMap;
use std::future::Future;
use std::ops::DerefMut;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use common_base::base::tokio;
use common_base::base::GlobalInstance;
use common_base::base::SignalStream;
use common_config::Config;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metrics::label_counter;
use common_metrics::label_gauge;
use common_settings::Settings;
use common_users::UserApiProvider;
use futures::future::Either;
use futures::StreamExt;
use parking_lot::RwLock;
use tracing::debug;
use tracing::info;

use crate::sessions::session::Session;
use crate::sessions::ProcessInfo;
use crate::sessions::SessionContext;
use crate::sessions::SessionManagerStatus;
use crate::sessions::SessionType;

static METRIC_SESSION_CONNECT_NUMBERS: &str = "session_connect_numbers";
static METRIC_SESSION_CLOSE_NUMBERS: &str = "session_close_numbers";
static METRIC_SESSION_ACTIVE_CONNECTIONS: &str = "session_connections";

pub struct SessionManager {
    pub(in crate::sessions) max_sessions: usize,
    pub(in crate::sessions) active_sessions: Arc<RwLock<HashMap<String, Weak<Session>>>>,
    pub status: Arc<RwLock<SessionManagerStatus>>,

    // When typ is MySQL, insert into this map, key is id, val is MySQL connection id.
    pub(crate) mysql_conn_map: Arc<RwLock<HashMap<Option<u32>, String>>>,
    pub(in crate::sessions) mysql_basic_conn_id: AtomicU32,
}

impl SessionManager {
    pub fn init(conf: &Config) -> Result<()> {
        GlobalInstance::set(Self::create(conf));

        Ok(())
    }

    pub fn create(conf: &Config) -> Arc<SessionManager> {
        let max_sessions = conf.query.max_active_sessions as usize;
        Arc::new(SessionManager {
            max_sessions,
            mysql_basic_conn_id: AtomicU32::new(9_u32.to_le()),
            status: Arc::new(RwLock::new(SessionManagerStatus::default())),
            mysql_conn_map: Arc::new(RwLock::new(HashMap::with_capacity(max_sessions))),
            active_sessions: Arc::new(RwLock::new(HashMap::with_capacity(max_sessions))),
        })
    }

    pub fn instance() -> Arc<SessionManager> {
        GlobalInstance::get()
    }

    pub async fn create_session(&self, typ: SessionType) -> Result<Arc<Session>> {
        // TODO: maybe deadlock
        let config = GlobalConfig::instance();
        {
            let sessions = self.active_sessions.read();
            self.validate_max_active_sessions(sessions.len(), "active sessions")?;
        }
        let id = uuid::Uuid::new_v4().to_string();
        let session_typ = typ.clone();
        let mut mysql_conn_id = None;
        match session_typ {
            SessionType::MySQL => {
                let mysql_conn_map = self.mysql_conn_map.read();
                mysql_conn_id = Some(self.mysql_basic_conn_id.fetch_add(1, Ordering::Relaxed));
                self.validate_max_active_sessions(mysql_conn_map.len(), "mysql conns")?;
            }
            _ => {
                debug!(
                    "session type is {}, mysql_conn_map no need to change.",
                    session_typ
                );
            }
        }

        let tenant = config.query.tenant_id.clone();
        let user_api = UserApiProvider::instance();
        let session_settings = Settings::try_create(user_api, tenant).await?;
        let session_ctx = SessionContext::try_create(session_settings)?;
        let session = Session::try_create(id.clone(), typ.clone(), session_ctx, mysql_conn_id)?;

        let mut sessions = self.active_sessions.write();
        self.validate_max_active_sessions(sessions.len(), "active sessions")?;

        label_counter(
            METRIC_SESSION_CONNECT_NUMBERS,
            &config.query.tenant_id,
            &config.query.cluster_id,
        );
        label_gauge(
            METRIC_SESSION_ACTIVE_CONNECTIONS,
            sessions.len() as f64,
            &config.query.tenant_id,
            &config.query.cluster_id,
        );

        if !matches!(typ, SessionType::FlightRPC) {
            sessions.insert(session.get_id(), Arc::downgrade(&session));
        }

        if let SessionType::MySQL = session_typ {
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
        let config = GlobalConfig::instance();
        label_counter(
            METRIC_SESSION_CLOSE_NUMBERS,
            &config.query.tenant_id,
            &config.query.cluster_id,
        );

        // stop tracking session
        {
            // Make sure this write lock has been released before dropping.
            // Becuase droping session could re-enter `destroy_session`.
            let weak_session = { self.active_sessions.write().remove(session_id) };
            drop(weak_session);
        }

        // also need remove mysql_conn_map
        let mut mysql_conns_map = self.mysql_conn_map.write();
        for (k, v) in mysql_conns_map.deref_mut().clone() {
            if &v == session_id {
                mysql_conns_map.remove(&k);
            }
        }
    }

    pub fn graceful_shutdown(
        &self,
        mut signal: SignalStream,
        timeout_secs: i32,
    ) -> impl Future<Output = ()> {
        let active_sessions = self.active_sessions.clone();
        async move {
            info!(
                "Waiting {} secs for connections to close. You can press Ctrl + C again to force shutdown.",
                timeout_secs
            );
            let mut signal = Box::pin(signal.next());

            for _index in 0..timeout_secs {
                if SessionManager::destroy_idle_sessions(&active_sessions) {
                    return;
                }

                let interval = Duration::from_secs(1);
                let sleep = Box::pin(tokio::time::sleep(interval));
                match futures::future::select(sleep, signal).await {
                    Either::Right((_, _)) => break,
                    Either::Left((_, reserve_signal)) => signal = reserve_signal,
                };
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
        let sessions = self.active_sessions.read();

        let mut processes_info = Vec::with_capacity(sessions.len());
        for weak_ptr in sessions.values() {
            if let Some(active_session) = weak_ptr.upgrade() {
                processes_info.push(active_session.process_info());
            }
        }

        processes_info
    }

    fn destroy_idle_sessions(sessions: &Arc<RwLock<HashMap<String, Weak<Session>>>>) -> bool {
        // Read lock does not support reentrant
        // https://github.com/Amanieu/parking_lot::/blob/lock_api-0.4.4/lock_api/src/rwlock.rs#L422
        let mut active_sessions_read_guard = sessions.write();

        // First try to kill the idle session
        active_sessions_read_guard.retain(|_id, weak_ptr| -> bool {
            match weak_ptr.upgrade() {
                None => false,
                Some(session) => {
                    session.kill();
                    true
                }
            }
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

    fn get_active_user_session_num(&self) -> usize {
        let active_sessions = self.active_sessions.read();
        active_sessions
            .iter()
            .filter(|(_, y)| match y.upgrade() {
                None => false,
                Some(a) => a.get_type().is_user_session(),
            })
            .count()
    }

    pub fn get_current_session_status(&self) -> SessionManagerStatus {
        let mut status_t = self.status.read().clone();
        let active_session = self.get_active_user_session_num();
        status_t.running_queries_count = active_session as u64;
        status_t
    }
}
