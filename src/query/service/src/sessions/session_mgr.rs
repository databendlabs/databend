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
use std::time::Duration;

use common_base::base::tokio;
use common_base::base::SignalStream;
use common_base::base::Singleton;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metrics::label_counter;
use common_settings::Settings;
use common_users::UserApiProvider;
use futures::future::Either;
use futures::StreamExt;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tracing::debug;
use tracing::info;

use crate::sessions::session::Session;
use crate::sessions::ProcessInfo;
use crate::sessions::SessionContext;
use crate::sessions::SessionManagerStatus;
use crate::sessions::SessionType;
use crate::Config;

pub struct SessionManager {
    pub(in crate::sessions) conf: Config,
    pub(in crate::sessions) max_sessions: usize,
    pub(in crate::sessions) active_sessions: Arc<RwLock<HashMap<String, Arc<Session>>>>,
    pub status: Arc<RwLock<SessionManagerStatus>>,

    // When typ is MySQL, insert into this map, key is id, val is MySQL connection id.
    pub(crate) mysql_conn_map: Arc<RwLock<HashMap<Option<u32>, String>>>,
    pub(in crate::sessions) mysql_basic_conn_id: AtomicU32,
}

static SESSION_MANAGER: OnceCell<Singleton<Arc<SessionManager>>> = OnceCell::new();

impl SessionManager {
    pub fn init(conf: Config, v: Singleton<Arc<SessionManager>>) -> Result<()> {
        v.init(Self::create(conf))?;

        SESSION_MANAGER.set(v).ok();
        Ok(())
    }

    pub fn create(conf: Config) -> Arc<SessionManager> {
        let max_sessions = conf.query.max_active_sessions as usize;
        Arc::new(SessionManager {
            conf,
            max_sessions,
            mysql_basic_conn_id: AtomicU32::new(9_u32.to_le() as u32),
            status: Arc::new(RwLock::new(SessionManagerStatus::default())),
            mysql_conn_map: Arc::new(RwLock::new(HashMap::with_capacity(max_sessions))),
            active_sessions: Arc::new(RwLock::new(HashMap::with_capacity(max_sessions))),
        })
    }

    pub fn instance() -> Arc<SessionManager> {
        match SESSION_MANAGER.get() {
            None => panic!("SessionManager is not init"),
            Some(session_manager) => session_manager.get(),
        }
    }

    pub fn get_conf(&self) -> Config {
        self.conf.clone()
    }

    pub async fn create_session(self: &Arc<Self>, typ: SessionType) -> Result<Arc<Session>> {
        // TODO: maybe deadlock
        let config = self.get_conf();
        {
            let sessions = self.active_sessions.read();
            if sessions.len() == self.max_sessions {
                return Err(ErrorCode::TooManyUserConnections(
                    "The current accept connection has exceeded max_active_sessions config",
                ));
            }
        }
        let id = uuid::Uuid::new_v4().to_string();
        let session_typ = typ.clone();
        let mut mysql_conn_id = None;
        match session_typ {
            SessionType::MySQL => {
                let mut conn_id_session_id = self.mysql_conn_map.write();
                mysql_conn_id = Some(self.mysql_basic_conn_id.fetch_add(1, Ordering::Relaxed));
                if conn_id_session_id.len() < self.max_sessions {
                    conn_id_session_id.insert(mysql_conn_id, id.clone());
                } else {
                    return Err(ErrorCode::TooManyUserConnections(
                        "The current accept connection has exceeded max_active_sessions config",
                    ));
                }
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
        let session_settings = Settings::try_create(&config, user_api, tenant).await?;
        let session_ctx = SessionContext::try_create(config.clone(), session_settings)?;
        let session = Session::try_create(id, typ, session_ctx, mysql_conn_id)?;

        let mut sessions = self.active_sessions.write();
        if sessions.len() < self.max_sessions {
            label_counter(
                super::metrics::METRIC_SESSION_CONNECT_NUMBERS,
                &config.query.tenant_id,
                &config.query.cluster_id,
            );

            sessions.insert(session.get_id(), session.clone());

            Ok(session)
        } else {
            Err(ErrorCode::TooManyUserConnections(
                "The current accept connection has exceeded max_active_sessions config",
            ))
        }
    }

    pub async fn create_rpc_session(
        self: &Arc<Self>,
        id: String,
        aborted: bool,
    ) -> Result<Arc<Session>> {
        // TODO: maybe deadlock?
        let config = self.get_conf();
        {
            let sessions = self.active_sessions.read();
            let v = sessions.get(&id);
            if v.is_some() {
                return Ok(v.unwrap().clone());
            }
        }

        let tenant = config.query.tenant_id.clone();
        let user_api = UserApiProvider::instance();
        let session_settings = Settings::try_create(&config, user_api, tenant).await?;
        let session_ctx = SessionContext::try_create(config.clone(), session_settings)?;
        let session = Session::try_create(id.clone(), SessionType::FlightRPC, session_ctx, None)?;

        let mut sessions = self.active_sessions.write();
        let v = sessions.get(&id);
        if v.is_none() {
            if aborted {
                return Err(ErrorCode::AbortedSession("Aborting server."));
            }

            label_counter(
                super::metrics::METRIC_SESSION_CONNECT_NUMBERS,
                &config.query.tenant_id,
                &config.query.cluster_id,
            );

            sessions.insert(id, session.clone());
            Ok(session)
        } else {
            Ok(v.unwrap().clone())
        }
    }

    #[allow(clippy::ptr_arg)]
    pub async fn get_session_by_id(self: &Arc<Self>, id: &str) -> Option<Arc<Session>> {
        let sessions = self.active_sessions.read();
        sessions.get(id).cloned()
    }

    #[allow(clippy::ptr_arg)]
    pub async fn get_id_by_mysql_conn_id(
        self: &Arc<Self>,
        mysql_conn_id: &Option<u32>,
    ) -> Option<String> {
        let sessions = self.mysql_conn_map.read();
        sessions.get(mysql_conn_id).cloned()
    }

    #[allow(clippy::ptr_arg)]
    pub fn destroy_session(self: &Arc<Self>, session_id: &String) {
        let config = self.get_conf();
        label_counter(
            super::metrics::METRIC_SESSION_CLOSE_NUMBERS,
            &config.query.tenant_id,
            &config.query.cluster_id,
        );

        // stop tracking session
        {
            let mut sessions = self.active_sessions.write();
            sessions.remove(session_id);
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
        self: &Arc<Self>,
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
                if SessionManager::destroy_idle_sessions(&active_sessions).await {
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
            active_sessions
                .read()
                .values()
                .for_each(Session::force_kill_session);
        }
    }

    pub async fn processes_info(self: &Arc<Self>) -> Vec<ProcessInfo> {
        let sessions = self.active_sessions.read();
        sessions
            .values()
            .map(Session::process_info)
            .collect::<Vec<_>>()
    }

    async fn destroy_idle_sessions(sessions: &Arc<RwLock<HashMap<String, Arc<Session>>>>) -> bool {
        // Read lock does not support reentrant
        // https://github.com/Amanieu/parking_lot::/blob/lock_api-0.4.4/lock_api/src/rwlock.rs#L422
        let active_sessions_read_guard = sessions.read();

        // First try to kill the idle session
        active_sessions_read_guard.values().for_each(Session::kill);
        let active_sessions = active_sessions_read_guard.len();

        match active_sessions {
            0 => true,
            _ => {
                info!("Waiting for {} connections to close.", active_sessions);
                false
            }
        }
    }
}
