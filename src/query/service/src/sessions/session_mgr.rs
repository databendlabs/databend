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

use std::future::Future;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use common_base::base::tokio;
use common_base::base::SignalStream;
use common_base::base::Singleton;
use common_config::Config;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metrics::label_counter;
use common_settings::Settings;
use common_users::UserApiProvider;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
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

pub struct SessionManager {
    pub(in crate::sessions) conf: Config,
    pub(in crate::sessions) max_sessions: usize,
    pub(in crate::sessions) active_sessions: Arc<DashMap<String, Weak<Session>>>,
    pub(in crate::sessions) active_session_counter: AtomicUsize,

    pub status: Arc<RwLock<SessionManagerStatus>>,

    // When typ is MySQL, insert into this map, key is id, val is MySQL connection id.
    pub(crate) mysql_conn_map: Arc<DashMap<Option<u32>, String>>,
    pub(in crate::sessions) mysql_basic_conn_id: AtomicU32,
    // present mysql connections
    pub(in crate::sessions) mysql_conn_counter: AtomicUsize,
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
            active_session_counter: AtomicUsize::new(0),
            mysql_basic_conn_id: AtomicU32::new(9_u32.to_le()),
            mysql_conn_counter: AtomicUsize::new(0),
            status: Arc::new(RwLock::new(SessionManagerStatus::default())),
            mysql_conn_map: Arc::new(DashMap::with_capacity(max_sessions)),
            active_sessions: Arc::new(DashMap::with_capacity(max_sessions)),
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

    pub async fn create_session(&self, typ: SessionType) -> Result<Arc<Session>> {
        // TODO: maybe deadlock
        let config = self.get_conf();
        {
            if self.active_sessions.len() == self.max_sessions {
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
                mysql_conn_id = self.add_mysql_session(id.clone())?;
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
        let session = Session::try_create(id, typ.clone(), session_ctx, mysql_conn_id)?;

        self.add_active_session(config, session, typ)
    }

    pub fn get_session_by_id(&self, id: &str) -> Option<Arc<Session>> {
        self.active_sessions
            .get(id)
            .and_then(|weak_ptr| weak_ptr.upgrade())
    }

    pub fn get_id_by_mysql_conn_id(&self, mysql_conn_id: &Option<u32>) -> Option<String> {
        self.mysql_conn_map
            .get(mysql_conn_id)
            .map(|e| e.value().clone())
    }

    pub fn destroy_session(&self, session_id: &String) {
        let config = self.get_conf();
        label_counter(
            super::metrics::METRIC_SESSION_CLOSE_NUMBERS,
            &config.query.tenant_id,
            &config.query.cluster_id,
        );

        // stop tracking session
        self.remove_active_session(session_id);

        // also need remove mysql_conn_map
        self.mysql_conn_map.retain(|_, v| v != session_id);
        // todo: maybe incorrect
        self.mysql_conn_counter
            .store(self.mysql_conn_map.len(), Ordering::Relaxed);
    }

    /// Gentlely destroy all idle sessions
    ///
    /// Note:
    /// since this is the end, atomic counters will not be updated
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
            let active_sessions = active_sessions
                .iter()
                .map(|e| e.value().clone())
                .collect::<Vec<_>>();
            for weak_ptr in &active_sessions {
                if let Some(active_session) = weak_ptr.upgrade() {
                    active_session.force_kill_session();
                }
            }
        }
    }

    pub fn processes_info(&self) -> Vec<ProcessInfo> {
        let mut processes_info = Vec::with_capacity(self.active_sessions.len());
        for entry in self.active_sessions.iter() {
            let weak_ptr = entry.value();
            if let Some(active_session) = weak_ptr.upgrade() {
                processes_info.push(active_session.process_info());
            }
        }

        processes_info
    }

    /// Gentlely destroy all idle sessions
    ///
    /// Note:
    /// since this is the end, atomic counters will not be updated
    fn destroy_idle_sessions(sessions: &Arc<DashMap<String, Weak<Session>>>) -> bool {
        sessions.retain(|_id, weak_ptr| -> bool {
            match weak_ptr.upgrade() {
                None => false,
                Some(session) => {
                    session.kill();
                    true
                }
            }
        });

        let active_sessions = sessions.len();

        match active_sessions {
            0 => true,
            _ => {
                info!("Waiting for {} connections to close.", active_sessions);
                false
            }
        }
    }

    /// record an session in `Self::active_sessions` and increase `Self::active_session_counter` by 1
    ///
    /// no changes will happen if failed
    fn add_active_session(
        &self,
        config: Config,
        session: Arc<Session>,
        typ: SessionType,
    ) -> Result<Arc<Session>> {
        // acquire write lock by calling `DashMap::entry`
        let entry = self.active_sessions.entry(session.get_id());
        // Note:
        // # Why not `self.active_sessions.len()`
        //
        // `self.active_sessions.entry()` had acquired write lock on the DashMap shard
        // but the `len()` requires read locks on all shards
        // this will cause a deadlock
        //
        // # Why must record with an Atomic typed value
        //
        // `entry()` will only acquire write lock on one shard
        // if concurrent write happens on different shards
        // multiple threads may go inside this critial zone
        if self.active_session_counter.fetch_add(1, Ordering::Relaxed) < self.max_sessions {
            label_counter(
                super::metrics::METRIC_SESSION_CONNECT_NUMBERS,
                &config.query.tenant_id,
                &config.query.cluster_id,
            );

            if !matches!(typ, SessionType::FlightRPC) {
                let s = Arc::downgrade(&session);
                match entry {
                    Entry::Occupied(mut o) => {
                        o.insert(s);
                    }
                    Entry::Vacant(v) => {
                        v.insert(s);
                    }
                }
            }

            Ok(session)
        } else {
            // go back one, since nothing inserted
            self.active_session_counter.fetch_sub(1, Ordering::Relaxed);
            Err(ErrorCode::TooManyUserConnections(
                "The current accept connection has exceeded max_active_sessions config",
            ))
        }
    }

    /// record a mysql connection in `Self::mysql_conn_map`, will increase `Self::mysql_conn_counter` and `Self::mysql_basic_conn_id` by 1
    ///
    /// if fail, `Self::mysql_basic_conn_id` will still increase by 1
    fn add_mysql_session(&self, uuid: String) -> Result<Option<u32>> {
        let mysql_conn_id = Some(self.mysql_basic_conn_id.fetch_add(1, Ordering::Relaxed));

        // acquire write lock by calling `DashMap::entry`
        let conn_id_session_id = self.mysql_conn_map.entry(mysql_conn_id);
        // Note:
        // # Why not `self.mysql_conn_map.len()`
        //
        // `self.mysql_conn_map.entry()` had acquired write lock on the DashMap shard
        // but the `len()` requires read locks on all shards
        // this will cause a deadlock
        //
        // # Why must record with an Atomic typed value
        //
        // `entry()` will only acquire write lock on one shard
        // if concurrent write happens on different shards
        // multiple threads may go inside this critial zone
        if self.mysql_conn_counter.fetch_add(1, Ordering::Relaxed) < self.max_sessions {
            match conn_id_session_id {
                Entry::Occupied(mut o) => {
                    o.insert(uuid);
                }
                Entry::Vacant(v) => {
                    v.insert(uuid);
                }
            }
        } else {
            // go back one, since nothing inserted
            self.mysql_conn_counter.fetch_sub(1, Ordering::Relaxed);
            return Err(ErrorCode::TooManyUserConnections(
                "The current accept connection has exceeded max_active_sessions config",
            ));
        }
        Ok(mysql_conn_id)
    }

    fn remove_active_session(&self, session_id: &str) {
        if self.active_sessions.remove(session_id).is_some() {
            self.active_session_counter.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
