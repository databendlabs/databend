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
use common_base::base::Runtime;
use common_base::base::SignalStream;
use common_base::infallible::RwLock;
use common_contexts::DalRuntime;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::init_operator;
use common_metrics::label_counter;
use common_tracing::init_query_logger;
use common_tracing::tracing;
use common_tracing::tracing_appender::non_blocking::WorkerGuard;
use futures::future::Either;
use futures::StreamExt;
use opendal::Operator;

use crate::catalogs::CatalogManager;
use crate::clusters::ClusterDiscovery;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::session::Session;
use crate::sessions::session_ref::SessionRef;
use crate::sessions::ProcessInfo;
use crate::sessions::SessionManagerStatus;
use crate::sessions::SessionType;
use crate::storages::cache::CacheManager;
use crate::Config;

pub struct SessionManager {
    pub(in crate::sessions) conf: RwLock<Config>,
    pub(in crate::sessions) discovery: RwLock<Arc<ClusterDiscovery>>,
    pub(in crate::sessions) catalogs: RwLock<Arc<CatalogManager>>,
    pub(in crate::sessions) http_query_manager: Arc<HttpQueryManager>,

    pub(in crate::sessions) max_sessions: usize,
    pub(in crate::sessions) active_sessions: Arc<RwLock<HashMap<String, Arc<Session>>>>,
    pub(in crate::sessions) storage_cache_manager: RwLock<Arc<CacheManager>>,
    pub(in crate::sessions) query_logger:
        RwLock<Option<Arc<dyn tracing::Subscriber + Send + Sync>>>,
    pub status: Arc<RwLock<SessionManagerStatus>>,
    storage_operator: RwLock<Operator>,
    storage_runtime: Arc<Runtime>,
    _guards: Vec<WorkerGuard>,
    // When typ is MySQL, insert into this map, key is id, val is MySQL connection id.
    pub(crate) mysql_conn_map: Arc<RwLock<HashMap<Option<u32>, String>>>,
    pub(in crate::sessions) mysql_basic_conn_id: AtomicU32,
}

impl SessionManager {
    pub async fn from_conf(conf: Config) -> Result<Arc<SessionManager>> {
        let catalogs = Arc::new(CatalogManager::new(&conf).await?);
        let storage_cache_manager = Arc::new(CacheManager::init(&conf.query));

        // Cluster discovery.
        let discovery = ClusterDiscovery::create_global(conf.clone()).await?;

        let storage_runtime = {
            let mut storage_num_cpus = conf.storage.num_cpus as usize;
            if storage_num_cpus == 0 {
                storage_num_cpus = std::cmp::max(1, num_cpus::get() / 2)
            }

            Runtime::with_worker_threads(storage_num_cpus, Some("IO-worker".to_owned()))?
        };

        // NOTE: Magic happens here. We will add a layer upon original storage operator
        // so that all underlying storage operations will send to storage runtime.
        let storage_operator = Self::init_storage_operator(&conf)
            .await?
            .layer(DalRuntime::new(storage_runtime.inner()));

        let http_query_manager = HttpQueryManager::create_global(conf.clone()).await?;
        let max_sessions = conf.query.max_active_sessions as usize;
        let active_sessions = Arc::new(RwLock::new(HashMap::with_capacity(max_sessions)));
        let status = Arc::new(RwLock::new(Default::default()));

        let (_guards, query_logger) = if conf.log.query_enabled {
            let (_guards, query_logger) = init_query_logger("query-detail", conf.log.dir.as_str());
            (_guards, Some(query_logger))
        } else {
            (Vec::new(), None)
        };
        let mysql_conn_map = Arc::new(RwLock::new(HashMap::with_capacity(max_sessions)));

        Ok(Arc::new(SessionManager {
            conf: RwLock::new(conf),
            catalogs: RwLock::new(catalogs),
            discovery: RwLock::new(discovery),
            http_query_manager,
            max_sessions,
            active_sessions,
            storage_cache_manager: RwLock::new(storage_cache_manager),
            query_logger: RwLock::new(query_logger),
            status,
            storage_operator: RwLock::new(storage_operator),
            storage_runtime: Arc::new(storage_runtime),
            _guards,
            mysql_conn_map,
            mysql_basic_conn_id: AtomicU32::new(9_u32.to_le() as u32),
        }))
    }

    pub fn get_conf(&self) -> Config {
        self.conf.read().clone()
    }

    pub fn get_cluster_discovery(self: &Arc<Self>) -> Arc<ClusterDiscovery> {
        self.discovery.read().clone()
    }

    pub fn get_http_query_manager(self: &Arc<Self>) -> Arc<HttpQueryManager> {
        self.http_query_manager.clone()
    }

    pub fn get_catalog_manager(self: &Arc<Self>) -> Arc<CatalogManager> {
        self.catalogs.read().clone()
    }

    pub fn get_storage_operator(self: &Arc<Self>) -> Operator {
        self.storage_operator.read().clone()
    }

    pub fn get_storage_cache_manager(&self) -> Arc<CacheManager> {
        self.storage_cache_manager.read().clone()
    }

    pub fn get_storage_runtime(&self) -> Arc<Runtime> {
        self.storage_runtime.clone()
    }

    pub async fn create_session(self: &Arc<Self>, typ: SessionType) -> Result<SessionRef> {
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
                tracing::debug!(
                    "session type is {}, mysql_conn_map no need to change.",
                    session_typ
                );
            }
        }
        let session =
            Session::try_create(config.clone(), id, typ, self.clone(), mysql_conn_id).await?;

        let mut sessions = self.active_sessions.write();
        if sessions.len() < self.max_sessions {
            label_counter(
                super::metrics::METRIC_SESSION_CONNECT_NUMBERS,
                &config.query.tenant_id,
                &config.query.cluster_id,
            );

            sessions.insert(session.get_id(), session.clone());

            Ok(SessionRef::create(session))
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
    ) -> Result<SessionRef> {
        // TODO: maybe deadlock?
        let config = self.get_conf();
        {
            let sessions = self.active_sessions.read();
            let v = sessions.get(&id);
            if v.is_some() {
                return Ok(SessionRef::create(v.unwrap().clone()));
            }
        }

        let session = Session::try_create(
            config.clone(),
            id.clone(),
            SessionType::FlightRPC,
            self.clone(),
            None,
        )
        .await?;

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
            Ok(SessionRef::create(session))
        } else {
            Ok(SessionRef::create(v.unwrap().clone()))
        }
    }

    #[allow(clippy::ptr_arg)]
    pub async fn get_session_by_id(self: &Arc<Self>, id: &str) -> Option<SessionRef> {
        let sessions = self.active_sessions.read();
        sessions
            .get(id)
            .map(|session| SessionRef::create(session.clone()))
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

        let mut sessions = self.active_sessions.write();
        sessions.remove(session_id);
        //also need remove mysql_conn_map
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
            tracing::info!(
                "Waiting {} secs for connections to close. You can press Ctrl + C again to force shutdown.",
                timeout_secs);
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

            tracing::info!("Will shutdown forcefully.");
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
        // https://github.com/Amanieu/parking_lot/blob/lock_api-0.4.4/lock_api/src/rwlock.rs#L422
        let active_sessions_read_guard = sessions.read();

        // First try to kill the idle session
        active_sessions_read_guard.values().for_each(Session::kill);
        let active_sessions = active_sessions_read_guard.len();

        match active_sessions {
            0 => true,
            _ => {
                tracing::info!("Waiting for {} connections to close.", active_sessions);
                false
            }
        }
    }

    // Init the storage operator by config.
    async fn init_storage_operator(conf: &Config) -> Result<Operator> {
        let op = init_operator(&conf.storage).await?;

        // Enable exponential backoff by default
        Ok(op.with_backoff(backon::ExponentialBackoff::default()))
    }

    pub async fn reload_config(&self) -> Result<()> {
        let config = {
            let mut config = self.conf.write();
            let config_file = config.config_file.clone();
            *config = Config::load()?;
            config.config_file = config_file;
            config.clone()
        };

        {
            let catalogs = CatalogManager::new(&config).await?;
            *self.catalogs.write() = Arc::new(catalogs);
        }

        *self.storage_cache_manager.write() = Arc::new(CacheManager::init(&config.query));

        {
            // NOTE: Magic happens here. We will add a layer upon original storage operator
            // so that all underlying storage operations will send to storage runtime.
            let operator = Self::init_storage_operator(&config)
                .await?
                .layer(DalRuntime::new(self.storage_runtime.inner()));
            *self.storage_operator.write() = operator;
        }

        {
            let discovery = ClusterDiscovery::create_global(config.clone()).await?;
            *self.discovery.write() = discovery;
        }

        Ok(())
    }

    pub fn get_query_logger(&self) -> Option<Arc<dyn tracing::Subscriber + Send + Sync>> {
        self.query_logger.write().to_owned()
    }
}
