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
use std::env;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use common_base::tokio;
use common_base::Runtime;
use common_base::SignalStream;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_metrics::label_counter;
use common_tracing::init_query_logger;
use common_tracing::tracing;
use common_tracing::tracing_appender::non_blocking::WorkerGuard;
use futures::future::Either;
use futures::StreamExt;
use opendal::credential::Credential;
use opendal::services::fs;
use opendal::services::s3;
use opendal::Accessor;
use opendal::Operator;
use opendal::Scheme as DalSchema;

use crate::catalogs::DatabaseCatalog;
use crate::clusters::ClusterDiscovery;
use crate::configs::Config;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::session::Session;
use crate::sessions::session_ref::SessionRef;
use crate::sessions::ProcessInfo;
use crate::sessions::SessionType;
use crate::sessions::Status;
use crate::storages::cache::CacheManager;
use crate::users::auth::auth_mgr::AuthMgr;
use crate::users::UserApiProvider;

pub struct SessionManager {
    pub(in crate::sessions) conf: RwLock<Config>,
    pub(in crate::sessions) discovery: RwLock<Arc<ClusterDiscovery>>,
    pub(in crate::sessions) catalog: RwLock<Arc<DatabaseCatalog>>,
    pub(in crate::sessions) user_manager: RwLock<Arc<UserApiProvider>>,
    pub(in crate::sessions) auth_manager: RwLock<Arc<AuthMgr>>,
    pub(in crate::sessions) http_query_manager: Arc<HttpQueryManager>,

    pub(in crate::sessions) max_sessions: usize,
    pub(in crate::sessions) active_sessions: Arc<RwLock<HashMap<String, Arc<Session>>>>,
    pub(in crate::sessions) storage_cache_manager: RwLock<Arc<CacheManager>>,
    pub(in crate::sessions) query_logger:
        RwLock<Option<Arc<dyn tracing::Subscriber + Send + Sync>>>,
    pub status: Arc<RwLock<Status>>,
    storage_operator: RwLock<Operator>,
    storage_runtime: Runtime,
    _guards: Vec<WorkerGuard>,
}

impl SessionManager {
    pub async fn from_conf(conf: Config) -> Result<Arc<SessionManager>> {
        let catalog = Arc::new(DatabaseCatalog::try_create_with_config(conf.clone()).await?);
        let storage_cache_manager = Arc::new(CacheManager::init(&conf.query));

        // Cluster discovery.
        let discovery = ClusterDiscovery::create_global(conf.clone()).await?;

        let storage_runtime = {
            let mut storage_num_cpus = conf.storage.storage_num_cpus as usize;
            if storage_num_cpus == 0 {
                storage_num_cpus = std::cmp::max(1, num_cpus::get() / 2)
            }
            Runtime::with_worker_threads(storage_num_cpus, Some("IO-worker".to_owned()))?
        };

        let storage_accessor = Self::init_storage_operator(&conf, &storage_runtime).await?;

        // User manager and init the default users.
        let user = UserApiProvider::create_global(conf.clone()).await?;
        let auth_manager = Arc::new(AuthMgr::create(conf.clone(), user.clone()).await?);
        let http_query_manager = HttpQueryManager::create_global(conf.clone()).await?;
        let max_sessions = conf.query.max_active_sessions as usize;
        let active_sessions = Arc::new(RwLock::new(HashMap::with_capacity(max_sessions)));
        let status = Arc::new(RwLock::new(Default::default()));

        let (_guards, query_logger) = if conf.log.log_query_enabled {
            let (_guards, query_logger) =
                init_query_logger("query-detail", conf.log.log_dir.as_str());
            (_guards, Some(query_logger))
        } else {
            (Vec::new(), None)
        };

        Ok(Arc::new(SessionManager {
            conf: RwLock::new(conf),
            catalog: RwLock::new(catalog),
            discovery: RwLock::new(discovery),
            user_manager: RwLock::new(user),
            http_query_manager,
            max_sessions,
            active_sessions,
            auth_manager: RwLock::new(auth_manager),
            storage_cache_manager: RwLock::new(storage_cache_manager),
            query_logger: RwLock::new(query_logger),
            status,
            storage_operator: RwLock::new(storage_accessor),
            storage_runtime,
            _guards,
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

    pub fn get_auth_manager(self: &Arc<Self>) -> Arc<AuthMgr> {
        self.auth_manager.read().clone()
    }

    /// Get the user api provider.
    pub fn get_user_manager(self: &Arc<Self>) -> Arc<UserApiProvider> {
        self.user_manager.read().clone()
    }

    pub fn get_catalog(self: &Arc<Self>) -> Arc<DatabaseCatalog> {
        self.catalog.read().clone()
    }

    pub fn get_storage_operator(self: &Arc<Self>) -> Operator {
        self.storage_operator.read().clone()
    }

    pub fn get_storage_cache_manager(&self) -> Arc<CacheManager> {
        self.storage_cache_manager.read().clone()
    }

    pub fn get_storage_runtime<'a>(self: &'a Arc<Self>) -> &'a Runtime {
        &self.storage_runtime
    }

    pub async fn create_session(self: &Arc<Self>, typ: SessionType) -> Result<SessionRef> {
        // TODO: maybe deadlock
        let config = self.get_config();
        {
            let sessions = self.active_sessions.read();
            if sessions.len() == self.max_sessions {
                return Err(ErrorCode::TooManyUserConnections(
                    "The current accept connection has exceeded mysql_handler_thread_num config",
                ));
            }
        }
        let session = Session::try_create(
            config.clone(),
            uuid::Uuid::new_v4().to_string(),
            typ,
            self.clone(),
        )
        .await?;

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
                "The current accept connection has exceeded mysql_handler_thread_num config",
            ))
        }
    }

    pub async fn create_rpc_session(
        self: &Arc<Self>,
        id: String,
        aborted: bool,
    ) -> Result<SessionRef> {
        // TODO: maybe deadlock?
        let config = self.get_config();
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
    pub fn destroy_session(self: &Arc<Self>, session_id: &String) {
        let config = self.get_config();
        label_counter(
            super::metrics::METRIC_SESSION_CLOSE_NUMBERS,
            &config.query.tenant_id,
            &config.query.cluster_id,
        );

        let mut sessions = self.active_sessions.write();
        sessions.remove(session_id);
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
    async fn init_storage_operator(conf: &Config, runtime: &Runtime) -> Result<Operator> {
        let storage_conf = &conf.storage;
        let schema_name = &storage_conf.storage_type;
        let schema = DalSchema::from_str(schema_name)
            .map_err(|e| ErrorCode::DalTransportError(e.to_string()))?;

        let accessor: Arc<dyn Accessor> = match schema {
            DalSchema::S3 => {
                let s3_conf = &storage_conf.s3;
                let mut builder = s3::Backend::build();
                // Runtime.
                {
                    builder.runtime(runtime.inner());
                }

                // Endpoint.
                {
                    builder.endpoint(&s3_conf.endpoint_url);
                }

                // Credential.
                {
                    builder.credential(Credential::hmac(
                        &s3_conf.access_key_id,
                        &s3_conf.secret_access_key,
                    ));
                }

                // Bucket.
                {
                    builder.bucket(&s3_conf.bucket);
                }

                // Root.
                {
                    if !s3_conf.root.is_empty() {
                        builder.root(&s3_conf.root);
                    }
                }

                builder
                    .finish()
                    .await
                    .map_err(|e| ErrorCode::DalTransportError(e.to_string()))?
            }
            DalSchema::Azblob => {
                todo!()
            }
            DalSchema::Fs => {
                let mut path = storage_conf.disk.data_path.clone();
                if !path.starts_with('/') {
                    path = env::current_dir().unwrap().join(path).display().to_string();
                }

                fs::Backend::build()
                    .root(&path)
                    .finish()
                    .await
                    .map_err(|e| ErrorCode::DalTransportError(e.to_string()))?
            }
        };

        Ok(Operator::new(accessor))
    }

    pub fn get_config(&self) -> Config {
        self.conf.read().clone()
    }

    pub async fn reload_config(&self) -> Result<()> {
        let config = {
            let mut config = self.conf.write();
            let config_file = config.config_file.clone();
            *config = Config::load_from_toml(&config_file)?;
            config.config_file = config_file;
            config.clone()
        };

        {
            let catalog = DatabaseCatalog::try_create_with_config(config.clone()).await?;
            *self.catalog.write() = Arc::new(catalog);
        }

        *self.storage_cache_manager.write() = Arc::new(CacheManager::init(&config.query));

        {
            let operator = Self::init_storage_operator(&config, &self.storage_runtime).await?;
            *self.storage_operator.write() = operator;
        }

        {
            let discovery = ClusterDiscovery::create_global(config.clone()).await?;
            *self.discovery.write() = discovery;
        }

        // User manager and init the default users.
        let user = {
            let user = UserApiProvider::create_global(config.clone()).await?;
            *self.user_manager.write() = user.clone();
            user
        };

        let auth_mgr = AuthMgr::create(config.clone(), user).await?;
        *self.auth_manager.write() = Arc::new(auth_mgr);

        Ok(())
    }

    pub fn get_query_logger(&self) -> Option<Arc<dyn tracing::Subscriber + Send + Sync>> {
        self.query_logger.write().to_owned()
    }
}
