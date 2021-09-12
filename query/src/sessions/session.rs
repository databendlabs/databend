// Copyright 2020 Datafuse Labs.
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

use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_exception::Result;
use common_infallible::Mutex;
use futures::channel::oneshot::Sender;
use futures::channel::*;

use crate::catalogs::impls::DatabaseCatalog;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::sessions::context_shared::DatafuseQueryContextShared;
use crate::sessions::DatafuseQueryContext;
use crate::sessions::DatafuseQueryContextRef;
use crate::sessions::SessionManagerRef;
use crate::sessions::Settings;

pub(in crate::sessions) struct MutableStatus {
    pub(in crate::sessions) abort: bool,
    pub(in crate::sessions) current_database: String,
    pub(in crate::sessions) session_settings: Arc<Settings>,
    #[allow(unused)]
    pub(in crate::sessions) client_host: Option<SocketAddr>,
    pub(in crate::sessions) io_shutdown_tx: Option<Sender<Sender<()>>>,
    pub(in crate::sessions) context_shared: Option<Arc<DatafuseQueryContextShared>>,
}

#[derive(Clone)]
pub struct Session {
    pub(in crate::sessions) id: String,
    pub(in crate::sessions) typ: String,
    pub(in crate::sessions) config: Config,
    pub(in crate::sessions) sessions: SessionManagerRef,
    pub(in crate::sessions) ref_count: Arc<AtomicUsize>,
    pub(in crate::sessions) mutable_state: Arc<Mutex<MutableStatus>>,
}

impl Session {
    pub fn try_create(
        config: Config,
        id: String,
        typ: String,
        sessions: SessionManagerRef,
    ) -> Result<Arc<Session>> {
        Ok(Arc::new(Session {
            id,
            typ,
            config,
            sessions,
            ref_count: Arc::new(AtomicUsize::new(0)),
            mutable_state: Arc::new(Mutex::new(MutableStatus {
                abort: false,
                current_database: String::from("default"),
                session_settings: Settings::try_create()?,
                client_host: None,
                io_shutdown_tx: None,
                context_shared: None,
            })),
        }))
    }

    pub fn get_id(self: &Arc<Self>) -> String {
        self.id.clone()
    }

    pub fn get_type(self: &Arc<Self>) -> String {
        self.typ.clone()
    }

    pub fn is_aborting(self: &Arc<Self>) -> bool {
        self.mutable_state.lock().abort
    }

    pub fn kill(self: &Arc<Self>) {
        let mut mutable_state = self.mutable_state.lock();

        mutable_state.abort = true;
        if mutable_state.context_shared.is_none() {
            if let Some(io_shutdown) = mutable_state.io_shutdown_tx.take() {
                let (tx, rx) = oneshot::channel();
                if io_shutdown.send(tx).is_ok() {
                    // We ignore this error because the receiver is return cancelled error.
                    let _ = futures::executor::block_on(rx);
                }
            }
        }
    }

    pub fn force_kill_session(self: &Arc<Self>) {
        self.force_kill_query();
        self.kill(/* shutdown io stream */);
    }

    pub fn force_kill_query(self: &Arc<Self>) {
        let mut mutable_state = self.mutable_state.lock();

        if let Some(context_shared) = mutable_state.context_shared.take() {
            context_shared.kill(/* shutdown executing query */);
        }
    }

    /// Create a query context for query.
    /// For a query, execution environment(e.g cluster) should be immutable.
    /// We can bind the environment to the context in create_context method.
    pub async fn create_context(self: &Arc<Self>) -> DatafuseQueryContextRef {
        let mut state_guard = self.mutable_state.lock();

        if state_guard.context_shared.is_none() {
            let config = self.config.clone();
            let shared = DatafuseQueryContextShared::try_create(config, self.clone());
            state_guard.context_shared = Some(shared);
        }

        match &state_guard.context_shared {
            Some(shared) => DatafuseQueryContext::from_shared(shared.clone()),
            None => unreachable!(),
        }
    }

    pub fn attach<F>(self: &Arc<Self>, host: Option<SocketAddr>, io_shutdown: F)
    where F: FnOnce() + Send + 'static {
        let (tx, rx) = futures::channel::oneshot::channel();
        let mut inner = self.mutable_state.lock();
        inner.client_host = host;
        inner.io_shutdown_tx = Some(tx);

        common_runtime::tokio::spawn(async move {
            if let Ok(tx) = rx.await {
                (io_shutdown)();
                tx.send(()).ok();
            }
        });
    }

    pub fn set_current_database(self: &Arc<Self>, database_name: String) {
        let mut inner = self.mutable_state.lock();
        inner.current_database = database_name;
    }

    pub fn get_current_database(self: &Arc<Self>) -> String {
        let inner = self.mutable_state.lock();
        inner.current_database.clone()
    }

    pub fn get_settings(self: &Arc<Self>) -> Arc<Settings> {
        self.mutable_state.lock().session_settings.clone()
    }

    pub fn try_get_cluster(self: &Arc<Self>) -> Result<ClusterRef> {
        Ok(self.sessions.get_cluster())
    }

    pub fn get_sessions_manager(self: &Arc<Self>) -> SessionManagerRef {
        self.sessions.clone()
    }

    pub fn get_catalog(self: &Arc<Self>) -> Arc<DatabaseCatalog> {
        self.sessions.get_catalog()
    }
}
