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

use common_exception::ErrorCode;
use common_exception::Result;
use common_macros::MallocSizeOf;
use common_mem_allocator::malloc_size;
use futures::channel::*;

use crate::catalogs::impls::DatabaseCatalog;
use crate::configs::Config;
use crate::sessions::context_shared::DatabendQueryContextShared;
use crate::sessions::DatabendQueryContext;
use crate::sessions::DatabendQueryContextRef;
use crate::sessions::MutableStatus;
use crate::sessions::SessionManagerRef;
use crate::sessions::Settings;
use crate::users::UserApiProvider;

#[derive(Clone, MallocSizeOf)]
pub struct Session {
    pub(in crate::sessions) id: String,
    pub(in crate::sessions) typ: String,
    #[ignore_malloc_size_of = "insignificant"]
    pub(in crate::sessions) config: Config,
    #[ignore_malloc_size_of = "insignificant"]
    pub(in crate::sessions) sessions: SessionManagerRef,
    pub(in crate::sessions) ref_count: Arc<AtomicUsize>,
    pub(in crate::sessions) mutable_state: Arc<MutableStatus>,
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
            mutable_state: Arc::new(MutableStatus::try_create()?),
        }))
    }

    pub fn get_id(self: &Arc<Self>) -> String {
        self.id.clone()
    }

    pub fn get_type(self: &Arc<Self>) -> String {
        self.typ.clone()
    }

    pub fn is_aborting(self: &Arc<Self>) -> bool {
        self.mutable_state.get_abort()
    }

    pub fn kill(self: &Arc<Self>) {
        let mutable_state = self.mutable_state.clone();
        mutable_state.set_abort(true);
        if mutable_state.context_shared_is_none() {
            if let Some(io_shutdown) = mutable_state.take_io_shutdown_tx() {
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
        let mutable_state = self.mutable_state.clone();

        if let Some(context_shared) = mutable_state.take_context_shared() {
            context_shared.kill(/* shutdown executing query */);
        }
    }

    /// Create a query context for query.
    /// For a query, execution environment(e.g cluster) should be immutable.
    /// We can bind the environment to the context in create_context method.
    pub async fn create_context(self: &Arc<Self>) -> Result<DatabendQueryContextRef> {
        let context_shared = self.mutable_state.get_context_shared();

        Ok(match context_shared.as_ref() {
            Some(shared) => DatabendQueryContext::from_shared(shared.clone()),
            None => {
                let config = self.config.clone();
                let discovery = self.sessions.get_cluster_discovery();

                let session = self.clone();
                let cluster = discovery.discover().await?;
                let shared = DatabendQueryContextShared::try_create(config, session, cluster);

                let ctx_shared = self.mutable_state.get_context_shared();
                match ctx_shared.as_ref() {
                    Some(shared) => DatabendQueryContext::from_shared(shared.clone()),
                    None => {
                        self.mutable_state.set_context_shared(Some(shared.clone()));
                        DatabendQueryContext::from_shared(shared)
                    }
                }
            }
        })
    }

    pub fn attach<F>(self: &Arc<Self>, host: Option<SocketAddr>, io_shutdown: F)
    where F: FnOnce() + Send + 'static {
        let (tx, rx) = futures::channel::oneshot::channel();
        self.mutable_state.set_client_host(host);
        self.mutable_state.set_io_shutdown_tx(Some(tx));

        common_base::tokio::spawn(async move {
            if let Ok(tx) = rx.await {
                (io_shutdown)();
                tx.send(()).ok();
            }
        });
    }

    pub fn set_current_database(self: &Arc<Self>, database_name: String) {
        self.mutable_state.set_current_database(database_name);
    }

    pub fn get_current_database(self: &Arc<Self>) -> String {
        self.mutable_state.get_current_database()
    }

    pub fn get_current_user(self: &Arc<Self>) -> Result<String> {
        self.mutable_state
            .get_current_user()
            .ok_or_else(|| ErrorCode::AuthenticateFailure("unauthenticated"))
    }

    pub fn set_current_user(self: &Arc<Self>, user: String) {
        self.mutable_state.set_current_user(user)
    }

    pub fn get_settings(self: &Arc<Self>) -> Arc<Settings> {
        self.mutable_state.get_settings()
    }

    pub fn get_sessions_manager(self: &Arc<Self>) -> SessionManagerRef {
        self.sessions.clone()
    }

    pub fn get_catalog(self: &Arc<Self>) -> Arc<DatabaseCatalog> {
        self.sessions.get_catalog()
    }

    pub fn get_user_manager(self: &Arc<Self>) -> Arc<UserApiProvider> {
        self.sessions.get_user_manager()
    }

    pub fn get_memory_usage(self: &Arc<Self>) -> usize {
        malloc_size(self)
    }
}
