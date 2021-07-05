// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use common_exception::Result;
use common_infallible::Mutex;
use futures::channel::oneshot::Sender;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::datasources::DataSource;
use crate::sessions::context_shared::FuseQueryContextShared;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionManagerRef;
use crate::sessions::Settings;

#[derive(PartialEq, Clone)]
pub enum State {
    Idle,
    Progress,
    Aborting,
    Aborted,
}

pub(in crate::sessions) struct MutableStatus {
    pub(in crate::sessions) state: State,
    pub(in crate::sessions) current_database: String,
    pub(in crate::sessions) session_settings: Arc<Settings>,
    pub(in crate::sessions) client_host: Option<SocketAddr>,
    pub(in crate::sessions) io_shutdown_tx: Option<Sender<()>>,
    pub(in crate::sessions) ref_count: usize,
    pub(in crate::sessions) context_shared: Option<Arc<FuseQueryContextShared>>,
}

#[derive(Clone)]
pub struct Session {
    pub(in crate::sessions) id: String,
    pub(in crate::sessions) config: Config,
    pub(in crate::sessions) sessions: SessionManagerRef,
    pub(in crate::sessions) mutable_state: Arc<Mutex<MutableStatus>>,
}

impl Session {
    pub fn try_create(
        config: Config,
        id: String,
        sessions: SessionManagerRef,
    ) -> Result<Arc<Session>> {
        Ok(Arc::new(Session {
            id,
            config,
            sessions,
            mutable_state: Arc::new(Mutex::new(MutableStatus {
                state: State::Idle,
                current_database: String::from("default"),
                session_settings: Settings::try_create()?,
                client_host: None,
                io_shutdown_tx: None,
                ref_count: 0,
                context_shared: None,
            })),
        }))
    }

    pub fn get_id(self: &Arc<Self>) -> String {
        self.id.clone()
    }

    pub fn is_aborting(self: &Arc<Self>) -> bool {
        match self.mutable_state.lock().state {
            State::Aborting | State::Aborted => true,
            _ => false,
        }
    }

    pub fn kill(self: &Arc<Self>) {
        let mut mutable_status = self.mutable_state.lock();
        if let Some(io_shutdown) = mutable_status.io_shutdown_tx.take() {
            // TODO: kill FuseQueryContext
            io_shutdown.send(());
        }
    }

    pub fn create_context(self: &Arc<Self>) -> FuseQueryContextRef {
        let state_guard = self.mutable_state.lock();

        match &state_guard.context_shared {
            Some(shared) => FuseQueryContext::from_shared(shared.clone()),
            None => {
                FuseQueryContext::from_shared(FuseQueryContextShared::try_create(
                    self.config.clone(),
                    self.clone(),
                ))
            }
        }
    }

    pub fn attach<F>(self: &Arc<Self>, host: Option<SocketAddr>, io_shutdown: F)
        where F: FnOnce() + Send + 'static
    {
        let (tx, rx) = futures::channel::oneshot::channel();
        let mut inner = self.mutable_state.lock();
        inner.client_host = host;
        inner.io_shutdown_tx = Some(tx);

        common_runtime::tokio::spawn(async move {
            if let Ok(_) = rx.await {
                (io_shutdown)()
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

    pub fn get_datasource(self: &Arc<Self>) -> Arc<DataSource> {
        self.sessions.get_datasource()
    }
}
