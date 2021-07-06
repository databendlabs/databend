// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use common_exception::Result;
use common_infallible::Mutex;
use futures::channel::oneshot::Sender;
use futures::channel::*;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::datasources::DataSource;
use crate::sessions::context_shared::FuseQueryContextShared;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionManagerRef;
use crate::sessions::Settings;

pub(in crate::sessions) struct MutableStatus {
    pub(in crate::sessions) abort: bool,
    pub(in crate::sessions) current_database: String,
    pub(in crate::sessions) session_settings: Arc<Settings>,
    pub(in crate::sessions) client_host: Option<SocketAddr>,
    pub(in crate::sessions) io_shutdown_tx: Option<Sender<Sender<()>>>,
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
                abort: false,
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
        self.mutable_state.lock().abort
    }

    pub fn kill(self: &Arc<Self>) {
        let mut mutable_state = self.mutable_state.lock();

        mutable_state.abort = true;
        if let Some(io_shutdown) = mutable_state.io_shutdown_tx.take() {
            if mutable_state.context_shared.is_none() {
                let (tx, rx) = oneshot::channel();
                io_shutdown.send(tx).is_ok();
                futures::executor::block_on(rx);
            }
        }
    }

    pub fn force_kill(self: &Arc<Self>) {
        {
            let mut mutable_state = self.mutable_state.lock();

            mutable_state.abort = true;
            if let Some(context_shared) = mutable_state.context_shared.take() {
                context_shared.kill(/* shutdown executing query */);
            }
        }

        self.kill(/* shutdown io stream */);
    }

    pub fn create_context(self: &Arc<Self>) -> FuseQueryContextRef {
        let state_guard = self.mutable_state.lock();

        match &state_guard.context_shared {
            Some(shared) => FuseQueryContext::from_shared(shared.clone()),
            None => FuseQueryContext::from_shared(FuseQueryContextShared::try_create(
                self.config.clone(),
                self.clone(),
            )),
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

    pub fn get_datasource(self: &Arc<Self>) -> Arc<DataSource> {
        self.sessions.get_datasource()
    }
}
