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

struct MutableStatus {
    state: State,
    current_database: String,
    session_settings: Arc<Settings>,
    client_host: Option<SocketAddr>,
    io_shutdown_tx: Option<Sender<()>>,
    ref_count: usize,
}

#[derive(Clone)]
pub struct Session {
    id: String,
    config: Config,
    sessions: SessionManagerRef,
    mutable_status: Arc<Mutex<MutableStatus>>,
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
            mutable_status: Arc::new(Mutex::new(MutableStatus {
                state: State::Idle,
                current_database: String::from("default"),
                session_settings: Settings::try_create()?,
                client_host: None,
                io_shutdown_tx: None,
                ref_count: 0,
            })),
        }))
    }

    pub fn get_id(self: &Arc<Self>) -> String {
        self.id.clone()
    }

    pub fn is_aborting(self: &Arc<Self>) -> bool {
        match self.mutable_status.lock().state {
            State::Aborting | State::Aborted => true,
            _ => false,
        }
    }

    pub fn create_context(self: &Arc<Self>) -> FuseQueryContextRef {
        // let inner = self.mutable_status.lock();

        // TODO: take inner context shared.
        FuseQueryContext::from_shared(FuseQueryContextShared::try_create(
            self.config.clone(),
            self.clone(),
        ))
    }

    pub fn attach<F>(self: &Arc<Self>, host: Option<SocketAddr>, io_shutdown: F)
        where F: FnOnce() + Send + 'static
    {
        let (tx, rx) = futures::channel::oneshot::channel();
        let mut inner = self.mutable_status.lock();
        inner.client_host = host;
        inner.io_shutdown_tx = Some(tx);

        common_runtime::tokio::spawn(async move {
            if let Ok(_) = rx.await {
                (io_shutdown)()
            }
        });
    }

    pub fn set_current_database(self: &Arc<Self>, database_name: String) {
        let mut inner = self.mutable_status.lock();
        inner.current_database = database_name;
    }

    pub fn get_current_database(self: &Arc<Self>) -> String {
        let inner = self.mutable_status.lock();
        inner.current_database.clone()
    }

    pub fn try_get_cluster(self: &Arc<Self>) -> Result<ClusterRef> {
        Ok(self.sessions.get_cluster())
    }

    pub fn get_settings(self: &Arc<Self>) -> Arc<Settings> {
        self.mutable_status.lock().session_settings.clone()
    }

    pub fn get_datasource(self: &Arc<Self>) -> Arc<DataSource> {
        self.sessions.get_datasource()
    }

    pub fn destroy_session_ref(self: &Arc<Self>) {
        if self.decrement_ref_count() {
            self.sessions.destroy_session(&self.id);
        }
    }

    pub fn increment_ref_count(self: &Arc<Self>) {
        let mut mutable_status = self.mutable_status.lock();
        mutable_status.ref_count += 1;
    }

    fn decrement_ref_count(self: &Arc<Self>) -> bool {
        let mut mutable_status = self.mutable_status.lock();

        if mutable_status.ref_count > 0 {
            mutable_status.ref_count -= 1;
        }

        mutable_status.ref_count == 0
    }
}
