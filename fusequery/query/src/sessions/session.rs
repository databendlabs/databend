// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use futures::future::AbortHandle;

use common_exception::{ErrorCode, Result};
use common_infallible::Mutex;
use common_planners::PlanNode;
use common_runtime::tokio::net::TcpStream;

use crate::configs::Config;
use crate::servers::AbortableService;
use crate::sessions::{FuseQueryContext, FuseQueryContextRef, Settings};
use crate::sessions::SessionManagerRef;
use std::net::SocketAddr;
use futures::channel::oneshot::{Receiver, Sender};
use crate::sessions::context_shared::FuseQueryContextShared;

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
}

#[derive(Clone)]
pub struct Session {
    id: String,
    config: Config,
    sessions: SessionManagerRef,
    mutable_status: Arc<Mutex<MutableStatus>>,
}

impl Session {
    pub fn try_create(config: Config, id: String, sessions: SessionManagerRef) -> Result<Arc<Session>> {
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
            })),
        }))
    }

    pub fn get_id(self: &Arc<Self>) -> String {
        self.id.clone()
    }

    pub fn is_aborting(self: &Arc<Self>) -> bool {
        match self.mutable_status.lock().state {
            State::Aborting | State::Aborted => true,
            _ => false
        }
    }

    pub fn try_create_context(self: &Arc<Self>) -> Result<FuseQueryContextRef> {
        let inner = self.mutable_status.lock();

        // TODO: take inner context shared.
        FuseQueryContext::from_shared(FuseQueryContextShared::try_create(
            self.config.clone(),
            self.clone(),
        )?)
    }

    pub fn attach<F: FnOnce() + Send + 'static>(self: &Arc<Self>, host: Option<SocketAddr>, io_shutdown: F) {
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

    pub fn update_database(self: &Arc<Self>, database_name: String) {
        let mut inner = self.mutable_status.lock();
        inner.current_database = database_name;
    }
}
