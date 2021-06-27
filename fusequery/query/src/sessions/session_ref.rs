// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::{Shutdown, SocketAddr};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;

use futures::future::AbortHandle;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_planners::PlanNode;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::datasources::DataSource;
use crate::sessions::{FuseQueryContext, Session, SessionManagerRef};
use crate::sessions::FuseQueryContextRef;
use crate::sessions::Settings;
use futures::channel::oneshot::Receiver;

/// SessionRef is the ptr of session.
/// Remove it in session_manager when the current session is not referenced (ref_count = 1)
pub struct SessionRef {
    typ: String,
    session: Arc<Session>,
    sessions: SessionManagerRef,
}

impl SessionRef {
    pub fn create(typ: String, session: Arc<Session>, sessions: SessionManagerRef) -> SessionRef {
        SessionRef {
            typ,
            session,
            sessions,
        }
    }

    pub fn get_type(&self) -> String {
        self.typ.clone()
    }

    pub fn get_id(&self) -> String {
        self.session.get_id()
    }

    pub fn try_create_context(&self) -> Result<FuseQueryContextRef> {
        self.session.try_create_context()
    }

    pub fn is_aborting(&self) -> bool {
        self.session.is_aborting()
    }

    pub fn attach<F: FnOnce() + Send + 'static>(&self, host: Option<SocketAddr>, io_shutdown: F) {
        self.session.attach(host, io_shutdown)
    }
}

impl Drop for SessionRef {
    fn drop(&mut self) {
        // TODO destroy session if ref_count = 0
        // Attempt to destroy session when current reference is destroyed
        self.sessions.destroy_session(&self.get_id())
    }
}

