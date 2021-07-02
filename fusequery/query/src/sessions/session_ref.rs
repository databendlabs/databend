// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use common_exception::Result;

use crate::sessions::FuseQueryContextRef;
use crate::sessions::Session;
use crate::sessions::SessionManagerRef;

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

    pub fn create_context(&self) -> FuseQueryContextRef {
        self.session.create_context()
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
