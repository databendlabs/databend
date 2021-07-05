// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::sessions::FuseQueryContextRef;
use crate::sessions::Session;
use crate::sessions::SessionManagerRef;

/// SessionRef is the ptr of session.
/// Remove it in session_manager when the current session is not referenced
pub struct SessionRef {
    typ: String,
    session: Arc<Session>,
    sessions: SessionManagerRef,
}

impl SessionRef {
    pub fn create(typ: String, session: Arc<Session>, sessions: SessionManagerRef) -> SessionRef {
        session.increment_ref_count();
        SessionRef {
            typ,
            session,
            sessions,
        }
    }

    pub fn get_id(&self) -> String {
        self.session.get_id()
    }

    pub fn get_type(&self) -> String {
        self.typ.clone()
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
        self.session.destroy_session_ref();
    }
}

impl Session {
    pub fn destroy_session_ref(self: &Arc<Self>) {
        if self.decrement_ref_count() {
            log::info!("Destroy session {}", self.id);
            self.sessions.destroy_session(&self.id);
        }
    }

    pub fn increment_ref_count(self: &Arc<Self>) {
        let mut mutable_status = self.mutable_state.lock();
        mutable_status.ref_count += 1;
    }

    fn decrement_ref_count(self: &Arc<Self>) -> bool {
        let mut mutable_status = self.mutable_state.lock();

        if mutable_status.ref_count > 0 {
            mutable_status.ref_count -= 1;
        }

        mutable_status.ref_count == 0
    }
}
