// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;

use crate::sessions::FuseQueryContextRef;
use crate::sessions::ProcessInfo;
use crate::sessions::Session;

/// SessionRef is the ptr of session.
/// Remove it in session_manager when the current session is not referenced
pub struct SessionRef {
    typ: String,
    session: Arc<Session>,
}

impl SessionRef {
    pub fn create(typ: String, session: Arc<Session>) -> SessionRef {
        session.increment_ref_count();
        SessionRef { typ, session }
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

    pub fn processes_info(self: &Arc<Self>) -> Vec<ProcessInfo> {
        self.session.processes_info()
    }
}

impl Drop for SessionRef {
    fn drop(&mut self) {
        self.session.destroy_session_ref();
    }
}

impl Session {
    pub fn destroy_session_ref(self: &Arc<Self>) {
        if self.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            std::sync::atomic::fence(Acquire);
            log::debug!("Destroy session {}", self.id);
            self.sessions.destroy_session(&self.id);
        }
    }

    pub fn increment_ref_count(self: &Arc<Self>) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }
}
