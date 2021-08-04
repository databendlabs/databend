// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;

use crate::sessions::Session;

/// SessionRef is the ptr of session.
/// Remove it in session_manager when the current session is not referenced
pub struct SessionRef {
    session: Arc<Session>,
}

impl SessionRef {
    pub fn create(session: Arc<Session>) -> SessionRef {
        session.increment_ref_count();
        SessionRef { session }
    }
}

impl Deref for SessionRef {
    type Target = Arc<Session>;

    fn deref(&self) -> &Self::Target {
        &self.session
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
