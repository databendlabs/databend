// Copyright 2021 Datafuse Labs.
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

use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;

use common_tracing::tracing;

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

impl Clone for SessionRef {
    fn clone(&self) -> Self {
        SessionRef::create(self.session.clone())
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
            tracing::debug!("Destroy session {}", self.id);
            self.session_mgr.destroy_session(&self.id);
        }
    }

    pub fn increment_ref_count(self: &Arc<Self>) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }
}
