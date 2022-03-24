// Copyright 2022 Datafuse Labs.
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

use std::time::Instant;

pub struct SessionStatus {
    pub session_started_at: Instant,
    pub last_query_finished_at: Option<Instant>,
}

impl SessionStatus {
    pub(crate) fn query_finish(&mut self) {
        self.last_query_finished_at = Some(Instant::now())
    }

    pub(crate) fn last_access(&self) -> Instant {
        self.last_query_finished_at
            .unwrap_or(self.session_started_at)
    }
}

impl Default for SessionStatus {
    fn default() -> Self {
        SessionStatus {
            session_started_at: Instant::now(),
            last_query_finished_at: None,
        }
    }
}
