// Copyright 2021 Datafuse Labs
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

use std::time::SystemTime;

#[derive(Clone)]
pub struct SessionManagerStatus {
    pub running_queries_count: u64,
    pub active_sessions_count: u64,
    pub max_running_query_execute_time: u64,
    pub last_query_started_at: Option<SystemTime>,
    pub last_query_finished_at: Option<SystemTime>,
    pub earliest_running_query_started_at: Option<SystemTime>,
    pub instance_started_at: SystemTime,
}

impl SessionManagerStatus {
    pub(crate) fn query_start(&mut self, now: SystemTime) {
        self.running_queries_count += 1;
        self.last_query_started_at = Some(now)
    }

    pub(crate) fn query_finish(&mut self, now: SystemTime) {
        self.running_queries_count -= 1;
        if self.running_queries_count == 0 {
            self.last_query_finished_at = Some(now)
        }
    }
}

impl Default for SessionManagerStatus {
    fn default() -> Self {
        SessionManagerStatus {
            running_queries_count: 0,
            active_sessions_count: 0,
            max_running_query_execute_time: 0,
            last_query_started_at: None,
            last_query_finished_at: None,
            earliest_running_query_started_at: None,
            instance_started_at: SystemTime::now(),
        }
    }
}
